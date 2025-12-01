import logging

from ai_search import (
    AiSearchBuilder,
    build_embedding_client_factory,
    get_ai_setting,
    load_ai_search_restrictions,
)
from biothings.web.query import ESQueryBuilder, ESResultFormatter
from biothings.web.query.engine import AsyncESQueryBackend
from elasticsearch_dsl import A, Q, Search

logger = logging.getLogger(__name__)


def transform_lineage_response(response):
    facets = response.get('facets', {})
    lineage = facets.get('lineage', {})
    children = lineage.get('children_of_lineage', {})
    taxon_ids_data = children.get('taxon_ids', {})
    lineage_total = facets.get('lineage_total_count', {})

    transformed = {
        "lineage": {
            "totalRecords": facets.get("lineage_doc_count", {}).get(
                "doc_count"),
            "totalLineageRecords": lineage_total.get("inner_filter", {}).get(
                "to_parent", {}).get("doc_count"),
            "children": {
                "totalChildRecords": children.get("doc_count"),
                "totalUniqueChildRecords": children.get("to_parent", {}).get(
                    "doc_count"),
                "childTaxonCounts": [
                    {
                        "taxonId": term.get("term"),
                        "count": term.get("count"),
                    }
                    for term in taxon_ids_data.get("terms", [])
                ]
            }
        }
    }
    return transformed


class NDEQueryBuilder(ESQueryBuilder):
    # https://docs.biothings.io/en/latest/_modules/biothings/web/query/builder.html#ESQueryBuilder.default_string_query

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        restrictions = load_ai_search_restrictions()
        ai_index = get_ai_setting("AI_SEARCH_INDEX")
        self.ai_search_builder = AiSearchBuilder(
            index=ai_index,
            vector_field="ibmGraniteEmbedding",
            embedding_client_factory=build_embedding_client_factory(),
            staging_ids=restrictions["staging_ids"],
            prod_sources=restrictions["prod_sources"],
            resource_catalog_boost=1000.0,
            rescore_window=100,
            base_query_weight=0.5,
            rescore_weight=1.5,
            enable_rescore=True,
        )

    def _build_ai_search(self, query_text, options):
        if not self.ai_search_builder:
            raise ValueError("AI search is disabled for this deployment.")
        # Ensure backend targets the AI-specific index rather than the default
        options.biothing_type = "ai"
        try:
            return self.ai_search_builder.build_search(query_text, options)
        except Exception as exc:
            logger.exception("AI search failed: %s", exc)
            raise ValueError(f"AI search failed: {exc}") from exc

    def default_string_query(self, q, options):
        if getattr(options, "use_ai_search", False):
            return self._build_ai_search(q, options)

        search = Search()
        q = q.strip()

        # elasticsearch query string syntax
        # https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html#query-string-syntax
        if ":" in q or " AND " in q or " OR " in q:
            search = search.query(
                "query_string",
                query=q,
                default_operator="AND",
                lenient=True,
            )

        # term search
        elif q.startswith('"') and q.endswith('"'):
            queries = [
                # term query boosting
                Q("term", _id={"value": q.strip('"'), "boost": 5}),
                Q("term", name={"value": q.strip('"'), "boost": 5}),
                # query string
                Q(
                    "query_string",
                    query=q,
                    default_operator="AND",
                    lenient=True,
                ),
            ]

            search = search.query("dis_max", queries=queries)

        # simple text search
        else:
            queries = self.build_queries(q, None)
            search = search.query("dis_max", queries=queries)

        return search

    def build_queries(self, q, custom_function_script):
        def _function_score(base_query):
            params = {"query": base_query, "boost_mode": "replace"}
            if custom_function_script:
                params["script_score"] = {"script": custom_function_script}
            return Q("function_score", **params)

        queries = [
            _function_score(Q("term", _id={"value": q, "boost": 5})),
            _function_score(Q("term", name={"value": q, "boost": 5})),
            _function_score(
                Q(
                    "query_string",
                    query=q,
                    default_operator="AND",
                    lenient=True,
                )
            ),
        ]
        # check if q contains wildcards if not add wildcard query to every word
        if not ("*" in q or "?" in q):
            wc_query = Q(
                "query_string",
                query="* ".join(q.split()) + "*",
                default_operator="AND",
                boost=0.5,
                lenient=True,
            )
            queries.append(_function_score(wc_query))

        # Remove function_score wrapping if custom_function_script is None
        if custom_function_script is None:
            queries = [Q(q.query) for q in queries]

        return queries

    def apply_extras(self, search, options):
        # Align AI and keyword searches by always applying the standard
        # resource type filters here instead of inside individual builders.
        type_filter = [
            {"terms": {"@type": ["Dataset", "ResourceCatalog", "Sample"]}},
        ]

        computational_tool_condition = {
            "bool": {
                "must": [
                    {"term": {"@type": "ComputationalTool"}},
                    {"term": {"includedInDataCatalog.name": "bio.tools"}},
                ]
            }
        }

        search = search.filter(
            "bool", should=type_filter + [computational_tool_condition]
        )

        if options.extra_filter:
            search = search.query("query_string", query=options.extra_filter)

        # apply hist aggregation
        if options.hist:
            a = A(
                "date_histogram",
                field=options.hist,
                calendar_interval=options.hist_interval,
                min_doc_count=1,
            )
            search.aggs.bucket("hist_dates", a)
        # apply suggester
        if options.suggester:
            phrase_suggester = {
                "field": "name.phrase_suggester",
                "size": 3,
                "direct_generator": [
                    {
                        "field": "name.phrase_suggester",
                        "suggest_mode": "always",
                    }
                ],
                "max_errors": 2,
                "highlight": {"pre_tag": "<em>", "post_tag": "</em>"},
            }
            search = search.suggest(
                "nde_suggester", options.suggester, phrase=phrase_suggester)

        # apply function score
        if not getattr(options, "use_ai_search", False):
            if options.use_metadata_score:
                script_source = (
                    "double required_ratio = doc['_meta.completeness."
                    "required_ratio'].value;\n"
                    "double recommended_ratio = doc['_meta.completeness."
                    "recommended_score_ratio'].value;\n"
                    "double b = 1 - params.a;\n"
                    "double d = 1 - params.c;\n"
                    "double score = (params.a * _score) + (b * ((params.c * "
                    "required_ratio) + (d * recommended_ratio)));\n"
                    "if (doc['@type'].value == 'ResourceCatalog') {\n"
                    "    score *= params.boost_factor;\n"
                    "}\n"
                    "return score;\n"
                )
                custom_function_script = {
                    "source": script_source,
                    "params": {
                        "a": 0.8,
                        "c": 0.75,
                        "boost_factor": 1000.0,
                    },
                }
                function_score_query = Q(
                    "function_score",
                    script_score={"script": custom_function_script},
                    boost_mode="replace",
                )
                search = search.query(function_score_query)
            else:
                functions = [
                    {
                        "filter": {"term": {"@type": "ResourceCatalog"}},
                        "weight": 1000,
                    }
                ]

                search = search.query(
                    "function_score",
                    query=search.to_dict().get("query"),
                    functions=functions,
                    boost_mode="replace",
                )

        # apply multi-term aggregation
        if options.multi_terms_fields:
            multi_terms_size = options.get('multi_terms_size', 10)
            multi_terms_agg = A(
                "multi_terms",
                terms=[{"field": field}
                       for field in options.multi_terms_fields],
                size=multi_terms_size
            )
            search.aggs.bucket("multi_terms_agg", multi_terms_agg)

        # hide _meta object and suppress large embedding vectors in responses
        exclude_fields = ["ibmGraniteEmbedding"]
        if options.show_meta:
            search = search.source(includes=["*"], excludes=exclude_fields)
        else:
            search = search.source(excludes=exclude_fields + ["_meta"])

        # # spam filter
        # spam_filter = Q(
        #     "bool",
        #     should=[
        #         Q("bool", must=[Q("match", name="keto"),
        #           Q("match", name="gummies")]),
        #         Q("bool", must=[Q("match", description="keto"),
        #           Q("match", description="gummies")]),
        #     ],
        #     minimum_should_match=1
        # )
        # search = search.exclude(spam_filter)

        if options.get('lineage'):
            lineage_taxon_id = options.get('lineage')

            # Existing lineage aggregation
            lineage_agg = A('nested', path='_meta.lineage')

            children_of_lineage_filter = A(
                'filter', term={'_meta.lineage.parent_taxon': lineage_taxon_id}
            )

            taxon_ids_terms = A('terms', field='_meta.lineage.taxon')
            taxon_ids_terms.bucket('to_parent', A('reverse_nested'))

            children_of_lineage_filter.bucket('to_parent', A('reverse_nested'))
            children_of_lineage_filter.bucket('taxon_ids', taxon_ids_terms)

            lineage_agg.bucket('children_of_lineage',
                               children_of_lineage_filter)

            search.aggs.bucket('lineage', lineage_agg)

            # Count datasets matching species or infectiousAgent identifiers
            # Since these fields are not nested, we can query them directly
            lineage_taxon_filter = Q(
                'bool',
                should=[
                    Q('term', **{'species.identifier': lineage_taxon_id}),
                    Q('term', **
                      {'infectiousAgent.identifier': lineage_taxon_id})
                ],
                minimum_should_match=1
            )

            # Create a filter aggregation using the above filter
            lineage_doc_count_agg = A('filter', lineage_taxon_filter)

            # Include this aggregation at the top level
            search.aggs.bucket('lineage_doc_count', lineage_doc_count_agg)

            # New aggregation for counting total lineage records
            # based on _meta.lineage.taxon
            lineage_total_filter = A('nested', path='_meta.lineage')
            lineage_total_inner_filter = A(
                'filter', term={'_meta.lineage.taxon': lineage_taxon_id})
            lineage_total_inner_filter.bucket('to_parent', A('reverse_nested'))
            lineage_total_filter.bucket(
                'inner_filter', lineage_total_inner_filter)
            search.aggs.bucket('lineage_total_count', lineage_total_filter)

        return super().apply_extras(search, options)


class NDEESQueryBackend(AsyncESQueryBackend):
    def adjust_index(self, original_index, query, **options):
        if options.get("use_ai_search"):
            return self.indices.get("ai", original_index)
        return original_index


class NDEFormatter(ESResultFormatter):
    def transform_aggs(self, res):
        def transform_agg(agg_res):
            if 'buckets' in agg_res:
                agg_res['_type'] = 'terms'
                agg_res['terms'] = agg_res.pop('buckets')
                agg_res['other'] = agg_res.pop('sum_other_doc_count', 0)
                agg_res['missing'] = agg_res.pop(
                    'doc_count_error_upper_bound', 0)
                count = 0
                for bucket in agg_res['terms']:
                    bucket['count'] = bucket.pop('doc_count')
                    bucket['term'] = bucket.pop('key')
                    if 'key_as_string' in bucket:
                        bucket['term'] = bucket.pop('key_as_string')
                    count += bucket['count']
                    for k in list(bucket.keys()):
                        if isinstance(bucket[k], dict):
                            transform_agg(bucket[k])
                agg_res['total'] = count
            else:
                for k in list(agg_res.keys()):
                    if isinstance(agg_res[k], dict):
                        transform_agg(agg_res[k])
        transform_agg(res)

        # If lineage aggregations are present in the facets,
        # apply our transformation
        if 'facets' in res and 'lineage' in res['facets']:
            raw_lineage = res['facets']['lineage']
            raw_lineage_doc_count = res['facets'].get('lineage_doc_count', {})
            raw_lineage_total_count = res['facets'].get(
                'lineage_total_count', {})
            # Prepare a temporary response structure for the transformer
            lineage_response = {
                "facets": {
                    "lineage": raw_lineage,
                    "lineage_doc_count": raw_lineage_doc_count,
                    "lineage_total_count": raw_lineage_total_count
                }
            }
            transformed_lineage = transform_lineage_response(lineage_response)
            # Replace the raw lineage aggregation with our
            # transformed structure
            res['facets']['lineage'] = transformed_lineage['lineage']
            res['facets'].pop('lineage_doc_count', None)
            res['facets'].pop('lineage_total_count', None)
        elif 'lineage' in res:
            # For responses where lineage is at the top level
            # rather than under 'facets'
            raw_lineage = res['lineage']
            raw_lineage_doc_count = res.get('lineage_doc_count', {})
            raw_lineage_total_count = res.get('lineage_total_count', {})
            lineage_response = {
                "facets": {
                    "lineage": raw_lineage,
                    "lineage_doc_count": raw_lineage_doc_count,
                    "lineage_total_count": raw_lineage_total_count
                }
            }
            transformed_lineage = transform_lineage_response(lineage_response)
            res['lineage'] = transformed_lineage['lineage']
            res.pop('lineage_doc_count', None)
            res.pop('lineage_total_count', None)

        return res
