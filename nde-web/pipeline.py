import json

from biothings.web.query import ESQueryBuilder, ESResultFormatter
from elasticsearch_dsl import A, Q, Search


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

    def default_string_query(self, q, options):
        search = Search()
        q = q.strip()

        # elasticsearch query string syntax
        # https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html#query-string-syntax
        if ":" in q or " AND " in q or " OR " in q:
            search = search.query("query_string", query=q, default_operator="AND", lenient=True)

        # term search
        elif q.startswith('"') and q.endswith('"'):
            queries = [
                # term query boosting
                Q("term", _id={"value": q.strip('"'), "boost": 5}),
                Q("term", name={"value": q.strip('"'), "boost": 5}),
                # query string
                Q("query_string", query=q, default_operator="AND", lenient=True),
            ]

            search = search.query("dis_max", queries=queries)

        # simple text search
        else:
            queries = [
                # term query boosting
                Q("term", _id={"value": q, "boost": 5}),
                Q("term", name={"value": q, "boost": 5}),
                # query string
                Q("query_string", query=q, default_operator="AND", lenient=True),
            ]

            # check if q contains wildcards; if not, add wildcard queries
            if not ("*" in q or "?" in q):
                wc_query = Q(
                    "query_string",
                    query="* ".join(q.split()) + "*",
                    default_operator="AND",
                    boost=0.5,
                    lenient=True,
                )
                queries.append(wc_query)

            search = search.query("dis_max", queries=queries)

        return search

    def apply_extras(self, search, options):

        # remove specific documents from the search results
        with open("exclusions.json") as f:
            data = json.load(f)

        # Get the list of staging IDs
        staging_ids = data.get("staging_ids")

        # Get the list of prod sources
        prod_sources = data.get("prod_catalogs")

        # exclude staging IDs from the search results
        search = search.query("bool", must_not=[Q("ids", values=staging_ids)])

        # include only documents from the allowed prod sources
        search = search.query("bool", must=[Q("terms", **{"includedInDataCatalog.name": prod_sources})])

        # We only want those of type Dataset or ResourceCatalog.
        # Filter to allow @type Dataset, ResourceCatalog and ComputationalTool only from Bio.tools
        filter_conditions = [
            # Include Dataset and ResourceCatalog
            {"terms": {"@type": ["Dataset", "ResourceCatalog"]}},
        ]

        computational_tool_condition = {
            "bool": {
                "must": [
                    {"term": {"@type": "ComputationalTool"}},
                    {"term": {"includedInDataCatalog.name": "bio.tools"}}
                ]
            }
        }

        search = search.filter(
            "bool", should=filter_conditions + [computational_tool_condition])

        # Apply the new metadata-based scoring by default:
        # This script considers the completeness ratios and boosts ResourceCatalog items.
        custom_function_script = {
            "source": """
                double required_ratio = doc['_meta.completeness.required_ratio'].value;
                double recommended_ratio = doc['_meta.completeness.recommended_score_ratio'].value;
                double b = 1 - params.a;
                double d = 1 - params.c;
                double score = (params.a * _score) + (b * ((params.c * required_ratio) + (d * recommended_ratio)));
                if (doc['@type'].value == 'ResourceCatalog') {
                    score *= params.boost_factor;
                }
                return score;
            """,
            "params": {
                "a": 0.8,
                "c": 0.75,
                "boost_factor": 1000.0
            }
        }

        function_score_query = Q("function_score", script_score={
                                 "script": custom_function_script}, boost_mode="replace")
        search = search.query(function_score_query)

        # apply extra-filtering if present
        if options.extra_filter:
            search = search.query("query_string", query=options.extra_filter)

        # apply hist aggregation if requested
        if options.hist:
            a = A(
                "date_histogram",
                field=options.hist,
                calendar_interval=options.hist_interval,
                min_doc_count=1,
            )
            search.aggs.bucket("hist_dates", a)

        # apply suggester if requested
        if options.suggester:
            phrase_suggester = {
                "field": "name.phrase_suggester",
                "size": 3,
                "direct_generator": [{"field": "name.phrase_suggester", "suggest_mode": "always"}],
                "max_errors": 2,
                "highlight": {"pre_tag": "<em>", "post_tag": "</em>"},
            }
            search = search.suggest("nde_suggester", options.suggester, phrase=phrase_suggester)

        # apply multi-term aggregation if requested
        if options.multi_terms_fields:
            multi_terms_size = options.get('multi_terms_size', 10)
            multi_terms_agg = A(
                "multi_terms",
                terms=[{"field": field} for field in options.multi_terms_fields],
                size=multi_terms_size
            )
            search.aggs.bucket("multi_terms_agg", multi_terms_agg)

        # Manage _meta field visibility
        if options.show_meta:
            search = search.source(includes=["*"], excludes=[])
        else:
            search = search.source(excludes=["_meta"])

        # Add lineage aggregations
        if options.get('lineage'):
            lineage_taxon_id = options.get('lineage')

            # Existing lineage aggregation
            lineage_agg = A('nested', path='_meta.lineage')

            children_of_lineage_filter = A(
                'filter', term={'_meta.lineage.parent_taxon': lineage_taxon_id})

            taxon_ids_terms = A('terms', field='_meta.lineage.taxon')
            taxon_ids_terms.bucket('to_parent', A('reverse_nested'))

            children_of_lineage_filter.bucket('to_parent', A('reverse_nested'))
            children_of_lineage_filter.bucket('taxon_ids', taxon_ids_terms)

            lineage_agg.bucket('children_of_lineage',
                               children_of_lineage_filter)

            search.aggs.bucket('lineage', lineage_agg)

            # New aggregation for counting datasets based on species.identifier and infectiousAgent.identifier
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
