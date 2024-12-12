import json

from biothings.web.query import ESQueryBuilder, ESResultFormatter
from elasticsearch_dsl import A, Q, Search


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

        return super().apply_extras(search, options)


class NDEFormatter(ESResultFormatter):
    def transform_aggs(self, res):
        for facet in res:
            res[facet]["_type"] = "terms"  # a type of ES Bucket Aggs
            res[facet]["terms"] = res[facet].pop("buckets")
            # Quick fix b/c hist aggregation is not a nested aggs and does not contain sum_other_doc_count and doc_count_error_upper_bound
            res[facet]["other"] = res[facet].pop("sum_other_doc_count", 0)
            res[facet]["missing"] = res[facet].pop("doc_count_error_upper_bound", 0)

            count = 0

            for bucket in res[facet]["terms"]:
                bucket["count"] = bucket.pop("doc_count")
                bucket["term"] = bucket.pop("key")
                if "key_as_string" in bucket:
                    bucket["term"] = bucket.pop("key_as_string")
                count += bucket["count"]

                # nested aggs
                for agg_k in list(bucket.keys()):
                    if isinstance(bucket[agg_k], dict):
                        bucket.update(self.transform_aggs(dict({agg_k: bucket[agg_k]})))

            res[facet]["total"] = count

        return res
