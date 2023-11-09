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
            search = search.query(
                "query_string", query=q, default_operator="AND", lenient=True
            )

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

            # check if q contains wildcards if not add wildcard query to every word
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

        # # terms to filter
        # terms = {"@type": ["Dataset", "ComputationalTool"]}
        # # we need to use the filter clause because we do not want the term scores to be calculated
        # search = search.filter('terms', **terms)

        return search

    def apply_extras(self, search, options):
        # We only want those of type Dataset or ComputationalTool. Terms to filter
        # terms = {"@type": ["Dataset", "ComputationalTool"]}

        # Temporary change for launch of the portal as requested by NIAID
        terms = {"@type": ["Dataset"]}
        search = search.filter("terms", **terms)

        # apply extra-filtering for frontend to avoid adding unwanted wildcards on certain queries
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
                    {"field": "name.phrase_suggester", "suggest_mode": "always"}
                ],
                "max_errors": 2,
                "highlight": {"pre_tag": "<em>", "post_tag": "</em>"},
            }
            search = search.suggest(
                "nde_suggester", options.suggester, phrase=phrase_suggester
            )

        # apply function score
        if options.use_metadata_score:
            function_score_query = Q(
                "function_score",
                boost_mode="sum",
                field_value_factor={"field": "metadata_score", "missing": 0},
            )
            search = search.query(function_score_query)

        # hide _meta object
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
