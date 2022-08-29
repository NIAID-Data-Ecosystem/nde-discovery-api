from biothings.web.query import ESQueryBuilder
from elasticsearch_dsl import Search, Q


class NDEQueryBuilder(ESQueryBuilder):

    # https://docs.biothings.io/en/latest/_modules/biothings/web/query/builder.html#ESQueryBuilder.default_string_query
    def default_string_query(self, q, options):

        search = Search()
        q = q.strip()

        # elasticsearch query string syntax
        # https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html#query-string-syntax
        if ":" in q or " AND " in q or " OR " in q:
            search = search.query('query_string', query=q, default_operator="AND", lenient=True)

        # term search
        elif q.startswith('"') and q.endswith('"'):
            queries = [
                # term query
                Q('term', _id={"value": q.strip('"'), "boost": 5}),
                # query string
                Q('query_string', query=q, default_operator="AND", lenient=True)
            ]

            search = search.query('dis_max', queries=queries)

        # simple text search
        else:
            queries = [
                # term query
                Q('term', _id={"value": q, "boost": 5}),
                # query string
                Q('query_string', query=q, default_operator="AND", lenient=True),
            ]

            # check if q contains wildcards if not add wildcard query to every word
            if not ("*" in q or "?" in q):
                wc_query = Q('query_string', query='* '.join(q.split()) + '*', default_operator="AND", boost=.5, lenient=True)
                queries.append(wc_query)

            search = search.query('dis_max', queries=queries)

        # # terms to filter
        # terms = {"@type": ["Dataset", "ComputationalTool"]}
        # # we need to use the filter clause because we do not want the term scores to be calculated
        # search = search.filter('terms', **terms)

        return search

    def apply_extras(self, search, options):
        # We only want those of type Dataset or ComputationalTool. Terms to filter
        terms = {"@type": ["Dataset", "ComputationalTool"]}
        search = search.filter('terms', **terms)

        # apply extra-filtering for frontend to avoid adding unwanted wildcards on certain queries
        if options.extra_filter:
            search = search.query("query_string", query=options.extra_filter)

        return super().apply_extras(search, options)
