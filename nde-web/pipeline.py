from biothings.web.query import ESQueryBuilder
from elasticsearch_dsl import Search, Q


class NDEQueryBuilder(ESQueryBuilder):

    # https://docs.biothings.io/en/latest/_modules/biothings/web/query/builder.html#ESQueryBuilder.default_string_query
    def default_string_query(self, q, options):

        search = Search()
        q = q.strip()
        # terms to filter
        terms = {"@type": ["Dataset", "Computational Tool"]}

        # elasticsearch query string syntax
        # https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html#query-string-syntax
        if ":" in q or " AND " in q or " OR " in q:
            # we need to use the filter clause because we do not want the term scores to be calculated
            return super().default_string_query(q, options).filter('terms', **terms)

        # term search TODO organize
        elif q.startswith('"') and q.endswith('"'):
            qs = [
                Q('term', _id={"value": q.strip('"'), "boost": 5}), 
                Q('query_string', query=q, default_operator="AND", lenient=True)
            ]
            search = search.query('dis_max', queries=qs).filter('terms', **terms)

        # simple text search
        else:
            search = search.query('dis_max', queries=[Q('term', _id={"value": q, "boost": 5}), 
                                             Q('query_string', query=q, lenient=True)]) \
                .filter('terms', **terms)

        return search

        # # term search
        # elif q.startswith('"') and q.endswith('"'):
        #     query = {
        #         "query": {
        #             "bool": {
        #                 "must": {
        #                     "dis_max": {
        #                         "queries": [
        #                             {"term": {"_id": {"value": q.strip('"'), "boost": 5}}},
        #                             {"query_string": {"query": q, "lenient": True, "default_operator": "AND"}}
        #                         ]
        #                     }
        #                 },
        #                 "filter": {
        #                     "terms": {"@type": ["Dataset", "Computational Tool"]}
        #                 }
        #             }
        #         }
        #     }
        #     return search.from_dict(query)
        #     # simple text search
        # else:
        #     query = {
        #         "query": {
        #             "bool": {
        #                 "must": {
        #                     "dis_max": {
        #                         "queries": [
        #                             {"term": {"_id": {"value": q, "boost": 5}}},
        #                             {"query_string": {"query": q, "lenient": True}}
        #                         ]
        #                     }
        #                 },
        #                 "filter": {
        #                     "terms": {"@type": ["Dataset", "Computational Tool"]}
        #                 }
        #             }
        #         }
        #     }
        
        #     return search.from_dict(query)
