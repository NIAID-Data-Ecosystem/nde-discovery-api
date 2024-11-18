import logging

from biothings.web.query import ESQueryBuilder, ESResultFormatter
from elasticsearch_dsl import A, Q, Search


class NDEQueryBuilder(ESQueryBuilder):
    # https://docs.biothings.io/en/latest/_modules/biothings/web/query/builder.html#ESQueryBuilder.default_string_query

    def default_string_query(self, q, options):
        search = Search()
        q = q.strip()

        # Function to check if a field is nested
        def is_nested_field(field_name):
            # List of nested fields
            nested_fields = ['_meta.lineage.taxon',
                             '_meta.lineage.parent_taxon']
            return field_name in nested_fields

        # Check if q is a simple field:value query
        if ':' in q and not (' AND ' in q or ' OR ' in q):
            field, value = q.split(':', 1)
            if is_nested_field(field):
                # Build nested query
                nested_path = '_meta.lineage'
                nested_query = Q('nested', path=nested_path,
                                 query=Q('term', **{field: int(value)}))
                search = search.query(nested_query)
            else:
                # Normal field
                search = search.query('term', **{field: value})
        else:
            # Existing logic for complex queries
            if ":" in q or " AND " in q or " OR " in q:
                # Parse the query and handle nested fields
                queries = []
                tokens = q.split()
                for token in tokens:
                    if ':' in token:
                        field, value = token.split(':', 1)
                        if is_nested_field(field):
                            nested_path = '_meta.lineage'
                            nested_query = Q('nested', path=nested_path, query=Q(
                                'term', **{field: int(value)}))
                            queries.append(nested_query)
                        else:
                            queries.append(Q('term', **{field: value}))
                    else:
                        queries.append(Q('match', _all=token))
                search = search.query('bool', must=queries)
            elif q.startswith('"') and q.endswith('"'):
                queries = [
                    Q("term", _id={"value": q.strip('"'), "boost": 5}),
                    Q("term", name={"value": q.strip('"'), "boost": 5}),
                    Q("query_string", query=q, default_operator="AND", lenient=True),
                ]
                search = search.query("dis_max", queries=queries)
            else:
                queries = self.build_queries(q, None)
                search = search.query("dis_max", queries=queries)

        return search

    def build_queries(self, q, custom_function_script):
        queries = [
            Q("function_score", query=Q("term", _id={"value": q, "boost": 5}),
              script_score={"script": custom_function_script} if custom_function_script else None, boost_mode="replace"),
            Q("function_score", query=Q("term", name={"value": q, "boost": 5}),
              script_score={"script": custom_function_script} if custom_function_script else None, boost_mode="replace"),
            Q("function_score", query=Q("query_string", query=q, default_operator="AND", lenient=True),
              script_score={"script": custom_function_script} if custom_function_script else None, boost_mode="replace"),
        ]
        # check if q contains wildcards if not add wildcard query to every word
        if not ("*" in q or "?" in q):
            wc_query = Q(
                "query_string",
                # original
                # query="* ".join(q.split()) + "*",
                # * before
                # query="*" + "* ".join(q.split()) + "*",
                # ? before
                query="* ".join(["?" + w for w in q.split()]) + "*",
                default_operator="AND",
                boost=0.5,
                lenient=True,
            )
            wc_function_score_query = Q("function_score", query=wc_query,
                                        script_score={"script": custom_function_script} if custom_function_script else None, boost_mode="replace")
            queries.append(wc_function_score_query)

        # Remove function_score wrapping if custom_function_script is None
        if custom_function_script is None:
            queries = [Q(q.query) for q in queries]
        # # terms to filter
        # terms = {"@type": ["Dataset", "ComputationalTool"]}
        # # we need to use the filter clause because we do not want the term scores to be calculated
        # search = search.filter('terms', **terms)

        return queries

    def apply_extras(self, search, options):
        logging.info(options)
        # We only want those of type Dataset or ComputationalTool. Terms to filter
        terms = {"@type": ["Dataset", "ComputationalTool", "ResourceCatalog"]}
        # Temporary change for launch of the portal as requested by NIAID
        # terms = {"@type": ["Dataset"]}
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
                "direct_generator": [{"field": "name.phrase_suggester", "suggest_mode": "always"}],
                "max_errors": 2,
                "highlight": {"pre_tag": "<em>", "post_tag": "</em>"},
            }
            search = search.suggest(
                "nde_suggester", options.suggester, phrase=phrase_suggester)

        # apply function score
        if options.use_metadata_score:
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
        else:
            functions = [
                {"filter": {"term": {"@type": "ResourceCatalog"}}, "weight": 1000}
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

        # hide _meta object
        if options.show_meta:
            search = search.source(includes=["*"], excludes=[])
        else:
            search = search.source(excludes=["_meta"])

        # spam filter
        spam_filter = Q(
            "bool",
            should=[
                Q("bool", must=[Q("match", name="keto"),
                  Q("match", name="gummies")]),
                Q("bool", must=[Q("match", description="keto"),
                  Q("match", description="gummies")]),
            ],
            minimum_should_match=1
        )
        search = search.exclude(spam_filter)

        if options.get('lineage'):
            lineage_taxon_id = options.get('lineage')
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

        return super().apply_extras(search, options)


class NDEFormatter(ESResultFormatter):
    def transform_aggs(self, res):
        def transform_agg(agg_res):
            # Handle 'terms' aggregations with 'buckets'
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
                    # Recursively transform nested aggregations in the bucket
                    for k in list(bucket.keys()):
                        if isinstance(bucket[k], dict):
                            transform_agg(bucket[k])
                agg_res['total'] = count
            else:
                # Handle other types of aggregations (e.g., 'nested', 'filter')
                for k in list(agg_res.keys()):
                    if isinstance(agg_res[k], dict):
                        transform_agg(agg_res[k])

        # Start the recursive transformation
        transform_agg(res)
        return res
