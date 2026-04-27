import asyncio
import importlib
import json
import time
from functools import lru_cache

import boto3
from biothings.web.query import ESQueryBuilder, ESResultFormatter
from biothings.web.query.engine import AsyncESQueryBackend
from botocore.config import Config as BotoConfig
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


def _load_runtime_config():
    """Load the Biothings runtime config module.

    The API server loads `config.py` as module name `config`. Importing it at
    module import time would create a circular dependency, so we import lazily.
    """
    return importlib.import_module("config")


def _find_first_query_string_query(query_obj):
    if isinstance(query_obj, dict):
        qs = query_obj.get("query_string")
        if isinstance(qs, dict) and "query" in qs:
            return qs.get("query")
        for value in query_obj.values():
            found = _find_first_query_string_query(value)
            if found:
                return found
    elif isinstance(query_obj, list):
        for item in query_obj:
            found = _find_first_query_string_query(item)
            if found:
                return found
    return None


def _looks_like_advanced_query_string(q: str) -> bool:
    q = (q or "").strip()
    if not q:
        return False
    return (":" in q) or (" AND " in q) or (" OR " in q) or ("(" in q and ")" in q)


# Sentinel query values that represent "browse all" — not a real search intent.
# AI / vector search should fall back to standard search for these.
_AI_SEARCH_PASSTHROUGH_QUERIES = frozenset({"", "__all__", "__any__", "*"})


class _TTLCache:
    def __init__(self, *, maxsize: int, ttl_s: float):
        self.maxsize = int(maxsize)
        self.ttl_s = float(ttl_s)
        self._data = {}

    def get(self, key):
        now = time.monotonic()
        item = self._data.get(key)
        if not item:
            return None
        expires_at, value = item
        if expires_at < now:
            self._data.pop(key, None)
            return None
        return value

    def set(self, key, value):
        if self.maxsize <= 0 or self.ttl_s <= 0:
            return
        # crude eviction: clear all when we exceed maxsize
        if len(self._data) >= self.maxsize:
            self._data.clear()
        self._data[key] = (time.monotonic() + self.ttl_s, value)


@lru_cache(maxsize=4)
def _load_tokenizer(tokenizer_id: str):
    from tokenizers import Tokenizer
    return Tokenizer.from_pretrained(tokenizer_id)


class _SageMakerEmbeddingClient:
    def __init__(
        self,
        endpoint_name: str,
        region: str,
        content_type: str,
        accept: str,
        timeout_s: int,
        tokenizer_id: str | None = None,
        max_tokens: int = 512,
    ):
        self.endpoint_name = endpoint_name
        self.region = region
        self.content_type = content_type
        self.accept = accept
        self.timeout_s = int(timeout_s)
        self.tokenizer_id = tokenizer_id
        self.max_tokens = int(max_tokens)

        self._client = boto3.client(
            "sagemaker-runtime",
            region_name=self.region,
            config=BotoConfig(
                connect_timeout=self.timeout_s,
                read_timeout=self.timeout_s,
                retries={"max_attempts": 2, "mode": "standard"},
            ),
        )

    def _truncate(self, text: str) -> str:
        if not self.tokenizer_id:
            return text
        tokenizer = _load_tokenizer(self.tokenizer_id)
        # Encode without special tokens so truncation is based on raw content
        # tokens; the endpoint adds [CLS]/[SEP] itself (2 tokens), so reserve
        # headroom below max_tokens.
        encoding = tokenizer.encode(text, add_special_tokens=False)
        ids = encoding.ids
        budget = self.max_tokens - 2
        if len(ids) <= budget:
            return text
        return tokenizer.decode(ids[:budget], skip_special_tokens=True)

    def embed_one(self, text: str):
        text = self._truncate(text)
        payload = json.dumps({"inputs": text}).encode("utf-8")
        response = self._client.invoke_endpoint(
            EndpointName=self.endpoint_name,
            ContentType=self.content_type,
            Accept=self.accept,
            Body=payload,
        )
        body = response["Body"].read()
        parsed = json.loads(body)
        if isinstance(parsed, list) and parsed and isinstance(parsed[0], list):
            return parsed[0]
        return parsed


class NDEESQueryBackend(AsyncESQueryBackend):
    """Custom ES backend enabling AI (vector) search while preserving facets.

    When `use_ai_search=true`, we:
      1) embed the user's query via SageMaker
      2) issue an Elasticsearch kNN query using `knn.filter` to apply the same
         type restrictions and optional `extra_filter`/`filter` constraints.

    Aggregations/facets remain supported because we keep the `aggs` section
    from
    the Search object built by the existing query builder.
    """

    def __init__(
        self,
        client,
        indices=None,
        scroll_time="1m",
        scroll_size=1000,
        multisearch_concurrency=5,
        total_hits_as_int=True,
    ):
        super().__init__(
            client,
            indices=indices,
            scroll_time=scroll_time,
            scroll_size=scroll_size,
            multisearch_concurrency=multisearch_concurrency,
            total_hits_as_int=total_hits_as_int,
        )

    @staticmethod
    @lru_cache(maxsize=1)
    def _embedding_client_from_config():
        cfg = _load_runtime_config()
        provider = getattr(cfg, "AI_EMBEDDING_PROVIDER", "sagemaker")
        if provider != "sagemaker":
            raise ValueError(f"Unsupported AI_EMBEDDING_PROVIDER: {provider}")
        return _SageMakerEmbeddingClient(
            endpoint_name=getattr(cfg, "AI_SAGEMAKER_ENDPOINT_NAME"),
            region=getattr(cfg, "AI_SAGEMAKER_REGION"),
            content_type=getattr(
                cfg, "AI_SAGEMAKER_CONTENT_TYPE", "application/json"),
            accept=getattr(cfg, "AI_SAGEMAKER_ACCEPT", "application/json"),
            timeout_s=getattr(cfg, "AI_SAGEMAKER_TIMEOUT", 30),
            tokenizer_id=getattr(cfg, "AI_EMBEDDING_TOKENIZER", None),
            max_tokens=getattr(cfg, "AI_EMBEDDING_MAX_TOKENS", 512),
        )

    @staticmethod
    def _build_type_filter():
        filter_conditions = [
            {"terms": {
                "@type": ["Dataset", "ResourceCatalog", "Sample", "DataCollection"]}},
        ]
        computational_tool_condition = {
            "bool": {
                "must": [
                    {"term": {"@type": "ComputationalTool"}},
                    {"term": {"includedInDataCatalog.name": "bio.tools"}},
                ]
            }
        }
        return {
            "bool": {
                "should": filter_conditions + [computational_tool_condition],
                "minimum_should_match": 1,
            }
        }

    @staticmethod
    def _query_string_filter(query: str):
        return {
            "query_string": {
                "query": query,
                "default_operator": "AND",
                "lenient": True,
            }
        }

    @staticmethod
    def _exists_filter(field: str):
        return {"exists": {"field": field}}

    @staticmethod
    def _extract_total_hits_value(total_obj) -> int:
        if isinstance(total_obj, dict):
            return int(total_obj.get("value") or 0)
        try:
            return int(total_obj or 0)
        except Exception:
            return 0

    async def _knn_sample_ids(
        self,
        *,
        index: str,
        vector_field: str,
        query_vector,
        knn_k: int,
        num_candidates: int,
        knn_filter,
        total_hits_as_int: bool,
    ):
        """Run a lightweight kNN request to retrieve top-k document IDs.

        This is used to compute aggregations/facets over the AI neighborhood
        (kNN top-k) instead of over a global `match_all` set.
        """
        body = {
            "size": int(knn_k),
            "_source": False,
            "stored_fields": [],
            "track_total_hits": False,
            "knn": {
                "field": vector_field,
                "query_vector": query_vector,
                "k": int(knn_k),
                "num_candidates": int(num_candidates),
                "filter": knn_filter,
            },
        }
        if total_hits_as_int:
            body["rest_total_hits_as_int"] = True

        res = await self.client.search(index=index, **body)
        if hasattr(res, "body"):
            res = res.body
        hits = (
            res.get("hits", {}).get("hits", [])
            if isinstance(res, dict)
            else []
        )
        ids = [h.get("_id")
               for h in hits if isinstance(h, dict) and h.get("_id")]
        return ids

    async def _execute_ai_search(self, query: Search, **options):
        cfg = _load_runtime_config()

        # Use the raw user query attached by NDEQueryBuilder.build() so we
        # never accidentally embed filter text (extra_filter, etc.) that the
        # builder folds into the Search body.
        raw_q = str(getattr(query, '_nde_raw_q', '') or '').strip()

        if raw_q in _AI_SEARCH_PASSTHROUGH_QUERIES:
            return await super().execute(query, **options)

        user_q = raw_q

        # Cache embeddings because the UI can trigger many facet calls
        # for the same query text.
        embed_ttl = float(getattr(cfg, "AI_EMBEDDING_CACHE_TTL_S", 300))
        embed_max = int(getattr(cfg, "AI_EMBEDDING_CACHE_MAXSIZE", 2048))
        if not hasattr(self, "_embed_cache"):
            self._embed_cache = _TTLCache(maxsize=embed_max, ttl_s=embed_ttl)

        embed_key = str(user_q)
        query_vector = self._embed_cache.get(embed_key)
        if query_vector is None:
            embedding_client = self._embedding_client_from_config()
            query_vector = await asyncio.to_thread(
                embedding_client.embed_one,
                str(user_q),
            )
            self._embed_cache.set(embed_key, query_vector)

        dims_expected = int(getattr(cfg, "AI_SEARCH_VECTOR_DIMS", 768))
        if not isinstance(query_vector, list) or len(query_vector) != dims_expected:
            got_dims = len(query_vector) if isinstance(
                query_vector, list) else "N/A"
            raise ValueError(
                f"Embedding dims mismatch (got {got_dims}, expected {dims_expected})."
            )

        vector_field = getattr(cfg, "AI_SEARCH_VECTOR_FIELD")
        track_total_hits = bool(
            getattr(cfg, "AI_SEARCH_TRACK_TOTAL_HITS", True))

        # Preserve all non-query pieces (aggs/suggest/_source/sort/etc.)
        # but drop the original text query — we use kNN scoring instead.
        # IMPORTANT: Do NOT set query={"match_all": {}} here.  ES treats
        # top-level `knn` + `query` as a hybrid (union) search.  match_all
        # would cause every document in the index to appear in results,
        # bypassing kNN filters when `sort` is used and inflating totals.
        search_body = query.to_dict()
        es_kwargs = dict(search_body)
        es_kwargs.pop("query", None)

        requested_size = int(
            es_kwargs.get("size", getattr(cfg, "AI_SEARCH_MAX_HITS", 10))
        )
        facet_size_opt = options.get("facet_size")
        facet_size = 10 if facet_size_opt is None else int(facet_size_opt)
        has_aggs = bool(es_kwargs.get("aggs"))
        original_aggs = es_kwargs.get("aggs") if has_aggs else None

        base_k = int(getattr(cfg, "AI_SEARCH_KNN_K", 10))
        # Heuristic: to make totals/facets useful by default, ensure the kNN
        # window is large enough even when the client requests size=0 or size=10.
        min_total_default = int(getattr(cfg, "AI_SEARCH_MIN_TOTAL_K", 200))
        min_total_k = min_total_default if track_total_hits else base_k
        knn_k = max(base_k, min_total_k, requested_size)
        # Only increase k for facets when we actually need buckets.
        # Many UI calls use `facet_size=0` (often with +/- _exists_) purely to
        # get totals; bumping k to 2000 there is wasted work.
        if has_aggs and facet_size > 0:
            # Facet calls can request very large facet_size (e.g. 1000). Using
            # `facet_size*10` explodes kNN work (10k+), which is too slow.
            # Use a dedicated, capped neighborhood size for facets.
            facet_knn_k = int(getattr(cfg, "AI_SEARCH_FACET_K", 2000))
            facet_knn_k_max = int(
                getattr(cfg, "AI_SEARCH_FACET_K_MAX", facet_knn_k))
            facet_target = max(facet_knn_k, base_k, requested_size)
            facet_target = min(facet_target, max(
                facet_knn_k_max, base_k, requested_size))
            knn_k = max(knn_k, facet_target)

        num_candidates = int(
            getattr(cfg, "AI_SEARCH_KNN_NUM_CANDIDATES", 1000))
        num_candidates = max(num_candidates, knn_k)

        # In AI mode, only return documents that are eligible for vector search
        # (have an embedding) and satisfy the API's type constraints.
        base_filters = [
            self._build_type_filter(),
            self._exists_filter(vector_field),
        ]

        # If the user supplied an advanced query_string (e.g. fielded constraints
        # like @type:"ResourceCatalog"), treat it as a hard filter.
        # This preserves expected filtering semantics while still using kNN for ranking.
        if _looks_like_advanced_query_string(str(user_q)):
            base_filters.append(self._query_string_filter(str(user_q)))

        extra_filter = options.get("extra_filter")

        # Standard Biothings filter param (query_string filter)
        std_filter = options.get("filter")
        if std_filter:
            base_filters.append(self._query_string_filter(str(std_filter)))

        # Include extra_filter in the kNN pre-filter so the
        # neighbourhood is scoped to the user's facet selection.
        # This ensures filtered queries always find matching docs
        # (e.g. ResourceCatalogs for "covid") rather than being
        # limited to a neutral neighbourhood that may exclude rare types.
        knn_filters = list(base_filters)
        if extra_filter:
            knn_filters.append(self._query_string_filter(str(extra_filter)))
        knn_filter = {"bool": {"filter": knn_filters}}

        es_kwargs["knn"] = {
            "field": vector_field,
            "query_vector": query_vector,
            "k": knn_k,
            "num_candidates": num_candidates,
            "filter": knn_filter,
        }

        # -----------------------------------------------------------------
        # Sampling: collect kNN neighbourhood IDs when aggregations are
        # present.  The knn_filter already includes extra_filter, so
        # sampled IDs reflect the filtered neighbourhood.  Facet counts
        # and hit totals both derive from this same scoped set.
        # -----------------------------------------------------------------
        agg_override = None
        sample_ids = None
        sample_ids_count = None

        if has_aggs:
            es_kwargs.pop("aggs", None)

            index = self.indices[options.get("biothing_type")]
            index = self.adjust_index(index, query, **options)

            ids_ttl = float(
                getattr(cfg, "AI_SEARCH_IDS_CACHE_TTL_S", 30))
            ids_max = int(
                getattr(cfg, "AI_SEARCH_IDS_CACHE_MAXSIZE", 512))
            if not hasattr(self, "_ids_cache"):
                self._ids_cache = _TTLCache(
                    maxsize=ids_max, ttl_s=ids_ttl)

            ids_cache_key = (
                vector_field,
                str(user_q),
                str(std_filter or ""),
                str(extra_filter or ""),
                int(knn_k),
                int(num_candidates),
            )
            sample_ids = self._ids_cache.get(ids_cache_key)
            if sample_ids is None:
                sample_ids = await self._knn_sample_ids(
                    index=index,
                    vector_field=vector_field,
                    query_vector=query_vector,
                    knn_k=knn_k,
                    num_candidates=num_candidates,
                    knn_filter=knn_filter,
                    total_hits_as_int=self.total_hits_as_int,
                )
                self._ids_cache.set(ids_cache_key, sample_ids)
            sample_ids_count = len(sample_ids)

            # --- Aggregations -------------------------------------------
            if sample_ids:
                agg_body = {
                    "size": 0,
                    "query": {
                        "bool": {"filter": [
                            {"ids": {"values": sample_ids}},
                        ]}},
                    "aggs": original_aggs,
                    "track_total_hits": False,
                }
                if self.total_hits_as_int:
                    agg_body["rest_total_hits_as_int"] = True
                agg_res = await self.client.search(
                    index=index, **agg_body)
                if hasattr(agg_res, "body"):
                    agg_res = agg_res.body
                if isinstance(agg_res, dict):
                    agg_override = (
                        agg_res.get("aggregations")
                        or agg_res.get("aggs")
                    )
            else:
                agg_override = {}

            # --- Facet-only (size=0) short-circuit ----------------------
            if requested_size <= 0:
                return {
                    "hits": {
                        "total": int(sample_ids_count or 0),
                        "max_score": None,
                        "hits": [],
                    },
                    "aggregations": agg_override or {},
                }

        # -----------------------------------------------------------------
        # Direct kNN path (no extra_filter)
        # -----------------------------------------------------------------
        if self.total_hits_as_int:
            es_kwargs["rest_total_hits_as_int"] = True
        if track_total_hits:
            es_kwargs["track_total_hits"] = True

        if "from" in es_kwargs:
            es_kwargs["from_"] = es_kwargs.pop("from")

        index = self.indices[options.get("biothing_type")]
        index = self.adjust_index(index, query, **options)
        res = await self.client.search(index=index, **es_kwargs)
        if hasattr(res, "body"):
            res = res.body
        if isinstance(res, dict) and isinstance(
                res.get("hits"), dict):
            if has_aggs and isinstance(sample_ids_count, int):
                res["hits"]["total"] = int(sample_ids_count)

        if (has_aggs and isinstance(res, dict)
                and agg_override is not None):
            res["aggregations"] = agg_override

        return res

    async def execute(self, query, **options):
        if options.get("use_ai_search") and isinstance(query, Search):
            return await self._execute_ai_search(query, **options)
        return await super().execute(query, **options)


class NDEQueryBuilder(ESQueryBuilder):
    # https://docs.biothings.io/en/latest/_modules/biothings/web/query/builder.html#ESQueryBuilder.default_string_query

    def build(self, q=None, **options):
        search = super().build(q, **options)
        if isinstance(search, Search):
            search._nde_raw_q = q
        return search

    def default_string_query(self, q, options):
        search = Search()
        q = q.strip()

        # elasticsearch query string syntax
        # https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html#query-string-syntax
        if ":" in q or " AND " in q or " OR " in q:
            search = search.query("query_string", query=q,
                                  default_operator="AND", lenient=True)

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
                query="* ".join(q.split()) + "*",
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

        return queries

    def apply_extras(self, search, options):
        # We only want those of type Dataset or ComputationalTool. Terms to filter
        # terms = {"@type": ["Dataset", "ComputationalTool"]}

        # Temporary change for launch of the portal as requested by NIAID
        # terms = {"@type": ["Dataset", "ResourceCatalog"]}
        # search = search.filter("terms", **terms)

        # Filter to allow @type Dataset, ResourceCatalog and ComputationalTool only from Bio.tools
        filter_conditions = [
            # Include Dataset and ResourceCatalog
            {"terms": {
                "@type": ["Dataset", "ResourceCatalog", "Sample", "DataCollection"]}},
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
            "bool",
            should=filter_conditions + [computational_tool_condition],
            minimum_should_match=1,
        )

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

        # hide _meta object and always exclude the vector embedding field
        cfg = _load_runtime_config()
        vector_field = getattr(cfg, "AI_SEARCH_VECTOR_FIELD", None)
        excludes = []
        if not options.show_meta:
            excludes.append("_meta")
        if vector_field:
            excludes.append(vector_field)

        if options.show_meta:
            search = search.source(includes=["*"], excludes=excludes)
        else:
            search = search.source(excludes=excludes)

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

        # Add companion "missing" aggregations for each facet so the
        # formatter can report the true count of documents without the field.
        for agg in options.aggs or []:
            field = agg.split("(")[0] if "(" in agg else agg
            if field:
                search.aggs.bucket(
                    f"__missing__{field}", "missing", field=field
                )

        return super().apply_extras(search, options)


class NDEFormatter(ESResultFormatter):
    def transform_aggs(self, res):
        # Extract companion "missing" aggregation counts added by the builder.
        missing_counts = {}
        for key in list(res.keys()):
            if key.startswith("__missing__"):
                facet_name = key[len("__missing__"):]
                missing_counts[facet_name] = res.pop(key).get("doc_count", 0)

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

        # Override missing counts with actual values from companion aggs.
        for facet_name, count in missing_counts.items():
            if facet_name in res:
                res[facet_name]['missing'] = count

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
