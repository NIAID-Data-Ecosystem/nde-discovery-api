import asyncio
import importlib
import json
import re
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


_EXISTS_ONLY_RE = re.compile(r"^\s*(?P<neg>-)?_exists_:(?P<field>.+?)\s*$")


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


class _SageMakerEmbeddingClient:
    def __init__(
        self,
        endpoint_name: str,
        region: str,
        content_type: str,
        accept: str,
        timeout_s: int,
    ):
        self.endpoint_name = endpoint_name
        self.region = region
        self.content_type = content_type
        self.accept = accept
        self.timeout_s = int(timeout_s)

        self._client = boto3.client(
            "sagemaker-runtime",
            region_name=self.region,
            config=BotoConfig(
                connect_timeout=self.timeout_s,
                read_timeout=self.timeout_s,
                retries={"max_attempts": 2, "mode": "standard"},
            ),
        )

    def embed_one(self, text: str):
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
    def _parse_exists_only_filter(extra_filter: str):
        """Detect `_exists_:field` / `-_exists_:field` patterns.

        Returns:
            (is_negated: bool, field: str) or None
        """
        if not extra_filter:
            return None
        m = _EXISTS_ONLY_RE.match(str(extra_filter))
        if not m:
            return None
        return (bool(m.group("neg")), m.group("field").strip())

    @staticmethod
    def _extract_total_hits_value(total_obj) -> int:
        if isinstance(total_obj, dict):
            return int(total_obj.get("value") or 0)
        try:
            return int(total_obj or 0)
        except Exception:
            return 0

    async def _count_exists_in_ids(self, *, index: str, ids, field: str, cache_key):
        """Count docs (within an IDs set) that have `field` present."""
        cfg = _load_runtime_config()
        ttl_s = float(getattr(cfg, "AI_SEARCH_IDS_CACHE_TTL_S", 30))
        maxsize = int(getattr(cfg, "AI_SEARCH_IDS_CACHE_MAXSIZE", 512))
        if not hasattr(self, "_exists_count_cache"):
            self._exists_count_cache = _TTLCache(maxsize=maxsize, ttl_s=ttl_s)

        key = ("exists_count", cache_key, str(field))
        cached = self._exists_count_cache.get(key)
        if cached is not None:
            return int(cached)

        body = {
            "size": 0,
            "query": {
                "bool": {
                    "filter": [
                        {"ids": {"values": list(ids)}},
                        {"exists": {"field": field}},
                    ]
                }
            },
            "track_total_hits": True,
        }
        if self.total_hits_as_int:
            body["rest_total_hits_as_int"] = True

        res = await self.client.search(index=index, **body)
        if hasattr(res, "body"):
            res = res.body
        total_obj = (
            res.get("hits", {}).get("total", 0)
            if isinstance(res, dict)
            else 0
        )
        total_val = self._extract_total_hits_value(total_obj)
        self._exists_count_cache.set(key, total_val)
        return int(total_val)

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

        search_body = query.to_dict()
        user_q = _find_first_query_string_query(search_body.get("query"))
        if not user_q:
            # fall back to whatever builder produced for q; if we can't find it,
            # skip AI search to avoid returning meaningless results.
            return await super().execute(query, **options)

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

        # kNN uses base structural filters only.  extra_filter is applied
        # AFTER kNN sampling so that facet selections narrow results
        # within the semantic neighbourhood instead of changing it.
        knn_filter = {"bool": {"filter": base_filters}}

        es_kwargs["knn"] = {
            "field": vector_field,
            "query_vector": query_vector,
            "k": knn_k,
            "num_candidates": num_candidates,
            "filter": knn_filter,
        }

        # -----------------------------------------------------------------
        # Sampling: collect kNN neighbourhood IDs when aggregations or
        # extra_filter are present.  Aggregation counts and filtered
        # hits are both scoped to this neutral neighbourhood so that
        # facet counts and hit totals always agree.
        # -----------------------------------------------------------------
        agg_override = None
        sample_ids = None
        sample_ids_count = None

        if has_aggs or extra_filter:
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

            # Cache key intentionally omits extra_filter so that the
            # same neighbourhood is reused regardless of facet
            # selections.
            ids_cache_key = (
                vector_field,
                str(user_q),
                str(std_filter or ""),
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
            if has_aggs:
                if sample_ids:
                    agg_filters = [
                        {"ids": {"values": sample_ids}}]
                    if extra_filter:
                        agg_filters.append(
                            self._query_string_filter(
                                str(extra_filter)))
                    agg_body = {
                        "size": 0,
                        "query": {
                            "bool": {"filter": agg_filters}},
                        "aggs": original_aggs,
                        "track_total_hits": False,
                    }
                    if self.total_hits_as_int:
                        agg_body[
                            "rest_total_hits_as_int"] = True
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
            if has_aggs and requested_size <= 0:
                if extra_filter and sample_ids:
                    count_body = {
                        "size": 0,
                        "query": {"bool": {"filter": [
                            {"ids": {"values": sample_ids}},
                            self._query_string_filter(
                                str(extra_filter)),
                        ]}},
                        "track_total_hits": True,
                    }
                    if self.total_hits_as_int:
                        count_body[
                            "rest_total_hits_as_int"] = True
                    count_res = await self.client.search(
                        index=index, **count_body)
                    if hasattr(count_res, "body"):
                        count_res = count_res.body
                    total = self._extract_total_hits_value(
                        count_res.get("hits", {}).get(
                            "total", 0))
                else:
                    total = int(sample_ids_count or 0)
                return {
                    "hits": {
                        "total": int(total),
                        "max_score": None,
                        "hits": [],
                    },
                    "aggregations": agg_override or {},
                }

            # --- Hits with extra_filter (two-phase) ---------------------
            # Filter the neutral neighbourhood by extra_filter so that
            # hit totals are consistent with facet counts.
            if extra_filter and sample_ids is not None:
                if not sample_ids:
                    res = {"hits": {
                        "total": 0, "max_score": None, "hits": []}}
                    if has_aggs and agg_override is not None:
                        res["aggregations"] = agg_override
                    return res

                hit_filters = [
                    {"ids": {"values": sample_ids}},
                    self._query_string_filter(str(extra_filter)),
                ]
                hit_body = {
                    "query": {"bool": {"filter": hit_filters}},
                    "size": requested_size,
                    "track_total_hits": True,
                }
                if self.total_hits_as_int:
                    hit_body["rest_total_hits_as_int"] = True
                from_val = es_kwargs.get(
                    "from_", es_kwargs.get("from", 0))
                if from_val:
                    hit_body["from_"] = from_val
                if "sort" in es_kwargs:
                    hit_body["sort"] = es_kwargs["sort"]
                if "_source" in es_kwargs:
                    hit_body["_source"] = es_kwargs["_source"]

                res = await self.client.search(
                    index=index, **hit_body)
                if hasattr(res, "body"):
                    res = res.body
                if (has_aggs and isinstance(res, dict)
                        and agg_override is not None):
                    res["aggregations"] = agg_override
                return res

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

    async def _execute_ai_batch_facets(self, query: Search, **options):
        """Execute a batched AI facet query with per-field kNN sampling.

        Instead of running N separate API calls (one per facet), this method:
          1. Embeds the query (shared, cached)
          2. Runs N+1 kNN samples **concurrently** — one per field with
             ``_exists_:<field>`` (matching old per-field behavior) plus
             one neutral sample for "None Specified" computation
          3. Builds a **single** ES aggregation query that combines all
             facets, each scoped to its field-specific kNN IDs

        The ``ai_facet_fields`` option must contain a comma-separated list
        of ES field names.
        """
        cfg = _load_runtime_config()

        ai_facet_fields_raw = options.get("ai_facet_fields", "")
        facet_fields = [
            f.strip()
            for f in str(ai_facet_fields_raw).split(",")
            if f.strip()
        ]
        if not facet_fields:
            return await self._execute_ai_search(query, **options)

        # --- Embed the user's query ----------------------------------------
        search_body = query.to_dict()
        user_q = _find_first_query_string_query(
            search_body.get("query")
        )
        if not user_q:
            return await super().execute(query, **options)

        embed_ttl = float(
            getattr(cfg, "AI_EMBEDDING_CACHE_TTL_S", 300)
        )
        embed_max = int(
            getattr(cfg, "AI_EMBEDDING_CACHE_MAXSIZE", 2048)
        )
        if not hasattr(self, "_embed_cache"):
            self._embed_cache = _TTLCache(
                maxsize=embed_max, ttl_s=embed_ttl
            )

        embed_key = str(user_q)
        query_vector = self._embed_cache.get(embed_key)
        if query_vector is None:
            embedding_client = self._embedding_client_from_config()
            query_vector = await asyncio.to_thread(
                embedding_client.embed_one,
                str(user_q),
            )
            self._embed_cache.set(embed_key, query_vector)

        dims_expected = int(
            getattr(cfg, "AI_SEARCH_VECTOR_DIMS", 768)
        )
        if (
            not isinstance(query_vector, list)
            or len(query_vector) != dims_expected
        ):
            got = (
                len(query_vector)
                if isinstance(query_vector, list)
                else "N/A"
            )
            raise ValueError(
                f"Embedding dims mismatch "
                f"(got {got}, expected {dims_expected})."
            )

        vector_field = getattr(cfg, "AI_SEARCH_VECTOR_FIELD")

        # --- Base kNN filters -----------------------------------------------
        base_filters = [
            self._build_type_filter(),
            self._exists_filter(vector_field),
        ]
        if _looks_like_advanced_query_string(str(user_q)):
            base_filters.append(
                self._query_string_filter(str(user_q))
            )

        std_filter = options.get("filter")
        if std_filter:
            base_filters.append(
                self._query_string_filter(str(std_filter))
            )

        extra_filter = options.get("extra_filter")
        # extra_filter is NOT added to base_filters — it is applied
        # after kNN sampling so facet selections narrow within the
        # semantic neighbourhood rather than changing it.

        # --- kNN parameters -------------------------------------------------
        facet_knn_k = int(
            getattr(cfg, "AI_SEARCH_FACET_K", 2000)
        )
        base_k = int(getattr(cfg, "AI_SEARCH_KNN_K", 10))
        knn_k = max(facet_knn_k, base_k)
        num_candidates = int(
            getattr(cfg, "AI_SEARCH_KNN_NUM_CANDIDATES", 1000)
        )
        num_candidates = max(num_candidates, knn_k)

        ids_ttl = float(
            getattr(cfg, "AI_SEARCH_IDS_CACHE_TTL_S", 30)
        )
        ids_max = int(
            getattr(cfg, "AI_SEARCH_IDS_CACHE_MAXSIZE", 512)
        )
        if not hasattr(self, "_ids_cache"):
            self._ids_cache = _TTLCache(
                maxsize=ids_max, ttl_s=ids_ttl
            )

        index = self.indices[options.get("biothing_type")]
        index = self.adjust_index(index, query, **options)

        # --- Per-field kNN sampling (concurrent) ----------------------------
        # Each field gets its own kNN neighborhood with _exists_:<field>
        # in the filter, matching the old per-field API behavior.
        async def _sample_with_exists(field_name):
            field_filters = list(base_filters) + [
                self._exists_filter(field_name),
            ]
            knn_filt = {"bool": {"filter": field_filters}}
            ck = (
                vector_field,
                str(user_q),
                str(std_filter or ""),
                "_exists_:" + field_name,
                int(knn_k),
                int(num_candidates),
            )
            ids = self._ids_cache.get(ck)
            if ids is None:
                ids = await self._knn_sample_ids(
                    index=index,
                    vector_field=vector_field,
                    query_vector=query_vector,
                    knn_k=knn_k,
                    num_candidates=num_candidates,
                    knn_filter=knn_filt,
                    total_hits_as_int=self.total_hits_as_int,
                )
                self._ids_cache.set(ck, ids)
            return ids

        async def _sample_neutral():
            knn_filt = {"bool": {"filter": base_filters}}
            ck = (
                vector_field,
                str(user_q),
                str(std_filter or ""),
                "",
                int(knn_k),
                int(num_candidates),
            )
            ids = self._ids_cache.get(ck)
            if ids is None:
                ids = await self._knn_sample_ids(
                    index=index,
                    vector_field=vector_field,
                    query_vector=query_vector,
                    knn_k=knn_k,
                    num_candidates=num_candidates,
                    knn_filter=knn_filt,
                    total_hits_as_int=self.total_hits_as_int,
                )
                self._ids_cache.set(ck, ids)
            return ids

        tasks = [_sample_with_exists(f) for f in facet_fields]
        tasks.append(_sample_neutral())
        results = await asyncio.gather(*tasks)

        per_field_ids = {}
        for i, field in enumerate(facet_fields):
            per_field_ids[field] = results[i]
        neutral_ids = results[-1]
        neutral_total = len(neutral_ids)

        # --- Build combined ES aggregation ----------------------------------
        facet_size = int(options.get("facet_size", 1000))
        aggs = {}
        has_any_ids = False

        for field in facet_fields:
            field_ids = per_field_ids[field]
            if field_ids:
                has_any_ids = True
                # Terms agg scoped to this field's kNN IDs,
                # further narrowed by extra_filter when present.
                field_agg_filters = [
                    {"ids": {"values": field_ids}},
                ]
                if extra_filter:
                    field_agg_filters.append(
                        self._query_string_filter(
                            str(extra_filter))
                    )
                aggs[field] = {
                    "filter": {
                        "bool": {"filter": field_agg_filters},
                    },
                    "aggs": {
                        "inner": {
                            "terms": {
                                "field": field,
                                "size": facet_size,
                            },
                        },
                    },
                }
            # Exists count from neutral IDs
            if neutral_ids:
                exists_filters = [
                    {"ids": {"values": neutral_ids}},
                    {"exists": {"field": field}},
                ]
                if extra_filter:
                    exists_filters.append(
                        self._query_string_filter(
                            str(extra_filter))
                    )
                aggs[field + "__exists"] = {
                    "filter": {
                        "bool": {
                            "filter": exists_filters,
                        },
                    },
                }

        if not has_any_ids:
            empty_aggs = {}
            for field in facet_fields:
                empty_aggs[field] = {
                    "doc_count_error_upper_bound": 0,
                    "sum_other_doc_count": 0,
                    "buckets": [],
                }
                empty_aggs[field + "__exists"] = {
                    "doc_count": 0,
                }
                empty_aggs[field + "__total"] = {
                    "doc_count": 0,
                }
            return {
                "hits": {
                    "total": 0,
                    "max_score": None,
                    "hits": [],
                },
                "aggregations": empty_aggs,
            }

        # --- Execute single ES query ----------------------------------------
        agg_body = {
            "size": 0,
            "aggs": aggs,
            "track_total_hits": False,
        }
        if self.total_hits_as_int:
            agg_body["rest_total_hits_as_int"] = True

        agg_res = await self.client.search(
            index=index, **agg_body
        )
        if hasattr(agg_res, "body"):
            agg_res = agg_res.body

        raw_aggs = (
            agg_res.get("aggregations")
            or agg_res.get("aggs")
            or {}
        )

        # --- Restructure: flatten filter→inner for formatter ----------------
        aggregations = {}
        for field in facet_fields:
            raw_facet = raw_aggs.get(field, {})
            inner = raw_facet.get("inner", {})
            aggregations[field] = inner

            # Per-field total: when extra_filter is active, use the
            # doc_count from the (now-filtered) agg bucket so the
            # total reflects the filtered neighbourhood.  Without
            # extra_filter, fall back to the raw sample size.
            if extra_filter:
                aggregations[field + "__total"] = {
                    "doc_count": raw_facet.get("doc_count", 0),
                }
            else:
                aggregations[field + "__total"] = {
                    "doc_count": len(
                        per_field_ids.get(field, [])),
                }

            # Exists in neutral neighbourhood
            raw_exists = raw_aggs.get(
                field + "__exists", {}
            )
            aggregations[field + "__exists"] = {
                "doc_count": raw_exists.get("doc_count", 0),
            }

        # Neutral total: when extra_filter is present, compute
        # from the exists agg counts; otherwise use raw sample size.
        if extra_filter and neutral_ids:
            # Count how many neutral IDs pass extra_filter.
            # We already have __exists counts per field, but for
            # the overall total we need a dedicated count.
            # Use the first field's __total filtered count as a
            # reasonable proxy, or run a quick count.
            # For accuracy, sum is not correct (fields overlap),
            # so we compute it directly.
            ef_count_body = {
                "size": 0,
                "query": {"bool": {"filter": [
                    {"ids": {"values": neutral_ids}},
                    self._query_string_filter(
                        str(extra_filter)),
                ]}},
                "track_total_hits": True,
            }
            if self.total_hits_as_int:
                ef_count_body[
                    "rest_total_hits_as_int"] = True
            ef_count_res = await self.client.search(
                index=index, **ef_count_body)
            if hasattr(ef_count_res, "body"):
                ef_count_res = ef_count_res.body
            resp_total = self._extract_total_hits_value(
                ef_count_res.get("hits", {}).get(
                    "total", 0))
        else:
            resp_total = int(neutral_total)

        return {
            "hits": {
                "total": int(resp_total),
                "max_score": None,
                "hits": [],
            },
            "aggregations": aggregations,
        }

    async def execute(self, query, **options):
        if options.get("use_ai_search") and isinstance(query, Search):
            if options.get("ai_facet_fields"):
                return await self._execute_ai_batch_facets(query, **options)
            return await self._execute_ai_search(query, **options)
        return await super().execute(query, **options)


class NDEQueryBuilder(ESQueryBuilder):
    # https://docs.biothings.io/en/latest/_modules/biothings/web/query/builder.html#ESQueryBuilder.default_string_query

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
