import hashlib
import json
import logging
import os
import random
from functools import lru_cache
from typing import Callable, Dict, List, Optional, Sequence

import requests
from elasticsearch_dsl import Q, Search

try:
    from biothings import config as bt_config
except ImportError:  # pragma: no cover - falls back to project config/env
    try:
        import config as bt_config  # type: ignore
    except ImportError:
        bt_config = None

logger = logging.getLogger(__name__)


def get_ai_setting(name: str, default=None):
    """Fetch AI configuration from biothings config first, then env vars."""

    if bt_config and hasattr(bt_config, name):
        value = getattr(bt_config, name)
        if value is not None:
            return value
    env_value = os.getenv(name)
    if env_value is not None:
        return env_value
    return default


class _EmbeddingClientProtocol:
    """Minimal protocol used by AiSearchBuilder for typing and duck-typing."""

    def embed(self, text: str) -> Sequence[float]:
        raise NotImplementedError  # pragma: no cover - interface definition


class SageMakerEmbeddingClient(_EmbeddingClientProtocol):
    """Embedding client that invokes an AWS SageMaker real-time endpoint."""

    def __init__(
        self,
        endpoint_name: str,
        region_name: Optional[str] = None,
        *,
        content_type: str = "application/json",
        accept: str = "application/json",
        timeout: int = 30,
    ) -> None:
        try:
            import boto3
            from botocore.config import Config
        # pragma: no cover - optional dependency
        except ImportError as exc:
            raise RuntimeError(
                "boto3 is required for the SageMaker embedding provider."
            ) from exc

        self._client = boto3.client(
            "sagemaker-runtime",
            region_name=region_name,
            config=Config(read_timeout=timeout, connect_timeout=timeout),
        )
        self.endpoint_name = endpoint_name
        self.content_type = content_type
        self.accept = accept

    def embed(self, text: str) -> Sequence[float]:
        payload = json.dumps({"text": text})
        response = self._client.invoke_endpoint(
            EndpointName=self.endpoint_name,
            ContentType=self.content_type,
            Accept=self.accept,
            Body=payload.encode("utf-8"),
        )
        body = response["Body"].read()
        data = json.loads(body.decode("utf-8"))
        vector = data.get("embedding") or data.get("vector")
        if vector is None:
            raise RuntimeError(
                "SageMaker endpoint response lacks an 'embedding' field."
            )
        return vector


class HttpEmbeddingClient(_EmbeddingClientProtocol):
    """Embedding client that calls an HTTP endpoint returning JSON."""

    def __init__(
        self,
        endpoint_url: str,
        *,
        api_key: Optional[str] = None,
        auth_header: str = "Authorization",
        timeout: int = 30,
    ) -> None:
        self.endpoint_url = endpoint_url
        self.api_key = api_key
        self.auth_header = auth_header
        self.timeout = timeout

    def embed(self, text: str) -> Sequence[float]:
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers[self.auth_header] = self.api_key
        try:
            response = requests.post(
                self.endpoint_url,
                json={"text": text},
                headers=headers,
                timeout=self.timeout,
            )
            response.raise_for_status()
        # pragma: no cover - network failures
        except requests.RequestException as exc:
            raise RuntimeError("HTTP embedding request failed") from exc
        data = response.json()
        vector = data.get("embedding") or data.get("vector")
        if vector is None:
            raise RuntimeError(
                "HTTP embedding response lacks an 'embedding' field."
            )
        return vector


class StubEmbeddingClient(_EmbeddingClientProtocol):
    """Deterministic embedding generator for local development and tests."""

    def __init__(self, dims: int = 768) -> None:
        self.dims = max(1, dims)

    def embed(self, text: str) -> Sequence[float]:
        digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
        seed = int(digest[:16], 16)
        rng = random.Random(seed)
        return [rng.random() for _ in range(self.dims)]


def _coerce_vector(raw_vector: Sequence[float]) -> List[float]:
    try:
        return [float(value) for value in raw_vector]
    except (TypeError, ValueError) as exc:
        raise RuntimeError(
            "Embedding provider returned a non-numeric vector."
        ) from exc


def build_embedding_client_factory() -> Callable[[], _EmbeddingClientProtocol]:
    """Lazily creates embedding clients based on environment configuration."""

    def _factory() -> _EmbeddingClientProtocol:
        provider_value = get_ai_setting("AI_EMBEDDING_PROVIDER", "sagemaker")
        provider = str(provider_value or "sagemaker").strip().lower()
        if provider == "http":
            endpoint = get_ai_setting(
                "AI_HTTP_EMBEDDING_ENDPOINT"
            ) or get_ai_setting("AI_EMBEDDING_ENDPOINT")
            if not endpoint:
                raise RuntimeError(
                    "AI_HTTP_EMBEDDING_ENDPOINT required for HTTP embeddings."
                )
            api_key = get_ai_setting("AI_HTTP_EMBEDDING_API_KEY")
            auth_header = get_ai_setting(
                "AI_HTTP_EMBEDDING_AUTH_HEADER", "Authorization"
            )
            timeout = int(get_ai_setting("AI_HTTP_EMBEDDING_TIMEOUT", 30))
            return HttpEmbeddingClient(
                endpoint_url=endpoint,
                api_key=api_key,
                auth_header=auth_header,
                timeout=timeout,
            )
        if provider == "stub":
            dims = int(get_ai_setting("AI_STUB_EMBEDDING_DIM", 768))
            return StubEmbeddingClient(dims=dims)
        if provider == "sagemaker":
            endpoint_name = get_ai_setting("AI_SAGEMAKER_ENDPOINT_NAME")
            if not endpoint_name:
                raise RuntimeError(
                    "AI_SAGEMAKER_ENDPOINT_NAME required."
                )
            region = get_ai_setting("AI_SAGEMAKER_REGION") or get_ai_setting(
                "AWS_REGION"
            )
            content_type = get_ai_setting(
                "AI_SAGEMAKER_CONTENT_TYPE", "application/json"
            )
            accept = get_ai_setting("AI_SAGEMAKER_ACCEPT", "application/json")
            timeout = int(get_ai_setting("AI_SAGEMAKER_TIMEOUT", 30))
            return SageMakerEmbeddingClient(
                endpoint_name=endpoint_name,
                region_name=region,
                content_type=content_type,
                accept=accept,
                timeout=timeout,
            )
        raise RuntimeError(
            (
                "Unsupported AI_EMBEDDING_PROVIDER '%s'. "
                "Use 'sagemaker', 'http', or 'stub'."
            )
            % provider
        )

    return _factory


@lru_cache(maxsize=1)
def load_ai_search_restrictions() -> Dict[str, List[str]]:
    """Loads staging and production catalog restrictions if configured."""

    default = {"staging_ids": [], "prod_sources": []}
    path = get_ai_setting("AI_SEARCH_EXCLUSIONS_PATH")
    if not path:
        return default
    try:
        with open(path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except FileNotFoundError:
        logger.warning("AI search exclusions file '%s' not found.", path)
        return default
    except json.JSONDecodeError:
        logger.warning("AI search exclusions file '%s' is invalid JSON.", path)
        return default
    return {
        "staging_ids": payload.get("staging_ids", []),
        "prod_sources": payload.get("prod_catalogs")
        or payload.get("prod_sources", []),
    }


class AiSearchBuilder:
    """Constructs Elasticsearch DSL queries for the hybrid AI search flow."""

    def __init__(
        self,
        *,
        index: Optional[str] = None,
        vector_field: str,
        embedding_client_factory: Callable[[], _EmbeddingClientProtocol],
        staging_ids: Optional[Sequence[str]] = None,
        prod_sources: Optional[Sequence[str]] = None,
        resource_catalog_boost: float = 1000.0,
        rescore_window: int = 100,
        base_query_weight: float = 0.5,
        rescore_weight: float = 1.5,
        enable_rescore: bool = True,
    ) -> None:
        self.index = index
        self.vector_field = vector_field
        self._embedding_client_factory = embedding_client_factory
        self._embedding_client: Optional[_EmbeddingClientProtocol] = None
        self.staging_ids = list(staging_ids or [])
        self.prod_sources = list(prod_sources or [])
        self.resource_catalog_boost = max(1.0, resource_catalog_boost)
        self.rescore_window = max(0, rescore_window)
        self.base_query_weight = base_query_weight
        self.rescore_weight = rescore_weight
        self.enable_rescore = enable_rescore and self.rescore_window > 0

    def build_search(self, query: str, options) -> Search:
        text = (query or "").strip()
        if not text:
            raise ValueError("AI search requires a non-empty query string.")

        vector = _coerce_vector(self._get_embedding_client().embed(text))
        filter_query = self._build_filter_query()
        script_score_query = self._build_script_score_query(
            vector, filter_query
        )
        if self.index:
            search = Search(index=self.index)
        else:
            search = Search()
        search = search.query(script_score_query)

        if self.enable_rescore:
            search = search.extra(rescore=self._build_rescore(vector))

        return search

    def _get_embedding_client(self) -> _EmbeddingClientProtocol:
        if self._embedding_client is None:
            if not self._embedding_client_factory:
                raise RuntimeError(
                    "Embedding client factory is not configured."
                )
            self._embedding_client = self._embedding_client_factory()
        return self._embedding_client

    def _build_filter_query(self) -> Q:
        must_clauses: List[Q] = []
        must_not_clauses: List[Q] = []

        # Base filters only enforce static exclusions (staging IDs, prod
        # catalogs); dynamic filters continue to flow through apply_extras.
        if self.prod_sources:
            must_clauses.append(
                Q(
                    "terms",
                    **{"includedInDataCatalog.name": self.prod_sources},
                )
            )

        if self.staging_ids:
            must_not_clauses.append(Q("ids", values=self.staging_ids))

        if must_clauses or must_not_clauses:
            bool_kwargs: Dict[str, List[Q]] = {}
            if must_clauses:
                bool_kwargs["must"] = must_clauses
            if must_not_clauses:
                bool_kwargs["must_not"] = must_not_clauses
            return Q("bool", **bool_kwargs)
        return Q("match_all")

    def _build_script_score_query(
        self, vector: Sequence[float], filter_query: Q
    ) -> Q:
        script = (
            "double baseScore = 0.0;\n"
            "if (!doc['{field}'].isEmpty()) {{\n"
            "    baseScore = cosineSimilarity(params.query_vector, "
            "doc['{field}']) + 1.0;\n"
            "}}\n"
            "if (params.resource_boost > 1 && doc['@type'].size() > 0 "
            "&& doc['@type'].value.equals('ResourceCatalog')) {{\n"
            "    baseScore *= params.resource_boost;\n"
            "}}\n"
            "return baseScore;\n"
        ).format(field=self.vector_field)
        return Q(
            "script_score",
            query=filter_query,
            script={
                "source": script,
                "params": {
                    "query_vector": vector,
                    "resource_boost": self.resource_catalog_boost,
                },
            },
        )

    def _build_rescore(self, vector: Sequence[float]) -> Dict:
        rescore_script = (
            "if (doc['{field}'].isEmpty()) {\n"
            "    return 0.0;\n"
            "}\n"
            "return dotProduct(params.queryVector, doc['{field}']) + 1.0;\n"
        ).format(field=self.vector_field)
        return {
            "window_size": self.rescore_window,
            "query": {
                "rescore_query": {
                    "script_score": {
                        "query": {"match_all": {}},
                        "script": {
                            "source": rescore_script,
                            "params": {"queryVector": vector},
                        },
                    }
                },
                "query_weight": self.base_query_weight,
                "rescore_query_weight": self.rescore_weight,
            },
        }
