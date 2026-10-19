"""Text search ranking: case-insensitive term boosts, exact one-letter words
in the wildcard clause, and the reuse-potential rescore (issue #44).

These build real queries with NDEQueryBuilder and inspect the request body.
"""

import sys
import types
from pathlib import Path

import pytest

WEB_DIR = Path(__file__).resolve().parents[1] / "nde-web"
sys.path.insert(0, str(WEB_DIR))

import config_web  # noqa: E402
import pipeline  # noqa: E402
import ranking  # noqa: E402
from biothings.web.options.manager import OptionSet, ReqArgs  # noqa: E402
from search_text import prefix_query  # noqa: E402


@pytest.fixture(autouse=True)
def runtime_config(monkeypatch):
    # The deployed config.py is not in the repo; stub dev's ranking settings.
    config = types.ModuleType("config")
    config.AI_SEARCH_VECTOR_FIELD = "ibmGraniteEmbedding"
    config.RANKING_DEFAULT = "reuse"
    config.RANKING_RESCORE_WINDOW = 5000
    config.RANKING_TEXT_WINDOW = 50
    config.RANKING_ALLOW_OVERRIDES = True
    monkeypatch.setitem(sys.modules, "config", config)
    return config


def build(q, **options):
    return pipeline.NDEQueryBuilder().build(q, **{"size": 10, **options}).to_dict()


def text_clauses(body):
    """The dis_max clauses of the user's text query."""
    stack = [body["query"]]
    while stack:
        node = stack.pop()
        if isinstance(node, dict):
            if "dis_max" in node:
                return node["dis_max"]["queries"]
            stack.extend(node.values())
        elif isinstance(node, list):
            stack.extend(node)
    raise AssertionError("no dis_max clause")


def script(rescorer):
    return rescorer["query"]["rescore_query"]["function_score"]["functions"][0]["script_score"]["script"]


def test_term_boosts_match_any_case():
    clauses = text_clauses(build("HIV"))

    assert {"term": {"_id": {"value": "hiv", "boost": 5}}} in clauses
    assert {"term": {"name": {"value": "hiv", "boost": 5}}} in clauses


def test_quoted_term_boosts_match_any_case():
    clauses = text_clauses(build('"GSE244573"'))

    assert {"term": {"_id": {"value": "gse244573", "boost": 5}}} in clauses


def test_one_letter_words_are_not_prefix_expanded():
    assert prefix_query("covid data") == "covid* data*"
    assert prefix_query("influenza A") == "influenza* A"
    assert prefix_query("type 1 diabetes") == "type* 1 diabetes*"


def test_text_search_is_rescored_by_reuse_potential():
    fields, text = build("asthma")["rescore"]

    # Doc-values signals over a wide window, the _source text check over the top.
    assert (fields["window_size"], text["window_size"]) == (5000, 50)
    assert script(fields)["source"] == ranking.FIELDS_SCRIPT
    assert script(text)["source"] == ranking.TEXT_SCRIPT
    assert "params._source" not in ranking.FIELDS_SCRIPT
    for rescorer in (fields, text):
        assert script(rescorer)["params"] == ranking.DEFAULT_PARAMS
        assert rescorer["query"]["score_mode"] == "multiply"


def test_relevance_sort_is_rescored():
    assert "rescore" in build("asthma", sort=["_score"])


@pytest.mark.parametrize(
    "q, options",
    [
        ("asthma", {"ranking": "classic"}),
        ("__all__", {}),
        ("asthma", {"sort": ["date"]}),
        ("asthma", {"size": 0}),
        ("asthma", {"fetch_all": True}),
    ],
)
def test_requests_that_keep_classic_order(q, options):
    assert "rescore" not in build(q, **options)


def test_ai_search_is_not_rescored():
    query = pipeline.NDEQueryBuilder().build("asthma", size=10, use_ai_search=True)

    assert "rescore" not in query.to_dict()


def test_classic_is_the_default_without_ranking_config(runtime_config):
    del runtime_config.RANKING_DEFAULT

    assert "rescore" not in build("asthma")


def test_ranking_params_tune_the_rescore():
    fields, text = build("asthma", ranking_params="w_ctx:0.05,window:1000,text_window:100")["rescore"]

    assert (fields["window_size"], text["window_size"]) == (1000, 100)
    assert script(fields)["params"]["w_ctx"] == 0.05


def test_ranking_params_need_overrides_enabled(runtime_config):
    runtime_config.RANKING_ALLOW_OVERRIDES = False

    fields, text = build("asthma", ranking_params="w_ctx:0.05,text_window:100")["rescore"]

    assert script(fields)["params"]["w_ctx"] == ranking.DEFAULT_PARAMS["w_ctx"]
    assert text["window_size"] == 50


@pytest.mark.parametrize("ranking_params", ["bogus:1", "w_ctx:high"])
def test_bad_ranking_params_are_rejected(ranking_params):
    with pytest.raises(ValueError):
        build("asthma", ranking_params=ranking_params)


def test_unknown_ranking_is_rejected():
    with pytest.raises(ValueError):
        build("asthma", ranking="bm25")


def parse_query_args(**args):
    """Parse request args the way the /query handler does."""
    options = OptionSet({"GET": config_web.QUERY_KWARGS["GET"]})
    return options.parse("GET", ReqArgs(query=args))


def test_requests_without_ranking_args_are_accepted():
    args = parse_query_args(q="asthma")

    assert args.ranking is None
    assert args.ranking_params is None


def test_ranking_args_reach_the_builder():
    args = parse_query_args(q="asthma", ranking="classic", ranking_params="w_ctx:0.05")

    assert args.ranking == "classic"
    assert args.ranking_params == "w_ctx:0.05"
