"""Reuse-potential ranking for text searches (issue #44).

Classic ranking is BM25 over the catch-all `all` field, whose length
normalization favors records with little metadata, such as single tables
from journal supplements. With ranking=reuse, the scores of the top hits are
multiplied by a bounded factor built from record-level signals, never the
repository:

* structured fields the repository provides (NDE-augmented values don't
  count, since they are derived from the same text),
* the sample or participant count, on a log scale where an unknown count
  stays neutral,
* a description that is missing or only repeats the name.

The build stores these per Dataset under `_meta.ranking`: thin_text (0 fine,
1 short, 2 missing or repeats the name), structured_fields (0-6),
sample_count, and reuse_factor, the multiplier with DEFAULT_PARAMS. Records
without it keep their score, so this falls back to classic order on an index
that wasn't built with it.

The factor rescores the top `window` hits per shard. Multiplying every hit
would stop Elasticsearch from skipping low-scoring documents, which costs
seconds on broad queries.
"""

RANKING_CLASSIC = "classic"
RANKING_REUSE = "reuse"
RANKINGS = (RANKING_CLASSIC, RANKING_REUSE)

DEFAULT_WINDOW = 5000

# The weights reuse_factor was built with. ranking_params can override them
# (except short_len, which thin_text already applied) where the config allows.
DEFAULT_PARAMS = {
    # Description missing or identical to the name.
    "p_thin": 0.8,
    # Name plus description shorter than 80 characters.
    "p_short": 0.9,
    # The text penalty is no worse than this with 2+ structured fields,
    # so records that carry their detail in fields (e.g. PDB) aren't hit.
    "p_floor_ctx": 0.95,
    # Per structured field the repository provides, up to ctx_cap fields.
    "w_ctx": 0.02,
    "ctx_cap": 4.0,
    # Sample count: x0.9 at 1, x1.0 at 10, x1.2 at 1,000 or more.
    "w_reuse": 0.2,
    "n_center_log": 1.0,
    "n_span_log": 2.0,
    # Bounds for the field and sample-count factor.
    "q_min": 0.7,
    "q_max": 1.45,
}

FACTOR_FIELD = "_meta.ranking.reuse_factor"

# Recomputes reuse_factor from the stored signals with the request's weights.
TUNED_SCRIPT = """
boolean has(def d, String f) { return d.containsKey(f) && d[f].size() > 0; }
if (!has(doc, '_meta.ranking.reuse_factor')) { return 1.0; }

long c = doc['_meta.ranking.structured_fields'].value;
long thin = doc['_meta.ranking.thin_text'].value;
double ctxF = 1.0 + params.w_ctx * Math.min(c, params.ctx_cap);
double reuseF = 1.0;
if (has(doc, '_meta.ranking.sample_count')) {
  double r = (Math.log10(Math.max(1.0, doc['_meta.ranking.sample_count'].value)) - params.n_center_log) / params.n_span_log;
  reuseF = 1.0 + params.w_reuse * Math.max(-0.5, Math.min(1.0, r));
}
double textF = thin == 2 ? params.p_thin : (thin == 1 ? params.p_short : 1.0);
if (c >= 2) { textF = Math.max(textF, params.p_floor_ctx); }
return Math.max(params.q_min, Math.min(params.q_max, ctxF * reuseF)) * textF;
"""


def settings(options, cfg):
    """Return (ranking, params, window, tuned) for a request.

    ranking_params ("w_ctx:0.03,window:2000") is honored only where the config
    sets RANKING_ALLOW_OVERRIDES; tuned says whether any weight changed.
    """
    ranking = options.get("ranking") or getattr(cfg, "RANKING_DEFAULT", RANKING_CLASSIC)
    if ranking not in RANKINGS:
        raise ValueError(f"Unknown ranking {ranking!r}; expected one of {', '.join(RANKINGS)}.")
    params = dict(DEFAULT_PARAMS)
    window = int(getattr(cfg, "RANKING_RESCORE_WINDOW", DEFAULT_WINDOW))
    tuned = False

    overrides = options.get("ranking_params")
    if overrides and getattr(cfg, "RANKING_ALLOW_OVERRIDES", False):
        for item in overrides.split(","):
            key, _, value = item.partition(":")
            key = key.strip()
            try:
                number = float(value)
            except ValueError:
                raise ValueError(f"Ranking parameter {key!r} needs a number, got {value!r}.") from None
            if key == "window":
                window = int(number)
            elif key in params:
                params[key] = number
                tuned = True
            else:
                raise ValueError(f"Unknown ranking parameter {key!r}.")
    return ranking, params, window, tuned


def can_rescore(options):
    """ES rejects rescore with a field sort; facet-only, scrolled (fetch_all)
    and AI-assisted requests skip it."""
    if options.get("use_ai_search") or options.get("fetch_all") or options.get("size") == 0:
        return False
    return all(key == "_score" for key in options.get("sort") or [])


def rescore(params, window, tuned=False):
    """Rescorer for the search body: the stored factor, or the factor
    recomputed with tuned weights."""
    if tuned:
        function = {"script_score": {"script": {"source": TUNED_SCRIPT, "params": params}}}
    else:
        function = {"field_value_factor": {"field": FACTOR_FIELD, "missing": 1.0}}
    return {
        "window_size": window,
        "query": {
            "score_mode": "multiply",
            "rescore_query": {"function_score": {"boost_mode": "replace", "functions": [function]}},
        },
    }
