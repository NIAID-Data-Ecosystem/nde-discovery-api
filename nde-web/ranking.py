"""Reuse-potential ranking for text searches (issue #44).

Classic ranking is BM25 over the catch-all `all` field, whose length
normalization favors records with little metadata, such as single tables
from journal supplements. With ranking=reuse, the top hits are rescored by a
bounded multiplier built from record-level signals, never the repository:

* a description that is missing or only repeats the name,
* structured fields the repository provides (NDE-augmented values don't
  count, since they are derived from the same text),
* the sample count, on a log scale where an unknown count stays neutral.

Records other than Datasets keep their score.

The multiplier runs in a rescore window because the description check needs
`_source`, and a script over every hit costs about a second per million
matches. Once the build precomputes these signals, this can become a
function_score over all hits.
"""

RANKING_CLASSIC = "classic"
RANKING_REUSE = "reuse"
RANKINGS = (RANKING_CLASSIC, RANKING_REUSE)

DEFAULT_WINDOW = 500

DEFAULT_PARAMS = {
    # Description missing or identical to the name.
    "p_thin": 0.8,
    # Name plus description shorter than short_len characters.
    "p_short": 0.9,
    "short_len": 80.0,
    # The text penalty is no worse than this with 2+ structured fields,
    # so records that carry their detail in fields (e.g. PDB) aren't hit.
    "p_floor_ctx": 0.95,
    # Per structured field the repository provides, up to ctx_cap fields.
    "w_ctx": 0.02,
    "ctx_cap": 4.0,
    # Sample count: x0.9 at 1 sample, x1.0 at 10, x1.2 at 1,000 or more.
    "w_reuse": 0.2,
    "n_center_log": 1.0,
    "n_span_log": 2.0,
    "q_min": 0.7,
    "q_max": 1.45,
}

REUSE_SCRIPT = """
boolean has(def d, String f) { return d.containsKey(f) && d[f].size() > 0; }
boolean provided(def d, String f, def aug) { return has(d, f + '.name') && !aug.contains(f); }

if (!has(doc, '@type') || doc['@type'].value != 'Dataset') { return 1.0; }

def aug = new HashSet();
if (has(doc, '_meta.recommended_augmented_fields')) { aug.addAll(doc['_meta.recommended_augmented_fields']); }
if (has(doc, '_meta.required_augmented_fields')) { aug.addAll(doc['_meta.required_augmented_fields']); }

int c = 0;
if (provided(doc, 'measurementTechnique', aug)) { c++; }
if (provided(doc, 'variableMeasured', aug)) { c++; }
if (provided(doc, 'species', aug) || provided(doc, 'infectiousAgent', aug) || provided(doc, 'healthCondition', aug)) { c++; }
if (has(doc, 'sample.@type')) { c++; }
if (has(doc, 'funding.@type') || has(doc, 'funding.funder.name')) { c++; }
if (has(doc, 'citation.@type') || has(doc, 'citation.doi') || has(doc, 'citation.identifier') || has(doc, 'isBasedOn.@type')) { c++; }
double ctxF = 1.0 + params.w_ctx * Math.min(c, params.ctx_cap);

double reuseF = 1.0;
if (has(doc, 'sample.numberOfItems.value')) {
  double n = 0;
  for (def v : doc['sample.numberOfItems.value']) { n = Math.max(n, v); }
  double r = (Math.log10(Math.max(1.0, n)) - params.n_center_log) / params.n_span_log;
  reuseF = 1.0 + params.w_reuse * Math.max(-0.5, Math.min(1.0, r));
}

def name = params._source['name'];
def desc = params._source['description'];
String nn = (name instanceof List ? String.join(' ', name) : (name == null ? '' : name.toString())).toLowerCase().trim();
String dn = (desc instanceof List ? String.join(' ', desc) : (desc == null ? '' : desc.toString())).toLowerCase().trim();
while (nn.endsWith('.')) { nn = nn.substring(0, nn.length() - 1).trim(); }
while (dn.endsWith('.')) { dn = dn.substring(0, dn.length() - 1).trim(); }
double textF = 1.0;
if (dn.length() == 0 || nn.equals(dn)) { textF = params.p_thin; }
else if (nn.length() + dn.length() < params.short_len) { textF = params.p_short; }
if (c >= 2) { textF = Math.max(textF, params.p_floor_ctx); }

return Math.max(params.q_min, Math.min(params.q_max, textF * ctxF * reuseF));
"""


def settings(options, cfg):
    """Return (ranking, params, window) for a request.

    ranking_params ("w_ctx:0.03,window:1000") is honored only where the
    config sets RANKING_ALLOW_OVERRIDES, so dev can tune live.
    """
    ranking = options.get("ranking") or getattr(cfg, "RANKING_DEFAULT", RANKING_CLASSIC)
    if ranking not in RANKINGS:
        raise ValueError(f"Unknown ranking {ranking!r}; expected one of {', '.join(RANKINGS)}.")
    params = {**DEFAULT_PARAMS, **getattr(cfg, "RANKING_REUSE_PARAMS", {})}
    window = int(getattr(cfg, "RANKING_RESCORE_WINDOW", DEFAULT_WINDOW))

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
            else:
                raise ValueError(f"Unknown ranking parameter {key!r}.")
    return ranking, params, window


def can_rescore(options):
    """ES rejects rescore with a field sort; facet-only, scrolled (fetch_all)
    and AI-assisted requests skip it."""
    if options.get("use_ai_search") or options.get("fetch_all") or options.get("size") == 0:
        return False
    return all(key == "_score" for key in options.get("sort") or [])


def rescore(params, window):
    return {
        "window_size": window,
        "query": {
            "score_mode": "multiply",
            "rescore_query": {
                "function_score": {
                    "boost_mode": "replace",
                    "functions": [{"script_score": {"script": {"source": REUSE_SCRIPT, "params": params}}}],
                }
            },
        },
    }
