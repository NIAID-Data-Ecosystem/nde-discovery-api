import sys
from pathlib import Path


SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "nde-web" / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

import compute_heuristics as compute_module  # noqa: E402


class FakeCollection:
    def aggregate(self, pipeline):
        if pipeline == compute_module.agg_defined_term("about"):
            return [
                {
                    "_id": {
                        "identifier": "topic_3305",
                        "name": "Public health and epidemiology",
                    },
                    "count": 3,
                    "url": "http://edamontology.org/topic_3305",
                    "inDefinedTermSet": "EDAM",
                    "termCode": "topic_3305",
                }
            ]
        return []


def test_about_is_included_as_defined_term_heuristic():
    result = compute_module.compute_for_source(FakeCollection())

    assert result["about"] == [
        {
            "@type": "DefinedTerm",
            "identifier": "topic_3305",
            "name": "Public health and epidemiology",
            "url": "http://edamontology.org/topic_3305",
            "inDefinedTermSet": "EDAM",
            "termCode": "topic_3305",
        }
    ]
