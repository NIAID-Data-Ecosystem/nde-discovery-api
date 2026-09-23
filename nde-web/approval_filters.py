"""Production search filter for curated DDE ResourceCatalog records."""


def dde_resource_catalog_approval_filter(approved_ids):
    """Exclude DDE catalog records unless their document ID is approved."""
    if not isinstance(approved_ids, list) or not all(
        isinstance(record_id, str) and record_id for record_id in approved_ids
    ):
        raise ValueError("exclusions.json requires prod_resource_catalog_ids")
    return {
        "bool": {
            "must_not": [
                {
                    "bool": {
                        "must": [
                            {"term": {"@type": "ResourceCatalog"}},
                            {
                                "terms": {
                                    "includedInDataCatalog.name": [
                                        "Data Discovery Engine",
                                        "Data Discovery Engine (DDE)",
                                        "Data Discovery Engine, NDE Systems Biology",
                                        "Data Discovery Engine, NIAID Data Ecosystem",
                                    ]
                                }
                            },
                        ],
                        "must_not": [{"ids": {"values": approved_ids}}],
                    }
                }
            ]
        }
    }
