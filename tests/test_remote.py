from biothings.tests.web import BiothingsWebTest


class TestQueryGET(BiothingsWebTest):
    # host = "api.data.niaid.nih.gov"
    host = "api.nde-dev.biothings.io/"
    prefix = "v1"

    # 1 term search
    def test_10(self):
        self.query(q="covid")

    # multiple terms
    def test_11(self):
        self.query(q="west siberian virus")

    # exact vs general
    def test_12(self):
        exact = self.query(q='"covid"')
        general = self.query(q="covid")
        assert exact["total"] < general["total"]

    # 1 Dataset
    def test_13(self):
        self.query(q="_id:accessclinicaldata_ACTT")

    # Datasets from a specific source
    def test_14(self):
        self.query(q='includedInDataCatalog.name:"Harvard Dataverse"')

    # Hist Search 1 year
    def test_15(self):
        res = self.query(
            q="__all__",
            extra_filter='(date:["2020-01-01" TO "2020-12-31"])',
            size="0",
            hist="date",
            hits=False,
        )
        assert len(res["facets"]["hist_dates"]["terms"]) == 1

    # hist search multiple years
    def test_16(self):
        res = self.query(
            q="__all__",
            extra_filter='(date:["2020-01-01" TO "2022-12-31"])',
            size="0",
            hist="date",
            hits=False,
        )
        assert len(res["facets"]["hist_dates"]["terms"]) > 1

    # test extra_filter
    def test_17(self):
        res = self.query(
            q="__all__",
            extra_filter='(healthCondition.name:("asthma")) AND -_exists_:measurementTechnique.name',
            size="0",
            facet_size="0",
            hits=False,
        )
        assert res["total"] > 0

    # field exists
    def test_18(self):
        res = self.query(q="_exists_:includedInDataCatalog")
        assert "includedInDataCatalog" in res["hits"][0].keys()

    # field does not exist
    def test_19(self):
        res = self.query(q="-_exists_:name")
        assert "name" not in res["hits"][0].keys()

    # every document should have includedinDataCatalog
    def test_20(self):
        assert self.query(q="-_exists_:includedInDataCatalog", hits=False)

    # every document should have url
    def test_21(self):
        assert self.query(q="-_exists_:url", hits=False)

    # make sure every conditionsOfAccess only has these 4 values
    def test_22(self):
        res = self.query(
            facets="conditionsOfAccess,includedInDataCatalog.name",
            q="_exists_:conditionsOfAccess",
        )
        enum = ["Open", "Restricted", "Closed", "Embargoed"]
        for value in res["facets"]["conditionsOfAccess"]["terms"]:
            assert value["term"] in enum, (
                "%s is not a valid includedInDataCatalog entry. List of sources that have includedInDataCatalog: %s"
                % (
                    value["term"],
                    res["facets"]["includedInDataCatalog.name"]["terms"],
                )
            )

    # -----------------
    # Metadata Related
    # -----------------

    def test_201(self):
        res = self.request("metadata").json()
        source_fields = ["schema", "identifier", "name", "description", "url"]
        for source_name, value in res["src"].items():
            assert "sourceInfo" in value.keys(), (
                "%s is missing the sourceInfo field." % source_name
            )
            for field in source_fields:
                assert (
                    field in value["sourceInfo"].keys()
                ), "%s is missing the %s field" % (source_name, field)
