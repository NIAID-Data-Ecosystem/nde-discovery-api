import logging
from biothings.web.handlers import MetadataSourceHandler
from tornado.web import RequestHandler


class WebAppHandler(RequestHandler):
    def get(self):
        if self.render('dist/index.html'):
            self.render('dist/index.html')
        else:
            logging.info("Unable to find dist folder from react app.")


class NDESourceHandler(MetadataSourceHandler):
    """
    GET /v1/metadata
    """

    def extras(self, _meta):

        # Example Object
        # <SOURCE_NAME> = https://api.data.niaid.nih.gov/v1/metadata
        # if <SOURCE_NAME> in _meta['src']:
        # _meta['src'][<SOURCE_NAME>]['sourceInfo'] = {
        # 'name': 'A proper source name',
        # 'description': 'A short description of what the source offers, usually found on the source's about page',
        # 'schema': 'A dict where the key is their metadata variable and the value is our transformation. Ex: {"summary":"description"},
        # 'url': 'The source's URL',
        # 'identifier':'includedInDataCatalog.name',
        # }

        if 'ncbi_pmc' in _meta['src']:
            _meta['src']['ncbi_pmc']['sourceInfo'] = {
                "name": "NCBI PMC",
                "description": "PubMed Central® (PMC) is a free full-text archive of biomedical and life sciences journal literature at the U.S. National Institutes of Health's National Library of Medicine (NIH/NLM). In keeping with NLM's legislative mandate to collect and preserve the biomedical literature, PMC is part of the NLM collection, which also includes NLM's extensive print and licensed electronic journal holdings and supports contemporary biomedical and health care research and practice as well as future scholarship. Available to the public online since 2000, PMC was developed and is maintained by the National Center for Biotechnology Information(NCBI) at NLM.",
                "schema": {"article-id": "identifer", "article-id": "url",  "volume": "citation.volume", 'journal-title': 'citation.journalName', 'pub-date': 'citation.datePublished', 'article-title': 'citation.name', 'article-id': 'citation.pmid', 'article-id': 'citation.identifier', 'article-id': 'citation.url', 'article-id': 'citation.doi', 'abstract': 'description', 'contrib': 'author'},
                "url": "https://www.ncbi.nlm.nih.gov/pmc/",
                "identifier": "NCBI PMC"
            }

        if 'veupathdb' in _meta['src']:
            _meta['src']['veupathdb']['sourceInfo'] = {
                "name": "VEuPathDB",
                "description": "The Eukaryotic Pathogen, Vector and Host Informatics Resource(VEuPathDB) is one of two Bioinformatics Resource Centers(BRCs) funded by the US National Institute of Allergy and Infectious Diseases(NIAID), with additional support from the Wellcome Trust(UK). VEuPathDB provides access to diverse genomic and other large scale datasets related to eukaryotic pathogens and invertebrate vectors of disease. Organisms supported by this resource include (but are not limited to) the NIAID list of emerging and re-emerging infectious diseases.",
                "schema": {"id": "identifer", "displayName": "name", "contact_name": "author", "summary": "description", "type": "measurementTechnique", "sdPublisher": "project_id", "short_attribution": "creditText", "release_policy": "conditionOfAccess", "version": "dateModified", "author": "affiliation", "GenomeHistory": "dateUpdated", "Version": "datePublished", "organism": "species", "HyperLinks": "distribution", "gene_count": "variableMeasured", "gene_type": "GeneTypeCounts"},
                "url": "https://veupathdb.org/veupathdb/app/",
                "identifier": "VEuPathDB"
            }

        if 'acd_niaid' in _meta['src']:
            _meta['src']['acd_niaid']['sourceInfo'] = {
                "name": "AccessClinicalData@NIAID",
                "description": "AccessClinicalData@NIAID is a NIAID cloud-based, secure data platform that enables sharing of and access to reports and data sets from NIAID COVID-19 and other sponsored clinical trials for the basic and clinical research community.",
                "schema":  {"title": "name", "cmc_unique_id": "identifier", "brief_summary": "description", "data_availability_date": "datePublished", "most_recent_update": "dateModified", "data_available": "additionalType", "creator": "funding.funder.name", "nct_number": "nctid, identifier", "condition": "healthCondition", "clinical_trial_website": "mainEntityOfPage", "publications": "citation", "data_available_for_request": "conditionsOfAccess"},
                "url": "https://accessclinicaldata.niaid.nih.gov/",
                "identifier": "AccessClinicalData@NIAID"
            }

        if 'sb_apps' in _meta['src']:
            _meta['src']['sb_apps']['sourceInfo'] = {
                "name": "Seven Bridges Public Apps Gallery",
                "description": "The Seven Bridges Public Apps Gallery offers a repository of publicly available apps suitable for many different types of data analysis. Apps include both tools (individual bioinformatics utilities) and workflows (chains or pipelines of connected tools). The publicly available apps are maintained by the Seven Bridges Platform bioinformatics team to represent the latest tool versions.",
                "schema":  {"class": "applicationCategory", "label": "name", "description": "description", "inputs": "input", "outputs": "output", "requirements": "softwareRequirements", "sbg:image_url": "thumbnailUrl", "sbg:toolkit": "applicationSuite", "sbg:license": "license", "sbg:links": "codeRepository", "sbg:categories": "applicationSubCategory", "sbg:toolAuthor": "author", "sbg:appVersion": "softwareVersion", "sbg:id": "url, identifier", "sbg:revisionNotes": "version", "sbg:modifiedOn": "dateModified", "sbg:createdOn": "dateCreated", "sbg:contributors": "contributor", "sbg:publisher": "sdPublisher", "sbg:workflowLanguage": "programmingLanguage"},
                "url": "https://workspace.niaiddata.org/public/apps",
                "identifier": "PublicApps@SevenBridges"
            }

        if 'zenodo' in _meta['src']:
            _meta['src']['zenodo']['sourceInfo'] = {
                "name": "Zenodo",
                "description": "The OpenAIRE project, in the vanguard of the open access and open data movements in Europe was commissioned by the EC to support their nascent Open Data policy by providing a catch-all repository for EC funded research. CERN, an OpenAIRE partner and pioneer in open source, open access and open data, provided this capability and Zenodo was launched in May 2013.",
                "schema": {
                    "book": "book", "bookChapter": "chapter", "annotationCollection": "collection", "collection": "collection", "software": "computationalTool", "dataManagementPlan": "creativeWork", "deliverable": "creativeWork", "interactiveResource": "creativeWork", "other": "creativeWork", "patent": "creativeWork", "proposal": "creativeWork", "section": "creativeWork", "dataset": "dataset", "drawing": "drawing", "diagram": "imageObject", "figure": "imageObject", "image": "imageObject", "plot": "imageObject", "lesson": "learningResource", "audiovisual": "mediaObject", "photo": "photograph", "poster": "poster", "presentation": "presentationDigitalDocument", "report": "report", "article": "scholarlyArticle", "conferencePaper": "scholarlyArticle", "journalArticle": "scholarlyArticle", "preprint": "scholarlyArticle", "publication": "scholarlyArticle", "thesis": "scholarlyArticle", "workingPaper": "scholarlyArticle", "softwareDocumentation": "techArticle", "technicalNote": "techArticle", "physicalObject": "thing", "video": "videoObject", "taxonomicTreatment": "scholarlyArticle", "projectDeliverable": "creativeWork", "outputManagementPlan": "creativeWork", "projectMilestone": "creativeWork"
                },
                "url": "https://zenodo.org/",
                "identifier": "Zenodo"
            }

        if 'dde' in _meta['src']:
            _meta['src']['dde']['sourceInfo'] = {
                "name": "Data Discovery Engine",
                "description": "The Data Discovery Engine is a streamlined process to create, distribute and harves findable metadata via interoperable Schema.org schemas. The biomedical and informatics communities have largely endorsed the spirit and basic components of the FAIR Data Principles. Biomedical data producers, including CTSA hubs, need actionable best-practice guidance on how to make their data discoverable and reusable, and bring the practical benefits of data sharing to researcher's own research projects, as well as the research community as a whole.",
                "schema": {"creator": "author", "_id": "identifier", "date_created": "dateCreated", "last_updated": "dateModifed", "@type": "@type", "measurementTechnique": "measurementTechnique", "infectiousAgent": "infectiousAgent", "infectiousDisease": "infectiousDisease", "species": "species"},
                "url": "https://discovery.biothings.io/",
                "identifier": "Data Discovery Engine"
            }

        if 'ncbi_geo' in _meta['src']:
            _meta['src']['ncbi_geo']['sourceInfo'] = {
                "name": "NCBI Gene Expression Omnibus",
                "description": "GEO is a public functional genomics data repository supporting MIAME-compliant data submissions. Array- and sequence-based data are accepted. Tools are provided to help users query and download experiments and curated gene expression profiles.",
                "schema": {"_id": "identifier", "contributor(s)": "author", "organization": "publisher", "title": "name", "organism": "species", "experiment type": "measurementTechnique", "summary": "description", "submission date": "datePublished", "last update date": "dateModified", "citation(s)": "citation"},
                "url": "https://www.ncbi.nlm.nih.gov/geo/",
                "identifier": "NCBI GEO"
            }

        if 'immport' in _meta['src']:
            _meta['src']['immport']['sourceInfo'] = {
                "name": "Immunology Database and Analysis Portal (ImmPort)",
                "description": "The ImmPort project provides advanced information technology support in the archiving and exchange of scientific data for the diverse community of life science researchers supported by NIAID/DAIT and serves as a long-term, sustainable archive of research and clinical data. The core component of ImmPort is an extensive data warehouse containing experimental data and metadata describing the purpose of the study and the methods of data generation. The functionality of ImmPort will be expanded continuously over the life of the BISC project to accommodate the needs of expanding research communities. The shared research and clinical data, as well as the analytical tools in ImmPort are available to any researcher after registration.",
                "schema": {
                    "_id": "identifer", "creator": "author", "citations": "citedBy", "identifers": "identifer", "species": "species", "measurementTechnique": "measurementTechnique", "distribution": "distribution", "includedInDataCatalog": "includedInDataCatalog", "date": "date"
                },
                "url": "https://www.immport.org/shared/home",
                "identifier": "ImmPort"
            }

        if 'omicsdi' in _meta['src']:
            _meta['src']['omicsdi']['sourceInfo'] = {
                "name": "Omics Discovery Index (OmicsDI)",
                "description": "The Omics Discovery Index (OmicsDI) provides a knowledge discovery framework across heterogeneous omics data (genomics, proteomics, transcriptomics and metabolomics).",
                "schema": {"_id": "identifer", "citation": "citation", "creator": "author", "description": "description", "distribution": "distribution", "keywords": "keywords", "name": "name", "sameAs": "sameAs", "variableMeasured": "variableMeasured"},
                "url": "https://www.omicsdi.org/",
                "identifier": "Omics Discovery Index (OmicsDI)"
            }

        if 'mendeley' in _meta['src']:
            _meta['src']['mendeley']['sourceInfo'] = {
                "name": "Mendeley Data",
                "description": "Mendeley Data, a product of Elsevier, is one of the newest entrants in the research data repository landscape; the platform was released in April 2016. Mendeley Data is a general-purpose repository, allowing researchers in any field to upload and publish research data. Mendeley Data also allows researchers to share unpublished data privately with research collaborators.",
                "schema": {"id": "identifer", "doi": "doi", "name": "name", "description": "description", "contributors": "contributors", "files": "distribution", "articles": "citation", "categories": "keywords", "publish_date": "datePublished", "related_links": "citation", "modified_on": "dateModified", "links": "url", "repository": "sdPublisher"},
                "url": "https://www.omicsdi.org/",
                "identifier": "Mendeley"
            }

        if 'reframedb' in _meta['src']:
            _meta['src']['reframedb']['sourceInfo'] = {
                "name": "reframeDB",
                "description": "The ReFRAME collection of 12,000 compounds is a best-in-class drug repurposing library containing nearly all small molecules that have reached clinical development or undergone significant preclinical profiling. The purpose of such a screening collection is to enable rapid testing of compounds with demonstrated safety profiles in new indications, such as neglected or rare diseases, where there is less commercial motivation for expensive research and development.",
                "schema": {"assay_id": "identifer", "assay_title": "name", "title_short": "alternateName", "authors": "author", "summary": "description", "purpose": "description", "protocol": "description", "readout": "description", "detection_method": "description", "detection_reagents": "description", "components": "description", "drug_conc": "description", "indication": "healthCondition", "assay_type": "measurementTechnique", "bibliography": "citation"},
                "url": "https://www.omicsdi.org/",
                "identifier": "ReframeDB"
            }

        return _meta
