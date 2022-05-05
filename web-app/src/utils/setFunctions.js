export const setColor = (label) => {
    // We are using this util function to assign a color the label.
    let color = "bg-red-600";
    switch (label) {
        case "Security fixes":
            color = "bg-pink-600";
            break;
        case "Bug fixes":
            color = "bg-yellow-600";
            break;
        case "Changes":
            color = "bg-green-500";
            break;
        case "New Data Source":
            color = "bg-blue-600";
            break;
        case "Known issue":
            color = "bg-blue-600";
            break;
        default:
            color = "";
    }
    return color;
};
export const setName = (sourceName) => {
    let name = "";
    switch (sourceName) {
        case "sb_apps":
            name = "Seven Bridges Public Apps Gallery";
            break;
        case "dde":
            name = "Data Discovery Engine";
            break;
        case "zenodo":
            name = "Zenodo";
            break;
        case "ncbi_geo":
            name = "NCBI Gene Expression Omnibus";
            break;
        case "immport":
            name = "Immunology Database and Analysis Portal";
            break;
        case "omicsdi":
            name = "Omics Discovery Index (OmicsDI)";
            break;
        case "niaid":
            name = "AccessClinicalData@NIAID";
            break;

        default:
            name = "";
    }
    return name;
};
export const setDescription = (sourceName) => {
    let description = "";
    switch (sourceName) {
        case "sb_apps":
            description = "The Seven Bridges Public Apps Gallery offers a repository of publicly available apps suitable for many different types of data analysis. Apps include both tools (individual bioinformatics utilities) and workflows (chains or pipelines of connected tools). The publicly available apps are maintained by the Seven Bridges Platform bioinformatics team to represent the latest tool versions.";
            break;
        case "dde":
            description = "The biomedical and informatics communities have largely endorsed the spirit and basic components of the FAIR Data Principles. Biomedical data producers, including CTSA hubs, need actionable best-practice guidance on how to make their data discoverable and reusable, and bring the practical benefits of data sharing to researcher's own research projects, as well as the research community as a whole.";
            break;
        case "zenodo":
            description = "The OpenAIRE project, in the vanguard of the open access and open data movements in Europe was commissioned by the EC to support their nascent Open Data policy by providing a catch-all repository for EC funded research. CERN, an OpenAIRE partner and pioneer in open source, open access and open data, provided this capability and Zenodo was launched in May 2013.";
            break;
        case "ncbi_geo":
            description = "GEO is a public functional genomics data repository supporting MIAME-compliant data submissions. Array- and sequence-based data are accepted. Tools are provided to help users query and download experiments and curated gene expression profiles.";
            break;
        case "immport":
            description = "The ImmPort project provides advanced information technology support in the archiving and exchange of scientific data for the diverse community of life science researchers supported by NIAID/DAIT and serves as a long-term, sustainable archive of research and clinical data. The core component of ImmPort is an extensive data warehouse containing experimental data and metadata describing the purpose of the study and the methods of data generation. The functionality of ImmPort will be expanded continuously over the life of the BISC project to accommodate the needs of expanding research communities. The shared research and clinical data, as well as the analytical tools in ImmPort are available to any researcher after registration.";
            break;
        case "omicsdi":
            description = "The Omics Discovery Index (OmicsDI) provides a knowledge discovery framework across heterogeneous omics data (genomics, proteomics, transcriptomics and metabolomics).";
            break;
        case "niaid":
            description = "AccessClinicalData@NIAID is a NIAID cloud-based, secure data platform that enables sharing of and access to reports and data sets from NIAID COVID-19 and other sponsored clinical trials for the basic and clinical research community.";
            break;

        default:
            description = "";
    };
    return description;
};

export const setDateCreated = async (sourcePath) => {
    const url = `https://api.github.com/repos/NIAID-Data-Ecosystem/nde-crawlers/commits?path=${sourcePath}`
    const response = await fetch(url)
    const jsonData = await response.json()
    const dates = []
    jsonData.forEach(jsonObj => {
        dates.push(jsonObj.commit.author.date)
    });
    return dates[dates.length - 1]
};


export const setSchema = (sourceName) => {
    let schema = {};
    switch (sourceName) {
        case "sb_apps":
            schema = { "class": "applicationCategory", "label": "name", "description": "description", "inputs": "input", "outputs": "output", "requirements": "softwareRequirements", "sbg:image_url": "thumbnailUrl", "sbg:toolkit": "applicationSuite", "sbg:license": "license", "sbg:links": "codeRepository", "sbg:categories": "applicationSubCategory", "sbg:toolAuthor": "author", "sbg:appVersion": "softwareVersion", "sbg:id": "url, identifier", "sbg:revisionNotes": "version", "sbg:modifiedOn": "dateModified", "sbg:createdOn": "dateCreated", "sbg:contributors": "contributor", "sbg:publisher": "sdPublisher", "sbg:workflowLanguage": "programmingLanguage" };
            break;
        case "dde":
            schema = { "creator": "author", "_id": "identifier", "date_created": "dateCreated", "last_updated": "dateModifed", "@type": "@type", "measurementTechnique": "measurementTechnique", "infectiousAgent": "infectiousAgent", "infectiousDisease": "infectiousDisease", "species": "species" };
            break;
        case "zenodo":
            schema = {
                "book": "book", "bookChapter": "chapter", "annotationCollection": "collection", "collection": "collection", "software": "computationalTool", "dataManagementPlan": "creativeWork", "deliverable": "creativeWork", "interactiveResource": "creativeWork", "other": "creativeWork", "patent": "creativeWork", "proposal": "creativeWork", "section": "creativeWork", "dataset": "dataset", "drawing": "drawing", "diagram": "imageObject", "figure": "imageObject", "image": "imageObject", "plot": "imageObject", "lesson": "learningResource", "audiovisual": "mediaObject", "photo": "photograph", "poster": "poster", "presentation": "presentationDigitalDocument", "report": "report", "article": "scholarlyArticle", "conferencePaper": "scholarlyArticle", "journalArticle": "scholarlyArticle", "preprint": "scholarlyArticle", "publication": "scholarlyArticle", "thesis": "scholarlyArticle", "workingPaper": "scholarlyArticle", "softwareDocumentation": "techArticle", "technicalNote": "techArticle", "physicalObject": "thing", "video": "videoObject", "taxonomicTreatment": "scholarlyArticle", "projectDeliverable": "creativeWork", "outputManagementPlan": "creativeWork", "projectMilestone": "creativeWork"
            };
            break;
        case "ncbi_geo":
            schema = { "_id": "identifier", "contributor(s)": "author", "organization": "publisher", "title": "name", "organism": "species", "experiment type": "measurementTechnique", "summary": "description", "submission date": "datePublished", "last update date": "dateModified", "citation(s)": "citation" };
            break;
        case "immport":
            schema = {
                "_id": "identifer", "creator": "author", "citations": "citedBy", "identifers": "identifer", "species": "species", "measurementTechnique": "measurementTechnique", "distribution": "distribution", "includedInDataCatalog": "includedInDataCatalog", "date": "date"
            };
            break;
        case "omicsdi":
            schema = { "_id": "identifer", "citation": "citation", "creator": "author", "description": "description", "distribution": "distribution", "keywords": "keywords", "name": "name", "sameAs": "sameAs", "variableMeasured": "variableMeasured" };
            break;
        case "niaid":
            schema = { "title": "name", "cmc_unique_id": "identifier", "brief_summary": "description", "data_availability_date": "datePublished", "most_recent_update": "dateModified", "data_available": "additionalType", "creator": "funding.funder.name", "nct_number": "nctid, identifier", "condition": "healthCondition", "clinical_trial_website": "mainEntityOfPage", "publications": "citation", "data_available_for_request": "conditionsOfAccess" };
            break;

        default:
            schema = "";
    };
    return schema;
};
