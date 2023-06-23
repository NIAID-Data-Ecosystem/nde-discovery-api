import copy
from biothings.web.settings.default import APP_LIST, ANNOTATION_KWARGS, QUERY_KWARGS

ES_INDICES = {
    # 'zenodo': 'zenodo_current',
    # 'immport': 'immport_current'
   None: 'nde_all_current'
    #'zenodo': 'zenodo_20221020_6h4aac2v'
    #'acd': 'acd_niaid_20221109_o6tbj5ct'
}
APP_LIST += [
    (r"/{ver}/metadata/?", "handlers.NDESourceHandler"),
]

# replace default landing page handler
assert APP_LIST[0][0] == '/'
APP_LIST[0] = ('/', 'handlers.WebAppHandler')


# *****************************************************************************
# Elasticsearch Query Pipeline and Customizations
# *****************************************************************************

SOURCE_TYPEDEF={
     'extra_filter': {
        'type': str, 'default': None
     },
     'hist': {
        'type': str, 'default': None
     },
     'hist_interval': {
        'type': str, 'default': 'year'
     },
     'suggester': {
        'type': str, 'default': None
     }
}

QUERY_KWARGS = copy.deepcopy(QUERY_KWARGS)
QUERY_KWARGS['GET'].update(SOURCE_TYPEDEF)

ES_DOC_TYPE: 'dataset'
ES_QUERY_BUILDER = "pipeline.NDEQueryBuilder"
ES_RESULT_TRANSFORM = "pipeline.NDEFormatter"
ALLOW_NESTED_AGGS = True
DEFAULT_CACHE_MAX_AGE = 3600

try:
    from config_web_local import *
except ImportError:
    pass
