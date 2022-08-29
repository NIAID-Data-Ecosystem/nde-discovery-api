import copy
from biothings.web.settings.default import APP_LIST, ANNOTATION_KWARGS, QUERY_KWARGS

ES_INDICES = {
    # 'zenodo': 'zenodo_current',
    # 'immport': 'immport_current'
    None: 'nde_all_current'
    # 'zenodo': 'zenodo_20220614_fs30ogo7'
    # 'acd': 'acd_niaid_20220718_ulffyfib'
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
     }
}

QUERY_KWARGS = copy.deepcopy(QUERY_KWARGS)
QUERY_KWARGS['*'].update(SOURCE_TYPEDEF)

ES_DOC_TYPE: 'dataset'
ES_QUERY_BUILDER = "pipeline.NDEQueryBuilder"
ALLOW_NESTED_AGGS = True

try:
    from config_web_local import *
except ImportError:
    pass
