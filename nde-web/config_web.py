from biothings.web.settings.default import APP_LIST

ES_INDICES = {
    # 'zenodo': 'zenodo_current',
    # 'immport': 'immport_current'
    None: 'nde_all_current'
    # 'niaid': 'niaid_20220418_sjwnon1o'
}
APP_LIST += [
    (r"/{ver}/metadata/?", "handlers.NDESourceHandler"),
]

# replace default landing page handler
assert APP_LIST[0][0] == '/'
APP_LIST[0] = ('/', 'handlers.WebAppHandler')


ES_DOC_TYPE: 'dataset'

ES_QUERY_BUILDER = "pipeline.NDEQueryBuilder"
ALLOW_NESTED_AGGS = True

try:
    from config_web_local import *
except ImportError:
    pass
