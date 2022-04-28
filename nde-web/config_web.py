ES_INDICES = {
    # 'zenodo': 'zenodo_current',
    # 'immport': 'immport_current'
    None: 'nde_all_current'
    # 'niaid': 'niaid_20220418_sjwnon1o'
}

ES_DOC_TYPE: 'dataset'

ES_QUERY_BUILDER = "pipeline.NDEQueryBuilder"

try:
    from config_web_local import *
except ImportError:
    pass