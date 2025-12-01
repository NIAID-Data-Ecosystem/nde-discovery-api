import copy

from authn.authn_provider import UserCookieAuthProvider
from biothings.web.settings.default import APP_LIST, QUERY_KWARGS
from handlers import (
    GitHubLoginHandler,
    LogoutHandler,
    NDESourceHandler,
    ORCIDLoginHandler,
    UserInfoHandler,
    WebAppHandler,
)
from xsrf import XSRFToken

ES_INDICES = {
    # 'zenodo': 'zenodo_current',
    # 'immport': 'immport_current'
    None: "nde_all_current",
    # 'zenodo': 'zenodo_20221020_6h4aac2v'
    # 'acd': 'acd_niaid_20221109_o6tbj5ct'
}
APP_LIST += [
    (r"/{ver}/metadata/?", NDESourceHandler),
]

# OAuth and XSRF handlers
APP_LIST += [
    (r"/user_info", UserInfoHandler),
    (r"/logout", LogoutHandler),
    (r"/login/github", GitHubLoginHandler),
    (r"/login/orcid", ORCIDLoginHandler),
    (r"/xsrf_token", XSRFToken),
]

# Authentication provider chain for BioThingsAuthnMixin consumers
AUTHN_PROVIDERS = (
    (UserCookieAuthProvider, {}),
)

# replace default landing page handler
assert APP_LIST[0][0] == "/"
APP_LIST[0] = ("/", WebAppHandler)


# *****************************************************************************
# Elasticsearch Query Pipeline and Customizations
# *****************************************************************************

SOURCE_TYPEDEF = {
    "extra_filter": {"type": str, "default": None},
    "hist": {"type": str, "default": None},
    "hist_interval": {"type": str, "default": "year"},
    "suggester": {"type": str, "default": None},
    "use_metadata_score": {"type": bool, "default": False},
    "show_meta": {"type": bool, "default": False},
    "multi_terms_fields": {"type": list, "default": []},
    "multi_terms_size": {"type": int, "default": 10},
    "lineage": {"type": int, "default": None},
    "use_ai_search": {"type": bool, "default": False},
}

QUERY_KWARGS = copy.deepcopy(QUERY_KWARGS)
QUERY_KWARGS["GET"].update(SOURCE_TYPEDEF)

ES_DOC_TYPE = "dataset"
ES_QUERY_BUILDER = "pipeline.NDEQueryBuilder"
ES_QUERY_BACKEND = "pipeline.NDEESQueryBackend"
ES_RESULT_TRANSFORM = "pipeline.NDEFormatter"
ALLOW_NESTED_AGGS = True
DEFAULT_CACHE_MAX_AGE = 3600

# Scroll_id customizations
ES_SCROLL_TIME = "3m"
# Size of each scroll request return
ES_SCROLL_SIZE = 500

try:
    from config_web_local import *  # noqa: F401,F403
except ImportError:
    pass
