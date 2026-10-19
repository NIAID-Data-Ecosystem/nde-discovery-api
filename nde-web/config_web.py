import copy

from authn.authn_provider import UserCookieAuthProvider
from biothings.web.settings.default import APP_LIST, QUERY_KWARGS
from handlers import (
    GoogleLoginHandler,
    GitHubLoginHandler,
    LogoutHandler,
    MicrosoftLoginHandler,
    NDESourceHandler,
    ORCIDLoginHandler,
    UserInfoHandler,
    WebAppHandler,
)
from user_data import (
    UserDataHandler,
    UserFavoriteDatasetsHandler,
    UserFavoriteSearchesHandler,
)
from xsrf import XSRFToken

ES_INDICES = {
    # 'zenodo': 'zenodo_current',
    # 'immport': 'immport_current'
    None: "nde_all_current",
    # 'zenodo': 'zenodo_20221020_6h4aac2v'
    # 'acd': 'acd_niaid_20221109_o6tbj5ct'
}

# Elasticsearch index for persistent user profiles and preferences
ES_USER_INDEX = "nde_user_profiles"
APP_LIST += [
    (r"/{ver}/metadata/?", NDESourceHandler),
]

# OAuth and XSRF handlers
APP_LIST += [
    (r"/user_info", UserInfoHandler),
    (r"/logout", LogoutHandler),
    (r"/login/github", GitHubLoginHandler),
    (r"/login/orcid", ORCIDLoginHandler),
    (r"/login/google", GoogleLoginHandler),
    (r"/login/microsoft", MicrosoftLoginHandler),
    (r"/xsrf_token", XSRFToken),
]

# User data persistence handlers
APP_LIST += [
    (r"/user/data", UserDataHandler),
    (r"/user/data/favorites/searches", UserFavoriteSearchesHandler),
    (r"/user/data/favorites/datasets", UserFavoriteDatasetsHandler),
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
    # No "enum": biothings validates the default (None) against it too.
    # ranking.settings() rejects unknown values instead.
    "ranking": {"type": str, "default": None},
    "ranking_params": {"type": str, "default": None},
}

# Ranking of text searches (issue #44, see ranking.py): "classic" is BM25
# alone; "reuse" rescores the top hits per shard by reuse potential, the
# fields and sample count over RANKING_RESCORE_WINDOW and the text check
# over RANKING_TEXT_WINDOW. dev defaults to "reuse" and accepts
# ranking_params (e.g. "w_ctx:0.03,text_window:100") so weights can be tuned
# without a deploy.
RANKING_DEFAULT = "reuse"
RANKING_RESCORE_WINDOW = 5000
RANKING_TEXT_WINDOW = 50
RANKING_ALLOW_OVERRIDES = True

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
