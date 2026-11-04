import copy

from authn.authn_provider import UserCookieAuthProvider
from biothings.web.settings.default import APP_LIST, QUERY_KWARGS
from handlers import (
    GoogleLoginHandler,
    GitHubLoginHandler,
    LogoutHandler,
    MicrosoftLoginHandler,
    NDESourceHandler,
    OktaLoginHandler,
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

# /metadata?dev=true lists every installed Python package and its version, so
# it stays off on public hosts. A host's config.py can set this to True to turn
# it back on for debugging.
ALLOW_METADATA_DEV = False

# OAuth and XSRF handlers
APP_LIST += [
    (r"/user_info", UserInfoHandler),
    (r"/logout", LogoutHandler),
    (r"/login/github", GitHubLoginHandler),
    (r"/login/orcid", ORCIDLoginHandler),
    (r"/login/google", GoogleLoginHandler),
    (r"/login/microsoft", MicrosoftLoginHandler),
    (r"/login/okta", OktaLoginHandler),
    (r"/xsrf_token", XSRFToken),
]

# NIH Login (NIH Okta) client that NIH CIT registered for staging (RITM0706436).
# NIH asked us to use its "default" authorization server as the issuer.
# /login/okta returns 404 until a host's config.py sets OKTA_CLIENT_SECRET.
# Production will get its own client ID and issuer.
OKTA_CLIENT_ID = "0oa25awn4oQbxvg2m0j7"
OKTA_ISSUER = "https://loginstg.nih.gov/oauth2/default"
# The provider behind each staging NIH Login idp (the ID token's idp claim), as
# seen in test sign-ins; production's IDs will differ. Profiles' _identifier
# uses these names. A sign-in through an unlisted IdP (HHS, for now) gets none.
OKTA_IDP_PROVIDERS = {
    "0oa1l0tjhgKL3YyBI0j7": "google",
    "0oa1lhb6inAL3BrG80j7": "microsoft",
    "0oa1lqd89clFs6IwV0j7": "orcid",
}

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
