import ast
from pathlib import Path


CONFIG_WEB = Path(__file__).resolve().parents[1] / "nde-web" / "config_web.py"


def _string_literals(node):
    for child in ast.walk(node):
        if isinstance(child, ast.Constant) and isinstance(child.value, str):
            yield child.value


def test_user_account_routes_are_registered():
    tree = ast.parse(CONFIG_WEB.read_text())
    strings = set(_string_literals(tree))

    assert "nde_user_profiles" in strings
    assert "/user_info" in strings
    assert "/login/github" in strings
    assert "/login/orcid" in strings
    assert "/xsrf_token" in strings
    assert "/user/data" in strings
    assert "/user/data/favorites/searches" in strings
    assert "/user/data/favorites/datasets" in strings
