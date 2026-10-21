"""Text-query pieces shared by search (pipeline) and saved-search counts."""


def term_value(q):
    """Value for the exact `_id` and `name` term boosts.

    Document ids are stored lowercase and the `name` analyzer lowercases its
    tokens, so a term with any uppercase letter never matches: "HIV" or
    "Asthma" lost the boost that "hiv" and "asthma" get.
    """
    return q.lower()


def prefix_query(q):
    """Prefix-expand each word for the wildcard recall clause.

    One-character words stay exact: "influenza A*" matches every term that
    starts with "a" and takes over a second, versus milliseconds without it.
    """
    return " ".join(word if len(word) == 1 else word + "*" for word in q.split())
