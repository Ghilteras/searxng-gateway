# SPDX-License-Identifier: AGPL-3.0-or-later
"""Serper.dev Google Search API — results via ``google.serper.dev/search``.

See https://serper.dev for API key.
"""

import os

about = {
    "website": "https://serper.dev",
    "wikidata_id": None,
    "official_api_documentation": "https://serper.dev/playground",
    "use_official_api": True,
    "require_api_key": True,
    "results": "JSON",
}

categories = ["general", "web"]
paging = False

api_url = "https://google.serper.dev/search"
api_key = None


def request(query, params):
    params["url"] = api_url
    params["method"] = "POST"
    params["headers"]["X-API-KEY"] = api_key
    params["json"] = {"q": query, "num": 10}
    return params


def response(resp):
    results = []
    json_results = resp.json()

    for item in json_results.get("organic", []):
        result = {
            "url": item.get("link"),
            "title": item.get("title"),
            "content": item.get("snippet"),
        }
        results.append(result)

    return results


def init(engine_settings):
    global api_key  # noqa: PLW0603
    # Read from env var directly; settings.yml has literal ${SERPER_API_KEY}
    # because SearXNG's YAML loader does not do env var substitution.
    api_key = os.environ.get("SERPER_API_KEY") or engine_settings.get("api_key")
    if not api_key:
        return False
    return True
