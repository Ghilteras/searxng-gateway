# SPDX-License-Identifier: AGPL-3.0-or-later
"""Mojeek API engine for SearXNG.

Uses Mojeek's official Search API (https://api.mojeek.com/search) instead
of HTML scraping. Requires API key (paid plan GBP 2 CPM = GBP 0.30/month for
~150 queries/month).

The native SearXNG 'mojeek' engine does HTML scraping and gets blocked
(403 after 1-2 requests due to Mojeek's internal rate-limiter;
robots.txt Disallow:/search; ToS 3.5(e) prohibits scraping).

For homelab with low query volume, Mojeek API is the proper way to use
this engine. Sign up at https://www.mojeek.com/support/api/search/quickstart.html
"""

import os
from urllib.parse import urlencode

about = {
    "website": "https://www.mojeek.com/support/api/search/quickstart.html",
    "wikidata_id": None,
    "official_api_documentation": "https://www.mojeek.com/support/api/search/",
    "use_official_api": True,
    "require_api_key": True,
    "results": "JSON",
}

categories = ["general"]
paging = False

api_key = None


def init(engine_settings):
    global api_key  # noqa: PLW0603
    api_key = os.environ.get("MOJEEK_API_KEY", "")
    if not api_key:
        # Engine stays disabled if no key configured
        return False
    return True


def request(query, params):
    params["url"] = (
        "https://api.mojeek.com/search?"
        + urlencode({"q": query, "api_key": api_key, "fmt": "json", "t": 10})
    )
    return params


def response(resp):
    results = []
    data = resp.json()
    for entry in data.get("resp", {}).get("res", []):
        results.append(
            {
                "url": entry.get("u", ""),
                "title": entry.get("t", ""),
                "content": entry.get("d", ""),
            }
        )
    return results
