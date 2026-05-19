"""JSON configuration loader used across operational pipeline entrypoints.

The function in this module provides a single, consistent way to read environment
configuration files that drive preprocessing, table creation, and benchmark runs.
"""

import json


def load_json(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)
