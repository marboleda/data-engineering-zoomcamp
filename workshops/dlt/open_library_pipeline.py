"""Pipeline to ingest data from the Open Library REST API."""

import dlt
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig


@dlt.source
def open_library_rest_api_source():
    """Define dlt resources from Open Library REST API endpoints.

    Uses the Search API (https://openlibrary.org/search.json) to query books.
    No authentication required for public read access.
    """
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://openlibrary.org/",
            # No auth - Open Library Search API is public
        },
        "resource_defaults": {
            "primary_key": "key",
            "write_disposition": "replace",
        },
        "resources": [
            {
                "name": "search",
                "endpoint": {
                    "path": "search.json",
                    "params": {
                        "q": "harry potter",
                        "limit": 100,
                    },
                    "data_selector": "docs",
                    "paginator": {
                        "type": "page_number",
                        "limit": 100,
                        "total_path": "num_found",
                        "page_param": "page",
                        "limit_param": "limit",
                    },
                },
            },
        ],
    }

    yield from rest_api_resources(config)


pipeline = dlt.pipeline(
    pipeline_name='open_library_pipeline',
    destination='duckdb',
    # `refresh="drop_sources"` ensures the data and the state is cleaned
    # on each `pipeline.run()`; remove the argument once you have a
    # working pipeline.
    refresh="drop_sources",
    # show basic progress of resources extracted, normalized files and load-jobs on stdout
    progress="log",
)


if __name__ == "__main__":
    load_info = pipeline.run(open_library_rest_api_source())
    print(load_info)  # noqa: T201
