"""REST API service layer for the el Extract-Load pipeline.

Depends on the `el` package; the core CLI does not depend on this package, so
web dependencies (fastapi/uvicorn/httpx) are only needed to run the API server.

Run: uvicorn api.app:app --host $API_HOST --port $API_PORT --workers 1
"""
