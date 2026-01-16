import azure.functions as func  # type: ignore[import]
import json
import logging
from pathlib import Path

from bronze_to_silver import run_bronze_to_silver_and_gold


app = func.FunctionApp()


@app.route(route="HttpExample", auth_level=func.AuthLevel.FUNCTION)
def HttpExample(req: func.HttpRequest) -> func.HttpResponse:
    """Simple health-check endpoint."""
    logging.info("Python HTTP trigger function processed a request.")

    name = req.params.get("name")
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            req_body = None
        if isinstance(req_body, dict):
            name = req_body.get("name")

    if name:
        return func.HttpResponse(
            f"Hello, {name}. This HTTP triggered function executed successfully."
        )

    return func.HttpResponse(
        "This HTTP triggered function executed successfully. "
        "Pass a name in the query string or in the request body for a personalized response.",
        status_code=200,
    )


@app.route(
    route="process-bronze-to-silver",
    methods=["GET", "POST"],
    auth_level=func.AuthLevel.FUNCTION,
)
def process_bronze_to_silver(req: func.HttpRequest) -> func.HttpResponse:
    """
    HTTP-triggered function that:
    - Reads the three raw datasets from data/bronze
    - Cleans/transforms them
    - Writes cleaned datasets as Parquet files to data/silver
    - Builds gold layer (county-joined) in-memory from the cleaned frames
    - Writes one joined Parquet to data/gold
    """
    logging.info("Starting bronze -> silver -> gold data pipeline run.")

    try:
        base_dir = Path(__file__).parent
        result_summary = run_bronze_to_silver_and_gold(base_dir)
    except Exception as exc:  
        logging.exception("Bronze -> silver -> gold pipeline failed.")
        error_body = {
            "status": "error",
            "message": str(exc),
        }
        return func.HttpResponse(
            json.dumps(error_body),
            status_code=500,
            mimetype="application/json",
        )

    return func.HttpResponse(
        json.dumps({"status": "ok", "outputs": result_summary}),
        status_code=200,
        mimetype="application/json",
    )