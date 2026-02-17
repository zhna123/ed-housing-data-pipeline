import azure.functions as func  # type: ignore[import]
import json
import logging
from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parent
SRC_DIR = ROOT_DIR / "src"
# Ensure the `src/` directory is on the import path so `import pipeline...` works
# when the function is executed from the repo root (e.g., Azure Functions zip deploy).
if str(SRC_DIR) not in sys.path:
    sys.path.append(str(SRC_DIR))

from pipeline.landing_to_bronze import run_landing_to_bronze_all
from pipeline.run_medallion import run_bronze_to_silver_and_gold


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
    route="landing-to-bronze",
    methods=["GET", "POST"],
    auth_level=func.AuthLevel.FUNCTION,
)
def landing_to_bronze(req: func.HttpRequest) -> func.HttpResponse:
    """
    HTTP-triggered function that:
    - Moves new files from landing drop to ingest_date partitions
    - Writes bronze parquet datasets and reports
    """
    logging.info("Starting landing -> bronze data pipeline run.")

    run_id = req.params.get("run_id")
    if not run_id:
        try:
            body = req.get_json()
        except ValueError:
            body = None
        if isinstance(body, dict):
            run_id = body.get("run_id")
    if run_id:
        import os

        os.environ["PIPELINE_RUN_ID"] = str(run_id)

    try:
        base_dir = Path(__file__).parent
        result_summary = run_landing_to_bronze_all(base_dir)
    except Exception as exc:
        logging.exception("Landing -> bronze pipeline failed.")
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
        json.dumps({"status": "ok", "landing_to_bronze": result_summary}),
        status_code=200,
        mimetype="application/json",
    )


@app.route(
    route="bronze-to-silver-gold",
    methods=["GET", "POST"],
    auth_level=func.AuthLevel.FUNCTION,
)
def bronze_to_silver_gold(req: func.HttpRequest) -> func.HttpResponse:
    """
    HTTP-triggered function that:
    - Reads bronze datasets for an ingest_date
    - Writes silver datasets and reports
    - Builds gold dataset and report
    """
    logging.info("Starting bronze -> silver -> gold data pipeline run.")

    run_id = req.params.get("run_id")
    if not run_id:
        try:
            body = req.get_json()
        except ValueError:
            body = None
        if isinstance(body, dict):
            run_id = body.get("run_id")
    if run_id:
        import os

        os.environ["PIPELINE_RUN_ID"] = str(run_id)

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
        json.dumps({"status": "ok", "bronze_to_silver_gold": result_summary}),
        status_code=200,
        mimetype="application/json",
    )
