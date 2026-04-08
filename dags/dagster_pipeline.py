from dagster import op, job
import subprocess
import os
import sys

# ----------------------
# Extraction step
# ----------------------
@op
def run_extract(context):
    context.log.info("Running extraction...")

    # Use subprocess.run for better control on Windows
    result = subprocess.run(
        [sys.executable, "convert_csv_to_json.py"],  # runs python script
        capture_output=True,
        text=True
    )

    context.log.info(result.stdout)
    if result.returncode != 0:
        context.log.error(result.stderr)
        raise Exception("Extraction failed!")

# ----------------------
# Silver transformation
# ----------------------
@op
def run_transform(context):
    context.log.info("Running silver transformation...")

    result = subprocess.run(
        [sys.executable, "silver.py"],
        capture_output=True,
        text=True
    )

    context.log.info(result.stdout)
    if result.returncode != 0:
        context.log.error(result.stderr)
        raise Exception("Silver transformation failed!")

# ----------------------
# DBT run
# ----------------------
@op
def run_dbt(context):
    context.log.info("Running dbt models...")

    # Path to your dbt project
    dbt_dir = os.path.join(os.getcwd(), "order_quality_project")

    result = subprocess.run(
        ["dbt", "run"],      # command
        capture_output=True,
        text=True,
        cwd=dbt_dir           # sets working directory
    )

    context.log.info(result.stdout)
    if result.returncode != 0:
        context.log.error(result.stderr)
        raise Exception("DBT run failed!")

# ----------------------
# Run tests
# ----------------------
@op
def run_tests(context):
    context.log.info("🧪 Running pytest tests...")

    result = subprocess.run(
        ["pytest", "test_etl_pipeline.py", "-v"],
        capture_output=True,
        text=True
    )

    context.log.info(result.stdout)
    if result.returncode != 0:
        context.log.error(result.stderr)
        raise Exception("Pytest failed")
    context.log.info("All tests passed successfully")

# ----------------------
# Dagster Job
# ----------------------
@job
def etl_pipeline():
    run_extract()
    run_transform()
    run_dbt()
    run_tests()