from dagster_pipeline import etl_pipeline

if __name__ == "__main__":
    # Execute the ETL pipeline in-process (no ReconstructableJob needed)
    result = etl_pipeline.execute_in_process()

    if result.success:
        print("🎉 Pipeline executed successfully!")
    else:
        print("❌ Pipeline failed. Check logs above.")