from etl_manager.etl import GlueJob

job = GlueJob('gluejob/', bucket = 'alpha-mojap-curated-open-data', job_role = 'airflow_osrm_scraper', job_arguments={'--test_arg': 'this is a test', '--enable-metrics': ''})

job.allocated_capacity = 4


try:
    job.run_job()
    job.wait_for_completion()
finally:
    job.cleanup()

