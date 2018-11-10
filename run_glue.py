from etl_manager.etl import GlueJob

g = GlueJob('gluejob/', bucket = 'alpha-mojap-curated-open-data', job_role = 'airflow_osrm_scraper', job_arguments={'--test_arg': 'this is a test', '--enable-metrics': ''})

g.allocated_capacity = 4

g.run_job()