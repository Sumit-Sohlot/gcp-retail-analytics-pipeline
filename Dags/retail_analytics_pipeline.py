from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.pubsub import PubSubPublishMessageOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

PROJECT_ID = "project-ba19f34b-fedd-4326-98f"
REGION = "us-central1"
CLUSTER_NAME = "cluster-19e7"
LANDING_BUCKET = "retail-landing-bucket"
CURATED_BUCKET = "retail-curated-bucket"
ARCHIVE_BUCKET = "retail-archive-bucket"
BQ_DATASET = "retail_dataset"
TOPIC_NAME = "retail-pipeline-topic"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='retail_sales_analytics_pipeline',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=['retail', 'gcp', 'dataproc', 'bigquery']
) as dag:

    # Sensors
    wait_for_sales = GCSObjectExistenceSensor(
        task_id='wait_for_sales_file',
        bucket=LANDING_BUCKET,
        object='sales/sales.csv',
        timeout=300,
        poke_interval=15,
        mode='poke'
    )

    wait_for_customers = GCSObjectExistenceSensor(
        task_id='wait_for_customers_file',
        bucket=LANDING_BUCKET,
        object='customers/customers.csv',
        timeout=300,
        poke_interval=15,
        mode='poke'
    )

    wait_for_inventory = GCSObjectExistenceSensor(
        task_id='wait_for_inventory_file',
        bucket=LANDING_BUCKET,
        object='inventory/inventory.csv',
        timeout=300,
        poke_interval=15,
        mode='poke'
    )

    # Validation Task
    def validate_files(**context):
        validation_status = 'success'
        context['ti'].xcom_push(
            key='validation_status',
            value=validation_status
        )
        print('Validation Successful')

    validation_task = PythonOperator(
        task_id='validate_files',
        python_callable=validate_files
    )

    # Branching
    def branch_logic(**context):
        validation_status = context['ti'].xcom_pull(
            task_ids='validate_files',
            key='validation_status'
        )
        if validation_status == 'success':
            return 'run_dataproc_etl'
        else:
            return 'pipeline_failed'

    branching_task = BranchPythonOperator(
        task_id='branch_validation',
        python_callable=branch_logic
    )

    # Failure Task
    def failure_task():
        print('Pipeline Validation Failed')

    pipeline_failed = PythonOperator(
        task_id='pipeline_failed',
        python_callable=failure_task
    )

    # Dataproc Job
    PYSPARK_JOB = {
        'reference': {
            'project_id': PROJECT_ID
        },
        'placement': {
            'cluster_name': CLUSTER_NAME
        },
        'pyspark_job': {
            'main_python_file_uri': f'gs://{LANDING_BUCKET}/scripts/retail_etl.py'
        }
    }

    run_dataproc_etl = DataprocSubmitJobOperator(
        task_id='run_dataproc_etl',
        job=PYSPARK_JOB,
        region=REGION,
        project_id=PROJECT_ID
    )

    # Load BigQuery
    load_fact_sales = GCSToBigQueryOperator(
        task_id='load_fact_sales',
        bucket=CURATED_BUCKET,
        source_objects=['fact_sales/*.parquet'],
        destination_project_dataset_table=f'{PROJECT_ID}.{BQ_DATASET}.fact_sales',
        source_format='PARQUET',
        write_disposition='WRITE_APPEND',
        autodetect=True
    )

    # Aggregation Query
    aggregation_query = BigQueryInsertJobOperator(
        task_id='run_aggregation_query',
        configuration={
            'query': {
                'query': f'''
                    CREATE OR REPLACE TABLE `{PROJECT_ID}.{BQ_DATASET}.daily_sales_summary`
                    AS
                    SELECT
                        store_id,
                        category,
                        SUM(sale_amount) AS total_sales,
                        COUNT(*) AS transaction_count,
                        AVG(sale_amount) AS avg_sales
                    FROM `{PROJECT_ID}.{BQ_DATASET}.fact_sales`
                    GROUP BY store_id, category
                ''',
                'useLegacySql': False
            }
        }
    )

    # PubSub Notification
    publish_message = PubSubPublishMessageOperator(
        task_id='publish_pipeline_status',
        project_id=PROJECT_ID,
        topic=TOPIC_NAME,
        messages=[{
            'data': b'Retail Pipeline Completed Successfully'
        }]
    )

    # Archive Files
    archive_sales_file = GCSToGCSOperator(
        task_id='archive_sales_file',
        source_bucket=LANDING_BUCKET,
        source_object='sales/sales.csv',
        destination_bucket=ARCHIVE_BUCKET,
        destination_object='archive/sales.csv',
        move_object=True
    )

    # DAG Flow
    [wait_for_sales, wait_for_customers, wait_for_inventory] >> validation_task
    validation_task >> branching_task
    branching_task >> run_dataproc_etl
    branching_task >> pipeline_failed
    run_dataproc_etl >> load_fact_sales
    load_fact_sales >> aggregation_query
    aggregation_query >> publish_message
    publish_message >> archive_sales_file
