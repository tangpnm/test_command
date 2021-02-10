# Import packages
from airflow import DAG
# from airflow.utils.dates import days_ago
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator

from datetime import datetime, timedelta

# Define default arguments
default_args = {
    'owner': 'best_selling',
    # 'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(0),
    'email': ['panumas.chuatcha@mail.kmutt.ac.th'],
    'retries': 3,
    'retry_delay': timedelta(minutes=10),
}

# Define dag variables
project_id = 'panumas-pipeline'
staging_dataset = 'SELLING_DWH_STAGING'
dwh_dataset = 'BEST_SELLING_DWH'
gs_bucket = 'transaction_daily'

# Set Schedule: Run pipeline once a day.
# Use cron to define exact time. Eg. 8:15am would be "15 08 * * *"
schedule_interval = "00 00 * * *"

# Define dag
with DAG('best-selling-product-pipeline',
    schedule_interval = schedule_interval,
        #   concurrency=5,
        #   max_active_runs=1,
    default_args = default_args
        ) as dag:

# Load data from GCS to BQ
    load_selling_transactions = GoogleCloudStorageToBigQueryOperator(
        task_id='load_selling_transactions',
        bucket=gs_bucket,
        source_objects=[
            'transaction_daily/selling_product/sold_{{ yesterday_ds }}.csv'],
        destination_project_dataset_table=f'{project_id}:{staging_dataset}.selling_transactions',
        write_disposition='WRITE_TRUNCATE',
        source_format='csv',
        skip_leading_rows=1,
        schema_fields=[
            {'name': 'transactionID', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'transactionDate', 'type': 'DATE', 'mode': 'NULLABLE'},
            {'name': 'productSold', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'unitsSold', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
        ],
    )

    # Check loaded data not null
    check_selling_transactions = BigQueryCheckOperator(
        task_id='check_transaction_data',
        sql='''
        #standardSQL
        SELECT
            COUNT(*) AS rows_in_partition
        FROM `{0}.{1}.selling_transactions_demo`    
        WHERE transactionDate = {2}
        '''.format(project_id, staging_dataset, '{{ yesterday_ds }}'
                ),
        use_legacy_sql=False,
        #####conn
        dag=dag,
    )

    # transform data and load to new data warehouse
    create_best_product = BigQueryOperator(
        task_id='create_best_product',
        sql='''
        #standardSQL
        SELECT 
            productSold, 
            SUM(unitsSold) AS summation,
            transactionDate
        FROM `{0}.{1}.selling_transactions_demo` 
        WHERE transactionDate = {2}
        GROUP BY productSold
        ORDER BY summation DESC LIMIT 1
        '''.format(project_id, staging_dataset, '{{ yesterday_ds }}'
                ),
        destination_dataset_table='{0}.{1}.best_selling_transactions'.format(
                project_id, dwh_dataset
        ),
        write_disposition='WRITE_APPEND',
        allow_large_results=True,
        use_legacy_sql=False,
        ### connnnn
        dag=dag,
    )

    # check best selling product
    check_best_product = BigQueryCheckOperator(
        task_id='check_best_product',
        sql='''
        #standardSQL
        SELECT
            COUNT(*) AS rows_in_partition
        FROM `{0}.{1}.hackernews_github_agg`    
        WHERE transactionDate = "{2}"
        '''.format(project_id, dwh_dataset, '{{ yesterday_ds }}'
                ),
        use_legacy_sql=False,
        ### connnnn
        dag=dag,
    )

    # Setting up Dependencies
    load_selling_transactions >> check_selling_transactions >> create_best_product >> check_best_product