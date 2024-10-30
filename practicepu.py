
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryExecuteQueryOperator
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago

# Default arguments for the DAG
def_arg = {
    'owner': 'airflow',
    'retries': 1,
    'start_date': days_ago(1)
}

# Define the DAG
dag = DAG(
    "walmart_pipeline",
    default_args=def_arg,
    description="gcs to bigquery",
    schedule_interval="@daily",
    catchup=False
)

# TASK 1: CREATING DATASET
create_dataset = BigQueryCreateEmptyDatasetOperator(
    task_id="create_dataset",
    dataset_id="aadarsh_walmart",
    location='US',
    dag=dag  # Add DAG assignment
)

# TASK 2: CREATING MERCHANT, STAGE, AND TARGET TABLES
create_merchant_table = BigQueryCreateEmptyTableOperator(
    task_id='create_merchant_table',
    dataset_id='aadarsh_walmart',
    table_id='merchants_tb',
    schema_fields=[
        {"name": "merchant_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "merchant_name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "merchant_category", "type": "STRING", "mode": "NULLABLE"},
        {"name": "merchant_country", "type": "STRING", "mode": "NULLABLE"},
        {"name": "last_update", "type": "TIMESTAMP", "mode": "NULLABLE"},
    ],
    dag=dag  # Add DAG assignment
)

create_sales_stage_table = BigQueryCreateEmptyTableOperator(
    task_id="create_sales_stage_table",
    dataset_id="aadarsh_walmart",
    table_id="Sales_stage_tb",
    schema_fields=[
        {"name": "sale_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "sale_date", "type": "DATE", "mode": "NULLABLE"},
        {"name": "product_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "quantity_sold", "type": "INT64", "mode": "NULLABLE"},
        {"name": "total_sale_amount", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "merchant_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "last_update", "type": "TIMESTAMP", "mode": "NULLABLE"},
    ],
    dag=dag  # Add DAG assignment
)

create_target_table = BigQueryCreateEmptyTableOperator(
    task_id="create_target_table",
    dataset_id="aadarsh_walmart",
    table_id="Target_tb",
    schema_fields=[
        {"name": "sale_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "sale_date", "type": "DATE", "mode": "NULLABLE"},
        {"name": "product_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "quantity_sold", "type": "INT64", "mode": "NULLABLE"},
        {"name": "total_sale_amount", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "merchant_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "merchant_name", "type": "STRING", "mode": "NULLABLE"},  

        {"name": "merchant_category", "type": "STRING", "mode": "NULLABLE"},
        {"name": "merchant_country", "type": "STRING", "mode": "NULLABLE"},
        {"name": "last_update", "type": "TIMESTAMP", "mode": "NULLABLE"},
    ],
    dag=dag  # Add DAG assignment
)

# TASK 3: DATA LOAD ON CREATED TABLE from GCS TO BIGQUERY
gcs_to_bigquery_merchant = GCSToBigQueryOperator(
    task_id="gcs_to_bigquery_merchant",
    bucket="sheshrao",
    source_objects=["isha/merchant_data/merchants_1.json"],
    destination_project_dataset_table="inlaid-marker-430214-u1.aadarsh_walmart.merchants_tb",
    source_format='NEWLINE_DELIMITED_JSON',
    write_disposition='WRITE_TRUNCATE',
    dag=dag  # Add DAG assignment
)

gcs_to_bigquery_sales_stage = GCSToBigQueryOperator(
    task_id="gcs_to_bigquery_sales_stage",
    bucket="sheshrao",
    source_objects=["isha/sales_data/walmart_sales_1.json"],
    destination_project_dataset_table="inlaid-marker-430214-u1.aadarsh_walmart.Sales_stage_tb",
    source_format='NEWLINE_DELIMITED_JSON',
    write_disposition='WRITE_TRUNCATE',
    dag=dag  # Add DAG assignment
)

# TASK 4: PERFORMING JOIN OPERATION ON MERCHANT AND STAGE TABLE
join_operation = BigQueryExecuteQueryOperator(
    task_id='join_operation',
    sql="""  
CREATE OR REPLACE TABLE `inlaid-marker-430214-u1.aadarsh_walmart.final_sales_staging` AS
SELECT 
   S.sale_id,
   S.sale_date,
   S.product_id,
   S.quantity_sold,
   S.total_sale_amount,
   M.merchant_id,
   M.merchant_name,
   M.merchant_category,
   M.merchant_country,
   CURRENT_TIMESTAMP() as last_update   
FROM  `inlaid-marker-430214-u1.aadarsh_walmart.Sales_stage_tb` S 
LEFT JOIN `inlaid-marker-430214-u1.aadarsh_walmart.merchants_tb` M
ON  S.merchant_id = M.merchant_id
""",
    use_legacy_sql=False,
    dag=dag  # Add DAG assignment
)

# TASK 5: PERFORMING UPDATE AND INSERT FROM FINAL SALES STAGING TABLE
merge_operation = BigQueryExecuteQueryOperator(
    task_id='merge_operation',
    sql="""  
MERGE INTO `inlaid-marker-430214-u1.aadarsh_walmart.Target_tb` AS target
USING `inlaid-marker-430214-u1.aadarsh_walmart.final_sales_staging` AS source
ON target.sale_id = source.sale_id              
WHEN MATCHED THEN
    UPDATE SET
        target.sale_date = source.sale_date,
        target.product_id = source.product_id,
        target.quantity_sold = source.quantity_sold,
        target.total_sale_amount = source.total_sale_amount,
        target.merchant_id = source.merchant_id,
        target.merchant_name = source.merchant_name,                                                         
        target.merchant_category = source.merchant_category,
        target.merchant_country = source.merchant_country,
        target.last_update = source.last_update
WHEN NOT MATCHED THEN
    INSERT (
        sale_id, sale_date, product_id, quantity_sold, total_sale_amount, 
        merchant_id, merchant_name, merchant_category, merchant_country, last_update
    )
    VALUES (
        source.sale_id, source.sale_date, source.product_id, source.quantity_sold, source.total_sale_amount, 
        source.merchant_id, source.merchant_name, source.merchant_category, source.merchant_country, source.last_update
    );
""",
    use_legacy_sql=False,
    dag=dag  # Add DAG assignment
)

# DEFINING TASK DEPENDENCIES
create_dataset >> create_merchant_table >> create_sales_stage_table >> create_target_table 
create_target_table >>gcs_to_bigquery_merchant >> gcs_to_bigquery_sales_stage >> join_operation >> merge_operation
