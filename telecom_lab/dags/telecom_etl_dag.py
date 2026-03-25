from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import duckdb
import pandas as pd


# Task 1
def transform_data():

    con = duckdb.connect()

    query = """

    SELECT 
        c.user_id,
        u.region,
        r.call_type_name,
        c.duration_mins,
        r.rate_per_min,

        c.duration_mins * r.rate_per_min AS total_cost

    FROM 'include/call_logs.csv' c

    JOIN 'include/users.csv' u
    ON c.user_id = u.user_id

    JOIN 'include/rates.csv' r
    ON c.call_type_id = r.call_type_id

    """

    df = con.execute(query).df()

    df.to_parquet("include/joined_data.parquet")



# Task 2
def filter_data():

    df = pd.read_parquet("include/joined_data.parquet")

    high_value = df[df["total_cost"] > 100]

    high_value.to_parquet("include/high_value_calls.parquet")



# Task 3
def insights():

    df = pd.read_parquet("include/high_value_calls.parquet")

    top_regions = (

        df
        .groupby("region")["total_cost"]
        .sum()
        .sort_values(ascending=False)
        .head(3)

    )

    print("Top 3 Regions by Revenue:")

    print(top_regions)



with DAG(

    dag_id="telecom_etl_pipeline",

    start_date=datetime(2024,1,1),

    schedule =None,

    catchup=False

) as dag:



    t1 = PythonOperator(

        task_id="join_data",

        python_callable=transform_data

    )



    t2 = PythonOperator(

        task_id="filter_high_value",

        python_callable=filter_data

    )



    t3 = PythonOperator(

        task_id="show_insights",

        python_callable=insights

    )



    t1 >> t2 >> t3