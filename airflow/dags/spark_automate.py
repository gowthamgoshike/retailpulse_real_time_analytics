from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.sensors.time_delta import TimeDeltaSensor

# ------------------------
# Paths
# ------------------------
VENV_PYTHON = "/Users/gowthamgoshike/projects/retailvenv/bin/python"
VENV_BIN = "/Users/gowthamgoshike/projects/retailvenv/bin/activate"
PROJECT_PATH = "/Users/gowthamgoshike/projects/retailpulse_real_time_analytics"

# Producers
ORDERS_PRODUCER = f"{PROJECT_PATH}/producers/orders_producer.py"
USER_ACTIVITY_PRODUCER = f"{PROJECT_PATH}/producers/user_activity_producer.py"
TRANSACTIONS_PRODUCER = f"{PROJECT_PATH}/producers/transactions_producer.py"

# Spark Jobs
SPARK_PACKAGES = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"

SPARK_USER_ACTIVITY = f"{PROJECT_PATH}/spark_jobs/user_activity.py"
SPARK_ORDERS_TRANSACTIONS = f"{PROJECT_PATH}/spark_jobs/orders_transactions.py"
SPARK_AGGREGATE_METRICS = f"{PROJECT_PATH}/spark_jobs/aggregate_metrics.py"
SPARK_TO_S3 = f"{PROJECT_PATH}/spark_jobs/to_s3.py"

# ------------------------
# DAG Default Args
# ------------------------
default_args = {
    "owner": "gowtham",
    "start_date": datetime(2026, 4, 1),
    "retries": 0,
}

# ------------------------
# DAG Definition
# ------------------------
with DAG(
    dag_id="retailpulse_etl_and_spark_parallel",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["retail", "etl", "spark"]
) as dag:

    # ------------------------
    # Producers (run in parallel)
    # ------------------------
    run_orders_producer = BashOperator(
        task_id="run_orders_producer",
        bash_command=f"{VENV_PYTHON} {ORDERS_PRODUCER} &",
    )

    run_user_activity_producer = BashOperator(
        task_id="run_user_activity_producer",
        bash_command=f"{VENV_PYTHON} {USER_ACTIVITY_PRODUCER} &",
    )

    run_transactions_producer = BashOperator(
        task_id="run_transactions_producer",
        bash_command=f"{VENV_PYTHON} {TRANSACTIONS_PRODUCER} &",
    )

    # ------------------------
    # Delay Sensors for Spark jobs (staggered start)
    # ------------------------
    delay_30 = TimeDeltaSensor(task_id="delay_30", delta=timedelta(seconds=30))
    delay_60 = TimeDeltaSensor(task_id="delay_60", delta=timedelta(seconds=60))
    delay_90 = TimeDeltaSensor(task_id="delay_90", delta=timedelta(seconds=90))

    # ------------------------
    # Spark Jobs
    # ------------------------
    spark_user_activity = BashOperator(
        task_id="spark_user_activity",
        bash_command=f"""
        source {VENV_BIN}
        cd {PROJECT_PATH}
        spark-submit --packages {SPARK_PACKAGES} --py-files config.zip {SPARK_USER_ACTIVITY}
        """
    )

    spark_orders_transactions = BashOperator(
        task_id="spark_orders_transactions",
        bash_command=f"""
        source {VENV_BIN}
        cd {PROJECT_PATH}
        spark-submit --packages {SPARK_PACKAGES} --py-files config.zip {SPARK_ORDERS_TRANSACTIONS}
        """
    )

    spark_aggregate_metrics = BashOperator(
        task_id="spark_aggregate_metrics",
        bash_command=f"""
        source {VENV_BIN}
        cd {PROJECT_PATH}
        spark-submit --packages {SPARK_PACKAGES} --py-files config.zip {SPARK_AGGREGATE_METRICS}
        """
    )

    spark_to_s3 = BashOperator(
        task_id="spark_to_s3",
        bash_command=f"""
        source {VENV_BIN}
        cd {PROJECT_PATH}
        spark-submit --packages {SPARK_PACKAGES} {SPARK_TO_S3}
        """
    )

    # ------------------------
    # Execution Graph
    # ------------------------

    # All producers run in parallel
    [run_orders_producer, run_user_activity_producer, run_transactions_producer]

    
    # Staggered start of Spark jobs
    delay_30 >> spark_orders_transactions
    delay_60 >> spark_aggregate_metrics
    delay_90 >> spark_to_s3