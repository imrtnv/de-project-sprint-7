#импорт библиотек для инициализации спарка
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os
import findspark
findspark.init()
findspark.find()

#переменные окружения спарка
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'

#импорт библиотек для DAG
from airflow import DAG
from datetime import date, datetime, timedelta
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


#задаем все переменные далее по коду (они будут обозначены коментариями в коде где они используются)
sname = "imrtnv"
hdfs_path = "hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020"
geo_path = "/user/master/data/geo/events/"
citygeodata_csv = f"{hdfs_path}/user/{sname}/data/citygeodata/"

## /usr/lib/spark/bin/spark-submit --master yarn --deploy-mode cluster initial_load_job.py imrtnv hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020 /user/master/data/geo/events/
## spark-submit --master yarn --deploy-mode cluster initial_load_job.py sname hdfs_path geo_path

args = {
    "owner": "imrtnv",
    'email': ['imrtnv@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3
}

with DAG(
        'Initial_full_migration_by_date_and_evend_type',
        default_args=args,
        description='Launch pyspark job for migration data',
        catchup=False,
        schedule_interval=None,
        start_date=datetime(2022, 10, 14),
        tags=['pyspark',  'hdfs'],
        is_paused_upon_creation=True,
) as dag:

    start_task = DummyOperator(task_id='start')

    initial_load_task = SparkSubmitOperator(
        task_id='initial_load_job',
        application ='/scripts/initial_load_job.py' ,
        conn_id= 'yarn_spark',
        application_args = [sname, hdfs_path, geo_path],
        conf={
        "spark.driver.maxResultSize": "20g"
        },
        executor_cores = 2,
        executor_memory = '4g'
    )

    end_task = DummyOperator(task_id='end')

    start_task >> initial_load_task >> end_task