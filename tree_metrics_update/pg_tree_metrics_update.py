from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from airflow.models import Variable
from airflow.models.dagrun import DagRun
import pendulum
import logging

logger = logging.getLogger('airflow')
logger.setLevel(logging.INFO)

default_args = {
    'owner': 'airflow',
    'start_date': pendulum.datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay' : pendulum.duration(minutes=2)
}

DAG_NAME = 'pg_tree_metrics_update'
SCHEDULE = '4 0 * * *' # обновление каждый день, с 8 по 15 число каждого месяца дополнительное обновление
TAGS = ["test", "internal", "clickhouse"]

AIRFLOW_HOME = 'AIRFLOW_HOME'
DAGS_SCRIPTS = f'/dags/sql_scripts/{DAG_NAME}/'

# универсальная перменная для определения количества дней, которые обновляем
BACKFILL_DAYS = int(Variable.get("backfill_days"))

# Скрипты разделены на группы, номер определяет приоритет, чем меньше, тем раньше они должны обновиться
SCRIPTS_0 = ['bi_metric_alt.sql','bi_metric_load_factor.sql','bi_metric_ltv_cac.sql','bi_load_factor_with_closed_clubs.sql']
SCRIPTS_1 = ['bi_metric_tree_by_month.sql']
SCRIPTS_2 = ['bi_metric_tree_by_year.sql']

# построение полного пути до sql файлов
def make_path(script_name):
    import os
    
    airflow_home = os.environ[AIRFLOW_HOME]
    
    return airflow_home + DAGS_SCRIPTS + script_name


@dag(
    dag_id = DAG_NAME,
    default_args = default_args,
    schedule_interval = SCHEDULE,
    catchup = False,
    tags = TAGS
)
def run_flow():

    '''
    одновременно может выполняться не больше 3х скриптов + airflow выводит имена скриптов
    ''' 
    @task(max_active_tis_per_dag=3, map_index_template="{{ script }}")
    def run_script_clickhouse(script):

        import time

        clickhouse_hook = ClickHouseHook(
                                        database='ddxfitness_prod_v2', 
                                        clickhouse_conn_id='clickhouse_peerdb_prod')

        with open(make_path(script)) as f:
            sql_text = f.read()

        date_to = pendulum.now().start_of('day')
        date_from = date_to.subtract(days=BACKFILL_DAYS)

        # параметры для sql файла, если необходимы, игнорируются, если их нет в sql 
        script_parameters = {
            "date_from": date_from.to_date_string(),
            "date_to": date_to.to_date_string()
        }

        sql_script = sql_text.format(**script_parameters)

        sql_scripts = sql_script.split(';')

        start = time.time()
        result = clickhouse_hook.execute(sql_scripts)
        end = time.time()

        logger.info(f"Script {script} run for {round(end-start)} seconds.")

    # выполняем скрипты 1 и скрипты 2 только с 8 по 15 числа
    @task.short_circuit()
    def check_day(dag_run: DagRun | None = None):

        run_id = dag_run.run_id
        is_manual = run_id.startswith('manual__')
        bypass = int(Variable.get("bypass_check_tree_metrics"))

        day = pendulum.now().day

        # в случае ручного обновления и флага bypass_check = 1 - обновляем все
        # Если bypass=2, то обновляем всегда (каждый день)
        if bypass == 2:
            logger.info("Bypassing always")
            return True

        if is_manual and (bypass == 1):
            logger.info("Bypassing check for dates")
            return True

        if day>=8 and day<16:
            logger.info("Making aggregated updates (date within updatable period)")
            return True

        logger.info("Skipping aggregated updates")

        return False

    # динамические таски, на каждый sql свой таск
    run_scripts_0 = run_script_clickhouse.override(task_id="run_scripts_0").expand(script = SCRIPTS_0)
    run_scripts_1 = run_script_clickhouse.override(task_id="run_scripts_1").expand(script = SCRIPTS_1)
    run_scripts_2 = run_script_clickhouse.override(task_id="run_scripts_2").expand(script = SCRIPTS_2)

    run_scripts_0 >> check_day() >> run_scripts_1 >>  run_scripts_2

run_flow()
