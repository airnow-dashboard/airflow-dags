from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.providers.docker.operators.docker import DockerOperator

from docker.types import Mount

shared_volume = Mount(
    target="/app/output",
    source="airnow-shared-volume",
    type="volume",
    read_only=False
)

with DAG(
    "airnow-autoupdate",
    is_paused_upon_creation=False,  # enable upon DAG creation
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        "depends_on_past": False,
        "email": ["admin@solsyn.dev"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function,
        # 'on_success_callback': some_other_function,
        # 'on_retry_callback': another_function,
        # 'sla_miss_callback': yet_another_function,
        # 'trigger_rule': 'all_success'
    },
    description="Autoupdate pipeline for airnow",
    schedule=timedelta(hours=4),
    start_date=datetime(2022, 12, 1),
    catchup=False,
    tags=["recurrent"],
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    t1 = DockerOperator(
        task_id="scrape_data",
        image="airnow-scraper",
        # env_file=None,
        auto_remove="force",
        mounts=[shared_volume],
        entrypoint="current"
    )

    t1.doc_md = dedent(
        """\
    #### Task Documentation
    You can document your task using the attributes `doc_md` (markdown),
    `doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
    rendered in the UI's Task Instance Details page.
    ![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)
    **Image Credit:** Randall Munroe, [XKCD](https://xkcd.com/license.html)
    """
    )

    dag.doc_md = __doc__  # providing that you have a docstring at the beginning of the DAG; OR
    dag.doc_md = """
    This is a documentation placed anywhere
    """  # otherwise, type it like this

    t1