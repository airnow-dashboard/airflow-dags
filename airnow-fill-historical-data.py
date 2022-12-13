from os import environ
from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.providers.docker.operators.docker import DockerOperator

from docker.types import Mount

output_target = "/app/output"

piper_env_vars = {
    "AIRNOW_DB_HOST": environ.get("AIRNOW_DB_HOST"),
    "AIRNOW_DB_NAME": environ.get("AIRNOW_DB_NAME"),
    "AIRNOW_DB_USER": environ.get("AIRNOW_DB_USER"),
    "AIRNOW_DB_PASSWORD": environ.get("AIRNOW_DB_PASSWORD"),
}

shared_volume = Mount(
    target=output_target,
    source="/var/lib/airnow",
    type="bind",
    read_only=False
)

with DAG(
    "airnow-fill-historical-data",
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
    },
    description="Autoupdate pipeline for airnow",
    schedule_interval=None,     # never run periodically
    start_date=datetime(2022, 12, 1),
    catchup=False,
    tags=["one-off"],
) as dag:

    scraper = DockerOperator(
        task_id="scrape_data",
        image="airnow-scraper",
        auto_remove="force",
        mounts=[shared_volume],
        command="historical"
    )

    scraper.doc_md = dedent(
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

    piper = DockerOperator(
        task_id="piper",
        image="piper",
        auto_remove="force",
        mounts=[shared_volume],
        command=[output_target, "historical"],
        network_mode="host",
        environment=piper_env_vars
    )

    scraper >> piper