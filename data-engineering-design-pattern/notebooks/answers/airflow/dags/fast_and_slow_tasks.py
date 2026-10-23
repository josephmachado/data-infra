import pendulum
from airflow.sdk import dag, get_current_context, task
from airflow.timetables.interval import CronDataIntervalTimetable
from datetime import timedelta

@dag(
    dag_id="fast_and_slow_tasks",
    schedule=CronDataIntervalTimetable(
        "* * * * *",  # every min
        timezone=pendulum.timezone("UTC"),
    ),
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
)
def fast_and_slow_pipeline():

    @task.branch
    def branch() -> str:

        context = get_current_context()
        minute = context["data_interval_start"].minute
        if minute % 5 == 0:
            return "slow_task"
        return "fast_task"

    @task
    def fast_task() -> None:
        from airflow.sdk import get_current_context

        context = get_current_context()
        start = context["data_interval_start"].strftime("%Y-%m-%d %H:%M:%S")
        end = context["data_interval_end"].strftime("%Y-%m-%d %H:%M:%S")
        print(f"[fast_task] start={start} end={end}")

    @task
    def slow_task() -> None:
        from airflow.sdk import get_current_context

        context = get_current_context()
        start = context["data_interval_start"].strftime("%Y-%m-%d %H:%M:%S")
        end = (context["data_interval_start"] - timedelta(minutes=60)).strftime("%Y-%m-%d %H:%M:%S")
        print(f"[slow_task] start={start} end={end}")

    branch() >> [fast_task(), slow_task()]


fast_and_slow_pipeline()
