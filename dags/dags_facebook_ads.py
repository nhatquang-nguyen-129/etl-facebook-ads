import sys
from pathlib import Path
ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_FOLDER_LOCATION))

from concurrent.futures import ThreadPoolExecutor, as_completed
import contextlib
import io

from dags._dags_campaign_insights import dags_campaign_insights
from dags._dags_ad_insights import dags_ad_insights

def dags_facebook_ads(
    *,
    access_token: str,
    account_id: str,
    start_date: str,
    end_date: str,
    max_workers: int = 2,
):
    """
    DAG Orchestration for Facebook Ads
    ---
    Principles:
        1. Initialize parallel execution with worker pool
        2. Submit campaign-level and ad-level tasks concurrently
        3. Monitor task completion using asynchronous future handling
        4. Capture execution status and surface task-level failures
        5. Finalize DAG execution with total runtime reporting
    ---
    Returns:
        1. None:
    """

    tasks = {
        "campaign_insights": dags_campaign_insights,
        "ad_insights": dags_ad_insights,
    }

    for task in tasks:
        print(
            " 🔄 [DAGS] Triggering to execute Facebook Ads "
            f"{task} task with isolated stdout buffer..."
        )

    futures = {}

    with ThreadPoolExecutor(max_workers=max_workers) as executor:

        for task, fn in tasks.items():

            buffer = io.StringIO()

            future = executor.submit(
                lambda fn=fn, buffer=buffer: (
                    contextlib.redirect_stdout(buffer).__enter__(),
                    fn(
                        access_token=access_token,
                        account_id=account_id,
                        start_date=start_date,
                        end_date=end_date,
                    ),
                    contextlib.redirect_stdout(buffer).__exit__(None, None, None),
                    buffer.getvalue(),
                )
            )

            futures[future] = (task, buffer)

        completed = set()

        for future in as_completed(futures):
            task, buffer = futures[future]
            completed.add(task)

            status = "SUCCESS"
            output = ""

            try:
                _, _, _, output = future.result()
            except Exception as e:
                status = "FAILED"
                output = str(e)

            if status == "SUCCESS":
                print(
                     "\n✅ [DAGS] Successfully executed Facebook Ads " 
                      f"{task} task with isolated stdout buffer."
                    )
            else:
                print(
                    "\n❌ [DAGS] Failed to execute Facebook Ads "
                    f"{task} task with isolated stdout buffer."
                )
            print("-" * 3)

            if output:
                print(output.strip())

            print("-" * 3 + "\n")

            remaining = len(tasks) - len(completed)
            print(
                "\n⏳ [DAGS] Waiting for "
                f"{remaining} Facebook Ads remaining tasks..."
            )
            print("-" * 3 + "\n")