import sys
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from contextlib import redirect_stdout
import io

ROOT_FOLDER_LOCATION = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT_FOLDER_LOCATION))

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

    tasks = [
        ("Task 1", dags_campaign_insights),
        ("Task 2", dags_ad_insights),
    ]

    print("🔄 Starting parallel execution...\n")

    def run_task(task_label, fn):
        buffer = io.StringIO()

        try:
            # Mỗi thread có buffer riêng
            with redirect_stdout(buffer):
                fn(
                    access_token=access_token,
                    account_id=account_id,
                    start_date=start_date,
                    end_date=end_date,
                )

            return task_label, "SUCCESS", buffer.getvalue()

        except Exception as e:
            return task_label, "FAILED", buffer.getvalue() + f"\n{str(e)}"

    results = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(run_task, label, fn)
            for label, fn in tasks
        ]

        # Chờ tất cả hoàn thành
        for future in futures:
            results.append(future.result())

    # 🔥 In sau khi tất cả hoàn thành
    print("\n================ FINAL OUTPUT ================\n")

    for task_label, status, output in results:
        print(f"{task_label} - {status}:")
        print(output.strip())
        print("-----\n")

    print("✅ Done.")