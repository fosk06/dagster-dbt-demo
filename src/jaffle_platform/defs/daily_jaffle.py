from typing import Union

import dagster as dg


@dg.schedule(cron_schedule="@daily", target="*")
def daily_jaffle(context: dg.ScheduleEvaluationContext) -> Union[dg.RunRequest, dg.SkipReason]:
    return dg.RunRequest(
        run_key=None,  # Unique key pour chaque run, None pour en générer un automatiquement
        run_config={},  # Configuration par défaut
        tags={"schedule_name": "daily_jaffle"}  # Tags pour identifier les runs de ce schedule
    )
