from keboola.component.dao import BaseType, ColumnDefinition, SupportedDataTypes, logging
from datetime import datetime
import tempo
from typing import Any


FILENAME = "worklogs.csv"

_COL_ID = "tempo_id"
_COL_ISSUE_ID = "issue_id"
_COL_AUTHOR_ACCOUNT_ID = "author_account_id"
_COL_START_DATE_TIME_UTC = "start_date_time_utc"
_COL_TIME_SPENT_SECONDS = "time_spent_seconds"
_COL_CREATED = "created"
_COL_UPDATED = "updated"


def column_definitions() -> dict[str, Any]:
    return {
        _COL_ID: ColumnDefinition(
            data_types=BaseType(dtype=SupportedDataTypes.INTEGER),
            nullable=False,
            primary_key=True,
            description="ID of worklog in Tempo system"
        ),
        _COL_ISSUE_ID: ColumnDefinition(
            data_types=BaseType(dtype=SupportedDataTypes.INTEGER),
            nullable=False,
            primary_key=False,
            description="issue ID"
        ),
        _COL_AUTHOR_ACCOUNT_ID: ColumnDefinition(
            data_types=BaseType(dtype=SupportedDataTypes.STRING, length=300),
            nullable=False,
            primary_key=False,
            description="author of the worklog"
        ),
        _COL_START_DATE_TIME_UTC: ColumnDefinition(
            data_types=BaseType(dtype=SupportedDataTypes.DATE),
            nullable=False,
            primary_key=False,
            description="start date of the worklog"
        ),
        _COL_TIME_SPENT_SECONDS: ColumnDefinition(
            data_types=BaseType(dtype=SupportedDataTypes.INTEGER, length="100"),
            nullable=False,
            primary_key=False,
            description="time spent"
        ),
        _COL_CREATED: ColumnDefinition(
            data_types=BaseType(dtype=SupportedDataTypes.DATE),
            nullable=False,
            primary_key=False,
            description="worklog created date"
        ),
        _COL_UPDATED: ColumnDefinition(
            data_types=BaseType(dtype=SupportedDataTypes.DATE),
            nullable=False,
            primary_key=False,
            description="worklog last updated date"
        ),
    }


def run(since: datetime) -> list[dict[str, Any]]:
    """
    since: datetime
    """
    def map_worklog_to_table(original_wl: dict) -> dict:
        startDTUTC = ""
        if "startDateTimeUtc" in original_wl.keys():
            startDTUTC = original_wl['startDateTimeUtc']
        else:
            startDTUTC = f"{original_wl['startDate']}T{original_wl['startTime']}.000Z"
        return {
            _COL_ID: original_wl['tempoWorklogId'],
            _COL_ISSUE_ID: original_wl['issue']['id'],
            _COL_AUTHOR_ACCOUNT_ID: original_wl['author']['accountId'],
            _COL_TIME_SPENT_SECONDS: original_wl['timeSpentSeconds'],
            _COL_START_DATE_TIME_UTC: startDTUTC,
            _COL_CREATED: original_wl['createdAt'],
            _COL_UPDATED: original_wl['updatedAt']
        }
    logging.info("Started to download worklogs")
    data = tempo.worklogs_updated_from(str(since.date()), map_worklog_to_table)
    logging.info("Download finished successfully")
    return data
