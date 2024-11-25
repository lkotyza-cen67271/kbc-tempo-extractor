#!/usr/bin/env python3.10
from keboola.component.dao import BaseType, ColumnDefinition, SupportedDataTypes, logging
from datetime import datetime, timedelta
import tempo
import hashlib
from typing import Optional
import time


FILENAME_APPROVALS = "approvals.csv"
FILENAME_APPROVAL_WORKLOGS = "approval_worklogs.csv"

_TABLE_APPROVALS = "approvals"
_TABLE_APPROVAL_WORKLOGS = "approval_worklogs"
_COL_APPR_ID = "approval_id"
_COL_WL_ID = "worklog_id"
_COL_ID = "id"
_COL_TEAM_ID = "team_id"
_COL_START = "period_start"
_COL_END = "period_end"
_COL_USER = "account_id"
_COL_STATUS = "status"


def table_column_definitions() -> dict[str, dict[str, ColumnDefinition]]:
    return {
        _TABLE_APPROVAL_WORKLOGS: {
            _COL_WL_ID: ColumnDefinition(
                data_types=BaseType(dtype=SupportedDataTypes.INTEGER, length="20"),
                nullable=False,
                primary_key=True,
                description="Worklog ID"
            ),
            _COL_APPR_ID: ColumnDefinition(
                data_types=BaseType(dtype=SupportedDataTypes.STRING, length="100"),
                nullable=False,
                primary_key=True,
                description="Approval ID"
            ),
        },
        _TABLE_APPROVALS: {
            _COL_ID: ColumnDefinition(
                data_types=BaseType(dtype=SupportedDataTypes.STRING, length="100"),
                nullable=False,
                primary_key=True,
                description="Approval ID"
            ),
            _COL_TEAM_ID: ColumnDefinition(
                data_types=BaseType(dtype=SupportedDataTypes.INTEGER, length="20"),
                nullable=False,
                primary_key=False,
                description="Team ID"
            ),
            _COL_START: ColumnDefinition(
                data_types=BaseType(dtype=SupportedDataTypes.DATE),
                nullable=False,
                primary_key=False,
                description="Approval period START date"
            ),
            _COL_END: ColumnDefinition(
                data_types=BaseType(dtype=SupportedDataTypes.DATE),
                nullable=False,
                primary_key=False,
                description="Approval period END date"
            ),
            _COL_USER: ColumnDefinition(
                data_types=BaseType(dtype=SupportedDataTypes.STRING, length="100"),
                nullable=False,
                primary_key=False,
                description="User to which is the approval associated"
            ),
            _COL_STATUS: ColumnDefinition(
                data_types=BaseType(dtype=SupportedDataTypes.STRING, length="20"),
                nullable=False,
                primary_key=False,
                description="Status of the approval"
            )
        }}


def run(since: datetime) -> tuple[list[dict], list[dict]]:
    """
    since: datetime

    returns tupple(approvals, approval_worklogs)
    """
    # Load Team info
    all_teams = tempo.teams()
    if all_teams is None:
        return ([], [])
    result: dict[str, list] = {
        "approvals": [],
        "approval_worklogs": []
    }
    for team in all_teams:
        # Load Approvals per team
        raw_out: list[dict] = []
        period_start_date = since
        while period_start_date < datetime.now():
            period = tempo.team_timesheet_approvals(team['id'], str(period_start_date.date()))
            if period is None:
                logging.warning("Period is None - retry 5 times")
                logging.warning(f"(team: {team['id']} - {team['name']}; period_start: {str(period_start_date.date())})")
                # sometimes call fails for no apparent reason so retry if failed
                for i in range(5):
                    time.sleep(2)
                    period = tempo.team_timesheet_approvals(team['id'], str(period_start_date.date()))
                    if period is not None:
                        break
                    logging.warning(f"team:{team['id']} Retry number {i+1} / 5")
                    if i == 4:
                        logging.error(f"Retrying approvals failed. Skipping team {team['id']}...")
            if period is None:
                break
            raw_out.extend(period)
            next_period_start_date = _next_period_start_from_current(period)
            if next_period_start_date is None:
                logging.debug("period_start_date is None increment manually (+1week)")
                next_period_start_date = period_start_date + timedelta(weeks=1)
            period_start_date = next_period_start_date
        appr, appr_worklogs = _transform_periods_for_keboola(all_periods=raw_out, team_id=team['id'])
        result['approvals'].extend(appr)
        result['approval_worklogs'].extend(appr_worklogs)
    return (result['approvals'], result['approval_worklogs'])


def _next_period_start_from_current(approvals: list[dict]) -> Optional[datetime]:
    if len(approvals) == 0:
        return
    return datetime.fromisoformat(approvals[0]['period']['to']) + timedelta(days=1)


def _date_from_str(iso_str: str) -> str:
    dt = datetime.fromisoformat(iso_str)
    return dt.isoformat()


def _calculate_approval_id(team_id, period_dates: dict) -> str:
    id_source = f"{team_id};{period_dates['from']};{period_dates['to']}"
    hashed_id = hashlib.sha256()
    hashed_id.update(bytes(id_source, "utf-8"))
    return hashed_id.hexdigest()


def _transform_periods_for_keboola(all_periods: list[dict], team_id: int) -> tuple[list[dict], list[dict]]:
    """
        Data for keboola needs to be transformed to flat structure coresponding to the table scheme

        returns (approval_list, worklog_list)
    """

    appr_output = []
    wl_output = []
    for period in all_periods:
        appr_id = _calculate_approval_id(team_id, period['period'])
        appr_out = {
            _COL_ID: appr_id,
            _COL_TEAM_ID: team_id,
            _COL_START: _date_from_str(period['period']['from']),
            _COL_END: _date_from_str(period['period']['to']),
            _COL_STATUS: period['status'],
            _COL_USER: period['user']
        }
        appr_output.append(appr_out)
        for wl in period['worklogs']:
            wl_out = {
                _COL_APPR_ID: appr_id,
                _COL_WL_ID: wl
            }
            wl_output.append(wl_out)
    return (appr_output, wl_output)
