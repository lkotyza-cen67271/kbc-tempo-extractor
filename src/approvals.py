#!/usr/bin/env python3.10
from keboola.component.dao import BaseType, ColumnDefinition, SupportedDataTypes
from datetime import datetime, timedelta
import tempo
import uuid
from typing import Optional


FILENAME_APPROVALS = "approvals.csv"
FILENAME_APPROVAL_WORKLOGS = "approval_worklogs.csv"

_TABLE_APPROVALS = "approvals"
_TABLE_APPROVAL_WORKLOGS = "approval_worklogs"
_COL_APPR_ID = "approval_id"
_COL_WL_ID = "worklog_id"
_COL_ID = "id"
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
                description="Approval ID"
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
            _COL_START: ColumnDefinition(
                data_types=BaseType(dtype=SupportedDataTypes.TIMESTAMP),
                nullable=False,
                primary_key=False,
                description="Approval period START date"
            ),
            _COL_END: ColumnDefinition(
                data_types=BaseType(dtype=SupportedDataTypes.TIMESTAMP),
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
    # Load Team info
    all_teams = tempo.teams()
    if all_teams is None:
        return ([], [])
    itrp_team_id: Optional[int] = None
    for team in all_teams:
        if team['name'] == "A830_04 IT Resource Pool":
            itrp_team_id = team['id']
    if itrp_team_id is None:
        return ([], [])

    # Load Approvals
    raw_out: list[dict] = []
    period_start_date = since
    while period_start_date < datetime.now():
        period = tempo.team_timesheet_approvals(itrp_team_id, str(period_start_date.date()))
        if period is None:
            return ([], [])
        raw_out.extend(period)
        period_start_date = _next_period_start_from_current(period)
        if period_start_date is None:
            return ([], [])

    out = _transform_periods_for_keboola(all_periods=raw_out)
    return out


def _next_period_start_from_current(approvals: list[dict]) -> Optional[datetime]:
    if len(approvals) == 0:
        return
    return datetime.fromisoformat(approvals[0]['period']['to']) + timedelta(days=1)


def _timestamp_from_iso_str(iso_str: str) -> float:
    dt = datetime.fromisoformat(iso_str)
    return dt.timestamp()


def _transform_periods_for_keboola(all_periods: list[dict]) -> tuple[list[dict], list[dict]]:
    """
        Data for keboola needs to be transformed to flat structure coresponding to the table scheme

        returns (approval_list, worklog_list)
    """
    appr_output = []
    wl_output = []
    for period in all_periods:
        appr_id = uuid.uuid4()
        appr_out = {
            _COL_ID: appr_id,
            _COL_START: _timestamp_from_iso_str(period['period']['from']),
            _COL_END: _timestamp_from_iso_str(period['period']['to']),
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
