#!/usr/bin/env python3.10
from keboola.component.dao import BaseType, ColumnDefinition, SupportedDataTypes
from datetime import datetime
import tempo
from typing import Optional

FILENAME_APPROVALS = "approvals.csv"
FILENAME_APPROVAL_WORKLOGS = "approval_worklogs.csv"

_COL_ID = "id"
_COL_FROM = "from"
_COL_TO = "to"
_COL_USER = "account_id"
_COL_STATUS = "status"

def column_definitions() -> dict[str, ColumnDefinition]:
    return {
        _COL_ID: ColumnDefinition(
            data_types=BaseType(dtype=SupportedDataTypes.INTEGER, length="20"),
            nullable=False,
            primary_key=True,
            description="Approval ID"
        ),
        _COL_FROM: ColumnDefinition(
            data_types=BaseType(dtype=SupportedDataTypes.TIMESTAMP),
            nullable=False,
            primary_key=False,
            description="Approval period START date"
        ),
        _COL_TO: ColumnDefinition(
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
    }


def run(since: datetime) -> Optional[list[dict[str, str | int]]]:
    # Load Team info
    all_teams = tempo.teams()
    if all_teams is None:
        return
    itrp_team_id: Optional[int] = None
    for team in all_teams:
        if team['name'] == "A830_04 IT Resource Pool":
            itrp_team_id = team['id']

    # TODO
    if itrp_team_id is None:
        return
    period_approvals = tempo.team_timesheet_approvals(itrp_team_id, str(since.date()))


