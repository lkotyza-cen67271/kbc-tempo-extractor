#!/usr/bin/env python3.10
from keboola.component.dao import BaseType, ColumnDefinition, SupportedDataTypes
import tempo
from typing import Optional


FILENAME_TEAMS = "teams.csv"
FILENAME_TEAM_MEMBERSHIPS = "team_membership.csv"

_TABLE_TEAMS = "teams"
_TABLE_TEAM_MEMBERSHIPS = "team_membership"

_COL_TEAM_ID = "team_id"
_COL_USER_ID = "account_id"
_COL_ID = "id"
_COL_LEAD_ID = "team_lead_id"
_COL_TEAM_NAME = "team_name"


def table_column_definitions() -> dict[str, dict[str, ColumnDefinition]]:
    return {
        _TABLE_TEAM_MEMBERSHIPS: {
            _COL_TEAM_ID: ColumnDefinition(
                data_types=BaseType(dtype=SupportedDataTypes.INTEGER, length="20"),
                nullable=False,
                primary_key=True,
                description="Team ID"
            ),
            _COL_USER_ID: ColumnDefinition(
                data_types=BaseType(dtype=SupportedDataTypes.STRING, length="100"),
                nullable=False,
                primary_key=True,
                description="Account ID"
            ),
        },
        _TABLE_TEAMS: {
            _COL_ID: ColumnDefinition(
                data_types=BaseType(dtype=SupportedDataTypes.INTEGER, length="20"),
                nullable=False,
                primary_key=True,
                description="TEAM ID"
            ),
            _COL_LEAD_ID: ColumnDefinition(
                data_types=BaseType(dtype=SupportedDataTypes.STRING, length="100"),
                nullable=False,
                primary_key=False,
                description="Team Lead's account_id"
            ),
            _COL_TEAM_NAME: ColumnDefinition(
                data_types=BaseType(dtype=SupportedDataTypes.STRING, length="100"),
                nullable=False,
                primary_key=False,
                description="Name of the Team"
            )
        }}


def run() -> dict[str, Optional[list[dict]]]:
    team_data = list()
    team_membership_data = list()
    # Process Teams
    teams = tempo.teams()
    if teams is None:
        return {_TABLE_TEAMS: None, _TABLE_TEAM_MEMBERSHIPS: None}
    for team in teams:
        team_data.append(_transform_team(team))
        # Load Users in Team
        memberships = tempo.team_membership(team['id'])
        if memberships is None:
            return {_TABLE_TEAMS: None, _TABLE_TEAM_MEMBERSHIPS: None}
        for membership in memberships:
            team_membership_data.append(_transform_team_membership(membership))
    return {
        _TABLE_TEAMS: team_data,
        _TABLE_TEAM_MEMBERSHIPS: team_membership_data
    }


def _transform_team(team: dict) -> dict[str, str | int]:
    return {
        _COL_ID: team['id'],
        _COL_TEAM_NAME: team['name'],
        _COL_LEAD_ID: team['lead']['accountId']
    }


def _transform_team_membership(membership: dict) -> dict:
    return {
        _COL_TEAM_ID: membership['team']['id'],
        _COL_USER_ID: membership['member']['accountId']
    }
