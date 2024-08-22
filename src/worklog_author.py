from typing import Optional
from datetime import datetime as dt

from keboola.component.dao import BaseType, ColumnDefinition, SupportedDataTypes
import jirac as jc
import tempo
import logging


FILENAME = "worklog_authors.csv"

COL_JIRA_WORKLOG_ID: str = "jira_worklog_id"
COL_AUTHOR_ID: str = "account_id"


def column_definitions() -> dict[str, ColumnDefinition]:
    return {
        COL_JIRA_WORKLOG_ID: ColumnDefinition(
            data_types=BaseType(dtype=SupportedDataTypes.INTEGER, length="20"),
            nullable=False,
            primary_key=True,
            description="Jira worklog id"
        ),
        COL_AUTHOR_ID: ColumnDefinition(
            data_types=BaseType(dtype=SupportedDataTypes.STRING, length="200"),
            nullable=False,
            primary_key=False,
            description="Jira user id"
        )
    }


def run(since: str) -> Optional[list[dict[str, str | int]]]:
    """
    loads jira worklog ids from 'since' and
    maps them to tempo worklog id and than finds author info from tempo

    since: isoformat date '2020-01-30'

    returns
    [
        { 'jira_worklog_id': int, 'account_id': str },
        ...
    ]
    """
    logging.info("started worklog_authors")
    date_from = dt.fromisoformat(since)
    since_mls = int(date_from.timestamp()) * 1_000
    # load jira worklog ids
    worklog_ids = jc.worklog_ids(since_mls)
    if worklog_ids is None:
        logging.error("[worklog_authors] failed to get jira worklogs")
        return
    logging.debug("[worklog_authors] loaded jira worklogs")
    # get mapping between jira id and tempo id
    mapped = tempo.jira_to_tempo_worklog_ids(worklog_ids)
    if mapped is None:
        logging.error("Failed to get mapping")
        return
    logging.debug("[worklog_authors] finished mapping jira to tempo")
    # get author id and write to file_output for each worklog
    current = -1
    file_ouput = []
    for tempo_id, jira_id in mapped.items():
        current += 1
        author = tempo.worklog_author(tempo_id)
        if author is None:
            logging.warning(f"[worklog_authors] unable to find author for jira_worklog_id {jira_id}")
            continue
        out = {
            'jira_worklog_id': jira_id,
            'account_id': author
        }
        file_ouput.append(out)
    logging.info("worklog_authors done")
    return file_ouput
