from typing import Optional
import datetime as dt
import jirac as jc
import tempo
import logging


FILENAME = "worklog_authors.csv"


def run(since: str) -> Optional[list[dict[str, str | int]]]:
    """
    maps jira worklog id to tempo worklog id and finds author info from tempo

    since: isoformat date '2020-01-30'

    returns
    [
        { 'jira_worklog_id': int, 'account_id': str },
        ...
    ]
    """
    date_from = dt.datetime.fromisoformat(since)
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
        logging.error("main", "Failed to get mapping")
        return
    logging.debug("[worklog_authors] finished mapping jira to tempo")
    # get author id and write to file_output for each worklog
    current = -1
    file_ouput = []
    for tempo_id, jira_id in mapped.items():
        current += 1
        author = tempo.worklog_author(tempo_id)
        if author is None:
            continue
        out = {
            'jira_worklog_id': jira_id,
            'account_id': author
        }
        file_ouput.append(out)
    logging.info("worklog_authors done")
    return file_ouput
