from requests import Session, Response
import json
from typing import Optional


_base_url = "https://api.eu.tempo.io/4"
_s = Session()


def init(token):
    _s.headers = {
        'Content-Type': "application/json",
        'Authorization': f"Bearer {token}"
    }


def jira_to_tempo_worklog_ids(jira_worklog_ids: list[int]) -> Optional[dict[int, int]]:
    """
        maps between jira worklog id and internal tempo worklog

        jira_worklog_ids: list of unique jira worklog ids

        returns {
            [tempo_worklog_id : int]: [jira_worklog_id : int],
            ...
        }
    """
    result = {}
    req = {
        'jiraWorklogIds': jira_worklog_ids
    }
    resp = _raw_post("/worklogs/jira-to-tempo?limit=500", data=req)
    if resp is None:
        return
    if resp.status_code < 200 or resp.status_code >= 300:
        return
    data = resp.json()
    for map in data['results']:
        result[map['tempoWorklogId']] = map['jiraWorklogId']
    next = _parse_next(data['metadata'])
    while next is not None:
        resp = _raw_post(next, data=req)
        if resp.status_code < 200 or resp.status_code >= 300:
            continue
        data = resp.json()
        for map in data['results']:
            result[map['tempoWorklogId']] = map['jiraWorklogId']
        next = _parse_next(data['metadata'])
    return result


def worklog_author(worklog_id: int) -> Optional[str]:
    resp = _raw_get(f"/worklogs/{worklog_id}")
    if resp.status_code < 200 or resp.status_code >= 300:
        return None
    data = resp.json()
    return data['author']['accountId']


def _parse_next(metadata: dict) -> Optional[str]:
    next: Optional[str] = metadata['next'] if "next" in metadata.keys() else None
    if next is not None:
        return next[len(_base_url):]
    return None


def _raw_get(endpoint, params=None) -> Response:
    assert endpoint is not None and len(endpoint) > 0
    raw_response = _s.get(f"{_base_url}{endpoint}", params=params)
    return raw_response


def _raw_post(endpoint, data: Optional[dict] = None) -> Response:
    assert endpoint is not None and len(endpoint) > 0
    raw_response = _s.post(f"{_base_url}{endpoint}", data=json.dumps(data))
    return raw_response
