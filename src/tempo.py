from requests import Session
import json
import math
import logger as log
from typing import Optional


_base_url = "https://api.eu.tempo.io/4"
_buffer_size = 1_000
_s = Session()


def init(token):
    _s.headers = {
        'Content-Type': "application/json",
        'Authorization': f"Bearer {token}"
    }


def raw_get(endpoint, params=None):
    if endpoint is None and len(endpoint) == 0:
        return None
    raw_response = _s.get(f"{_base_url}{endpoint}", params=params)
    return raw_response


def raw_put(endpoint, data: Optional[dict] = None):
    if endpoint is None and len(endpoint) == 0:
        return None
    raw_response = _s.put(f"{_base_url}{endpoint}", data=json.dumps(data))
    return raw_response


def raw_post(endpoint, data: Optional[dict] = None):
    if endpoint is None and len(endpoint) == 0:
        return None
    raw_response = _s.post(f"{_base_url}{endpoint}", data=json.dumps(data))
    return raw_response


def worklogs_for_issue(issue_id: str) -> Optional[dict]:
    result = []
    resp = raw_get(f"/worklogs/issue/{issue_id}")
    if resp.status_code < 200 or resp.status_code >= 300:
        log.error("worklogs_for_issue", f"{resp.status_code} - {resp.text}")
        return
    data = resp.json()
    result.extend(data['results'])
    next: Optional[str] = _parse_next(data['metadata'])
    while next is not None:
        resp = raw_get(next)
        if resp.status_code < 200 or resp.status_code >= 300:
            log.error("worklogs_for_issue", f"[rec] {resp.status_code} {resp.text}")
            return
        data = resp.json()
        result.extend(data['results'])
        next: Optional[str] = _parse_next(data['metadata'])
    return result


def tempo_to_jira_worklog_ids(tempo_worklog_ids: list[int]) -> Optional[dict]:
    result = {}
    req = {
        'tempoWorklogIds': tempo_worklog_ids
    }
    resp = raw_post("/worklogs/tempo-to-jira", data=req)
    if resp.status_code < 200 or resp.status_code >= 300:
        log.error("tempo_to_jira_worklog_ids", f"{resp.status_code} {resp.text}")
        return
    data = resp.json()
    next: Optional[str] = _parse_next(data['metadata'])
    for map in data['results']:
        result[map['tempoWorklogId']] = map['jiraWorklogId']
    while next is not None:
        resp = raw_post(next, data=req)
        if resp.status_code < 200 or resp.status_code >= 300:
            log.error("tempo_to_jira_worklog_ids", f"[rec] {resp.status_code} {resp.text}")
            return
        data = resp.json()
        next: Optional[str] = _parse_next(data['metadata'])
        for map in data['results']:
            result[map['tempoWorklogId']] = map['jiraWorklogId']
    return result


def jira_to_tempo_worklog_ids(jira_worklog_ids: list[int]) -> Optional[dict[str, int]]:
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
    resp = raw_post("/worklogs/jira-to-tempo?limit=500", data=req)
    if resp.status_code < 200 or resp.status_code >= 300:
        log.error("jira_to_tempo_worklog_ids", f"{resp.status_code} {resp.text}")
        return
    data = resp.json()
    for map in data['results']:
        result[map['tempoWorklogId']] = map['jiraWorklogId']
    next = _parse_next(data['metadata'])
    while next is not None:
        resp = raw_post(next, data=req)
        if resp.status_code < 200 or resp.status_code >= 300:
            log.error("jira_to_tempo_worklog_ids", f"{resp.status_code} {resp.text}")
            continue
        data = resp.json()
        for map in data['results']:
            result[map['tempoWorklogId']] = map['jiraWorklogId']
        next = _parse_next(data['metadata'])
        current = data['metadata']['offset'] + data['metadata']['count']
        log.status_line(current, len(jira_worklog_ids), "mapping jira to tempo")
    return result


def worklog_author(worklog_id: int) -> Optional[str]:
    resp = raw_get(f"/worklogs/{worklog_id}")
    if resp.status_code < 200 or resp.status_code >= 300:
        log.error("worklog_author", f"{resp.url}\n{resp.status_code} {resp.text}")
        return None
    data = resp.json()
    return data['author']['accountId']


def _parse_next(metadata: dict) -> Optional[str]:
    next: Optional[str] = metadata['next'] if "next" in metadata.keys() else None
    if next is not None:
        return next[len(_base_url):]
    return None
