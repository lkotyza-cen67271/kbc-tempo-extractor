from keboola.component.dao import logging
from requests import Session, Response
import json
import time
from typing import Optional, Callable


_base_url = "https://api.eu.tempo.io/4"
_s = Session()
_MAX_ITER_COUNT = 5


def init(token):
    _s.headers = {
        'Content-Type': "application/json",
        'Authorization': f"Bearer {token}"
    }


def tempo_to_jira_worklog_ids(tempo_worklog_ids: list[int]) -> Optional[dict[int, int]]:
    """
        maps between tempo worklog id and jira worklog id

        tempo_worklog_ids: list of unique tempo worklog ids

        returns {
            [tempo_worklog_id : int]: [jira_worklog_id : int],
            ...
        }
    """
    result = {}
    req = {
        'tempoWorklogIds': tempo_worklog_ids
    }
    resp = _raw_post("/worklogs/tempo-to-jira?limit=500", data=req)
    if resp is None or (resp.status_code < 200 or resp.status_code >= 300):
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


def team_membership(team_id: int) -> Optional[list[dict]]:
    """
    List of users in Tempo Team. https://apidocs.tempo.io/#tag/Team-Memberships/operation/getAllMemberships
    """
    resp = _raw_get(f"/team-memberships/team/{team_id}")
    if resp is None or (resp.status_code < 200 or resp.status_code >= 300):
        # log.error("tempo.team_memberships", "resp is None or status is not 2xx")
        return
    data = resp.json()
    return data['results']


def teams() -> Optional[list[dict]]:
    """
    List of teams in tempo. https://apidocs.tempo.io/#tag/Team

    returns [
        { id: text, summary: text, name: text, members: link }, ...
    ]
    """
    teams = []
    req = {
        "offset": 0,
        "limit": 50
    }
    resp = _raw_get("/teams", params=req)
    if resp is None or (resp.status_code < 200 or resp.status_code >= 300):
        logging.error(f"[tempo.teams resp] is None or status is {resp.status_code}")
        return
    data = resp.json()
    teams.extend(data['results'])
    next = _parse_next(data['metadata'])
    while next is not None:
        resp = _raw_get(next, params=req)
        if resp.status_code < 200 or resp.status_code >= 300:
            # log.error("tempo.teams", "resp is None or status is not 2xx")
            continue
        data = resp.json()
        teams.extend(data['results'])
        next = _parse_next(data['metadata'])
    return teams


def team_timesheet_approvals(team_id: int,
                             date_from: str,
                             load_worklogs: bool = True,
                             return_jira_worklogs: bool = False) -> Optional[list[dict]]:
    """
    timesheet approvals for specific team in Tempo Period

    team_id: int - id of the team
    date_from: str - date format yyyy-mm-dd
    load_worklogs: load worklogs for approvals
    return_jira_worklogs: bool - which worklog ids to load [TEMPO | JIRA]

    returns {
        period: {from: str, to: str},
        status: str,
        user: str (account_id),
        reviewer: Optional[str] (account_id),
        worklogs: [jira_id, jira_id, ...]
    }
    """
    results: list[dict] = []
    req = {
        "from": date_from
    }
    resp = _raw_get(f"/timesheet-approvals/team/{team_id}", params=req)
    if resp.status_code < 200 or resp.status_code >= 300:
        logging.error(f"tempo.team_timesheet_approvals resp is None or status is {resp.status_code}")
        headers = [f"{k}:{v}" for k, v in resp.headers.items()]
        logging.error(f"headers: {' ; '.join(headers)} body: {resp.text}")
        return
    data = resp.json()
    counter = 0
    for approval in data['results']:
        # log.status_line(counter, data['metadata']['count'], "Processing timesheet approvals")
        counter += 1
        out = {
            "period": approval['period'],
            "status": approval['status']['key'],
            "user": approval['user']['accountId'],
            "reviewer": approval['reviewer']['accountId'] if 'reviewer' in approval.keys() else None,
            "worklogs": []
        }
        if load_worklogs:
            tempo_worklog_ids = []
            worklogs = _worklogs_from_approval(approval)
            if worklogs is None:
                continue
            for worklog in worklogs:
                tempo_worklog_ids.append(worklog['tempoWorklogId'])
            if return_jira_worklogs:
                map_ttj = tempo_worklog_ids(tempo_worklog_ids)
                if map_ttj is None:
                    continue
                out['worklogs'] = list(map_ttj.values())
            else:
                out['worklogs'] = tempo_worklog_ids
        results.append(out)
    return results


def _worklogs_from_approval(approval: dict) -> Optional[list[dict]]:
    worklogs_url = str(approval['worklogs']['self'])
    parsed_url = str(worklogs_url[len(_base_url):])
    results = []
    # Load first page
    resp = _raw_get(parsed_url)
    if resp.status_code < 200 or resp.status_code >= 300:
        # log.error("tempo._worklogs_from_approval", "resp is None or status is not 2xx")
        return
    data = resp.json()
    results.extend(data['results'])
    next = _parse_next(data['metadata'])
    # Load rest of the pages if exists
    while next is not None:
        resp = _raw_get(next)
        if resp.status_code < 200 or resp.status_code >= 300:
            # log.error("tempo._worklogs_from_approval", "resp is None or status is not 2xx")
            continue
        data = resp.json()
        results.extend(data['results'])
        next = _parse_next(data['metadata'])
    return results


def worklogs_updated_from(since: str, modify_result: Callable = None) -> Optional[list[dict]]:
    """
    since: string <yyyy-MM-dd['T'HH:mm:ss]['Z']>
    """
    result = []
    req = {
        "updatedFrom": since,
        "limit": 5000
    }
    resp = _raw_get("/worklogs", params=req)
    if resp is None or (resp.status_code < 200 or resp.status_code >= 300):
        logging.error(f"[worklogs] is None or status is {resp.status_code}")
        return
    data = resp.json()
    for item in data['results']:
        modified_item = item
        if modify_result is not None:
            modified_item = modify_result(item)
        result.append(modified_item)
    next = _parse_next(data['metadata'])
    iter_c = 0
    while next is not None and iter_c < _MAX_ITER_COUNT:
        iter_c += 1
        resp = _raw_get(next, params=req)
        if resp is None or (resp.status_code < 200 or resp.status_code >= 300):
            logging.error(f"[worklogs]next[{next}] is None or status is {resp.status_code}")
            time.sleep(1)
            continue
        data = resp.json()
        for item in data['results']:
            modified_item = item
            if modify_result is not None:
                modified_item = modify_result(item)
            result.append(modified_item)
        next = _parse_next(data['metadata'])
        iter_c = 0
    if iter_c != 0:
        logging.error(f"[worklogs] failed to get all worklogs;iter_c={iter_c}")
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
