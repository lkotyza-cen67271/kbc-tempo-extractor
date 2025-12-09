from keboola.component.dao import logging
from requests import Session, Response
from requests.exceptions import JSONDecodeError
from exceptions import TempoResponseException
import json
from typing import Optional, Callable, Any


_base_url = "https://api.eu.tempo.io/4"
_s = Session()
_MAX_ITER_COUNT = 5


def init(token):
    _s.headers = {
        'Content-Type': "application/json",
        'Authorization': f"Bearer {token}"
    }


def tempo_to_jira_worklog_ids(tempo_worklog_ids: list[int]) -> dict[int, int]:
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
    data = _checked_post("/worklogs/tempo-to-jira?limit=500", data=req)
    for map in data['results']:
        result[map['tempoWorklogId']] = map['jiraWorklogId']
    next = _parse_next(data['metadata'])
    while next is not None:
        data = _checked_post(next, data=req)
        for map in data['results']:
            result[map['tempoWorklogId']] = map['jiraWorklogId']
        next = _parse_next(data['metadata'])
    return result


def jira_to_tempo_worklog_ids(jira_worklog_ids: list[int]) -> dict[int, int]:
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
    data = _checked_post("/worklogs/jira-to-tempo?limit=500", data=req)
    for map in data['results']:
        result[map['tempoWorklogId']] = map['jiraWorklogId']
    next = _parse_next(data['metadata'])
    while next is not None:
        data = _checked_post(next, data=req)
        for map in data['results']:
            result[map['tempoWorklogId']] = map['jiraWorklogId']
        next = _parse_next(data['metadata'])
    return result


def team_membership(team_id: int) -> list[dict]:
    """
    List of users in Tempo Team. https://apidocs.tempo.io/#tag/Team-Memberships/operation/getAllMemberships
    """
    data = _checked_get(f"/team-memberships/team/{team_id}")
    return data['results']


def teams() -> list[dict]:
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
    data = _checked_get("/teams", params=req)
    teams.extend(data['results'])
    next = _parse_next(data['metadata'])
    while next is not None:
        data = _checked_get(next, params=req)
        teams.extend(data['results'])
        next = _parse_next(data['metadata'])
    return teams


def attribute_config() -> list[dict[str, Any]]:
    """
    returns {
        attribute_key: str,
        attribute_name: str,
        attribute_type: str,
        attribute_values: str(json)
    }
    """
    def transform_data(data):
        transformed_output = []
        for item in data['results']:
            transformed_output.append({
                "attribute_key": item['key'],
                "attribute_name": item['name'],
                "attribute_type": item['type'],
                "attribute_values": json.dumps(item['values']) if 'values' in item.keys() else ""
            })
        return transformed_output
    result = []
    data = _checked_get("/work-attributes")
    result.extend(transform_data(data))
    next = _parse_next(data['metadata'])
    while next is not None:
        data = _checked_get(next)
        result.extend(transform_data(data))
        next = _parse_next(data['metadata'])
    return result


def worklog_attributes(worklogs: list) -> list[dict]:
    """
    loads attributes for specified worklogs

    worklogs: list(max length 500) - list of worklogs to load attributes for

    returns {
            tempo_worklog_id: int,
            attribute_key: str
            attribute_value: str
    }
    """
    if len(worklogs) > 500:
        logging.error("[tempo.worklog_attributes] reached limit of worklogs (500)")
        return
    req = {
        "tempoWorklogIds": worklogs
    }
    data = _checked_post("/worklogs/work-attribute-values/search", req)
    result = []
    for wl_attrs in data:
        worklog_id = wl_attrs['tempoWorklogId']
        for attribute in wl_attrs['workAttributeValues']:
            result.append({
                "tempo_worklog_id": worklog_id,
                "attribute_key": attribute['key'],
                "attribute_value": attribute['value']
            })
    return result


def team_timesheet_approvals(team_id: int,
                             date_from: str,
                             load_worklogs: bool = True,
                             worklog_source: bool = False) -> list[dict]:
    """
    timesheet approvals for specific team in Tempo Period

    team_id: int - id of the team
    date_from: str - date format yyyy-mm-dd
    load_worklogs: load worklogs for approvals
    worklog_source: bool - which worklog ids to load [TEMPO | JIRA]

    returns {
        period: {from: str, to: str},
        status: str,
        user: str (account_id),
        reviewer: Optional[str] (account_id),
        approved_by: Optional[str] (account_id),
        worklogs: [worklog_id, worklog_id, ...]
    }
    """
    results: list[dict] = []
    req = {
        "from": date_from
    }
    data = _checked_get(f"/timesheet-approvals/team/{team_id}", params=req)
    for approval in data['results']:
        approved_by = ""
        if approval['status']['key'] == "APPROVED":
            approved_by = approval['status']['actor']['accountId']
        out = {
            "period": approval['period'],
            "status": approval['status']['key'],
            "user": approval['user']['accountId'],
            "reviewer": approval['reviewer']['accountId'] if 'reviewer' in approval.keys() else None,
            "approved_by": approved_by,
            "worklogs": []
        }
        if load_worklogs:
            tempo_worklog_ids = []
            worklogs = _worklogs_from_approval(approval)
            if worklogs is None:
                continue
            for worklog in worklogs:
                tempo_worklog_ids.append(worklog['tempoWorklogId'])
            if worklog_source:  # Loads Jira
                map_ttj = tempo_to_jira_worklog_ids(tempo_worklog_ids)
                if map_ttj is None:
                    continue
                out['worklogs'] = list(map_ttj.values())
            else:  # Loads Tempo
                out['worklogs'] = tempo_worklog_ids
        results.append(out)
    return results


def _worklogs_from_approval(approval: dict) -> list[dict]:
    worklogs_url = str(approval['worklogs']['self'])
    parsed_url = str(worklogs_url[len(_base_url):])
    results = []
    # Load first page
    data = _checked_get(parsed_url)
    results.extend(data['results'])
    next = _parse_next(data['metadata'])
    # Load rest of the pages if exists
    while next is not None:
        data = _checked_get(next)
        results.extend(data['results'])
        next = _parse_next(data['metadata'])
    return results


def worklogs_updated_from(since: str, modify_result: Callable = None) -> list[dict]:
    """
    since: string <yyyy-MM-dd['T'HH:mm:ss]['Z']>
    """
    result = []
    req = {
        "updatedFrom": since,
        "limit": 5000
    }
    data = _checked_get("/worklogs", params=req)
    for item in data['results']:
        modified_item = item
        if modify_result is not None:
            modified_item = modify_result(item)
        result.append(modified_item)
    next = _parse_next(data['metadata'])
    while next is not None:
        data = _checked_get(next, params=req)
        for item in data['results']:
            modified_item = item
            if modify_result is not None:
                modified_item = modify_result(item)
            result.append(modified_item)
        next = _parse_next(data['metadata'])
    return result


def worklog_author(worklog_id: int) -> str:
    data = _checked_get(f"/worklogs/{worklog_id}")
    return data['author']['accountId']


def _parse_next(metadata: dict) -> str:
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


def _checked_get(endpoint: str, data: Optional[dict] = None) -> dict[str, Any]:
    """
    Description:
        calls the specified endpoint with GET method, then validates the response and returns it as a python-dict
    Args:
        endpoint: str - tempo endpoint to call,
                        for example in url: https://api.tempo.io/4/worklogs/tempo-to-jira
                        endpoint = /worklogs/tempo-to-jira
        data: *optional* dict - data that will be sent as request parameters
    Returns:
        Response.json()
    Raises:
        TempoResponseException - response code is not 2xx
        Exception - Response object is None or when the response content is empty string or invalid JSON
    """
    assert endpoint is not None and len(endpoint) > 0
    raw_resp = _raw_get(endpoint, data)
    if raw_resp is None:
        raise Exception(f"Response object is None - {endpoint}")
    if raw_resp.status_code < 200 or raw_resp.status_code >= 300:
        raise TempoResponseException(endpoint, raw_resp)
    data = {}
    try:
        data = raw_resp.json()
    except JSONDecodeError:
        raise Exception(f"Invalid JSON in response from TEMPO-API ({endpoint}) - response.text='{raw_resp.text}'")
    return data


def _checked_post(endpoint: str, data: Optional[dict] = None) -> dict[str, Any]:
    """
    Description:
        calls the specified endpoint with POST method, then validates the response and returns it as a python-dict
    Args:
        endpoint: str - tempo endpoint to call,
                        for example in url: https://api.tempo.io/4/worklogs/tempo-to-jira
                        endpoint = /worklogs/tempo-to-jira
        data: *optional* dict - data that will be sent in request body as JSON
    Returns:
        Response.json()
    Raises:
        TempoResponseException - response code is not 2xx
        Exception - Response object is None or when the response content is empty string or invalid JSON
    """
    assert endpoint is not None and len(endpoint) > 0
    raw_resp = _raw_post(endpoint, data)
    if raw_resp is None:
        raise Exception(f"Response object is None - {endpoint}")
    if raw_resp.status_code < 200 or raw_resp.status_code >= 300:
        raise TempoResponseException(endpoint, raw_resp)
    data = {}
    try:
        data = raw_resp.json()
    except JSONDecodeError:
        raise Exception(f"Invalid JSON in response from TEMPO-API ({endpoint}) - response.text='{raw_resp.text}'")
    return data
