from concurrent.futures import ThreadPoolExecutor
from requests import Session
from typing import Optional
import json


_base_url = ""
_s = Session()
_pool = ThreadPoolExecutor(max_workers=5)


_JQL_SEARCH_MAX_RESULTS = 100


def init(org_name, auth_tpl):
    global _base_url
    _base_url = f"https://{org_name}.atlassian.net"
    _s.auth = auth_tpl
    _s.headers = {
        'Content-Type': "application/json",
        'X-Atlassian-Token': "no-check"
    }


def raw_get_jira(endpoint, params=None):
    if endpoint is None and len(endpoint) == 0:
        return None
    raw_response = _s.get(f"{_base_url}{endpoint}", params=params)
    return raw_response


def raw_put_jira(endpoint, data):
    if endpoint is None and len(endpoint) == 0:
        return None
    raw_response = _s.put(f"{_base_url}{endpoint}", data=json.dumps(data))
    return raw_response


def raw_post_jira(endpoint, data):
    if endpoint is None and len(endpoint) == 0:
        return None
    raw_response = _s.post(f"{_base_url}{endpoint}", data=json.dumps(data))
    return raw_response


def worklog_ids(since: int, until: Optional[int] = None) -> Optional[list[int]]:
    """
        Get worklog ids from date since,
        stop paging after response['until'] > param['until']
            or reached last page

        since: int (UNIX timestamp in milliseconds)
        until: int (UNIX timestamp in milliseconds)
    """
    result = []
    params = {
        'since': since
    }
    resp = raw_get_jira("/rest/api/3/worklog/updated", params=params)
    if resp.status_code < 200 or resp.status_code >= 300:
        return None
    data = resp.json()
    result.extend([wl['worklogId'] for wl in data['values']])
    limit_reached = until is not None and data['until'] > until
    stop = limit_reached or bool(data['lastPage'])
    while not stop:
        params = {
            'since': data['until']
        }
        resp = raw_get_jira("/rest/api/3/worklog/updated", params=params)
        if resp.status_code < 200 or resp.status_code >= 300:
            continue
        data = resp.json()
        result.extend([wl['worklogId'] for wl in data['values']])
        limit_reached = until is not None and data['until'] > until
        stop = limit_reached or bool(data['lastPage'])
    return result
