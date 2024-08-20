from concurrent.futures import ThreadPoolExecutor
from requests import Session
from typing import Optional
import logger as log
import concutils
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


def jql_action_v2(jql_string: str, action: callable,
                  stop_after: Optional[int] = None,
                  modify_own_issues: bool = True,
                  _start: int = 0,
                  _break: bool = False,
                  _original_total: int = -1
                  ) -> None:
    """
        Search issues and perform 'action' on each of the issues.

        jql_string: jql select for issues
        action: function which is called on each issue
        stop_after: stop after x issues (rounded to maxResults) for testing
        modify_own_issues: function affects jql select
    """
    original_total = _original_total
    if _break or (stop_after is not None and _start >= stop_after):
        return
    search_data = {
        'jql': jql_string,
        'startAt': _start,
        'maxResults': _JQL_SEARCH_MAX_RESULTS
    }
    # request issues and validate response
    search_resp = raw_post_jira("/rest/api/latest/search", data=search_data)
    if search_resp.status_code < 200 or search_resp.status_code >= 300:
        log.error("jql_action_v2", f"search status_code {search_resp.status_code}")
        log.error("jql_action_v2", f"{search_data} - {search_resp.text}")
        return
    search_result = search_resp.json()
    if 'errorMessages' in search_result.keys():
        log.error("jql_action_v2", search_result)
        return
    if search_result['maxResults'] != _JQL_SEARCH_MAX_RESULTS:
        log.error("jql_action_v2", "maxResults did not match with response")
        log.error("jql_action_v2", f"maxResults={search_result['maxResults']}")
        return
    # only on first run
    if _start == 0 and _original_total < 0:
        original_total = search_result['total']
        log.info(jql_string)
    # set values based on 'modify_own_issues'
    progress = _start
    next_start = _start + search_result['maxResults']
    if modify_own_issues:
        progress = original_total - search_result['total']
        next_start = 0
    # run 'action' on issues
    for ix, issue in enumerate(list(search_result['issues'])):
        action(issue)
        log.status_line(progress + ix, original_total, issue['key'])
    # check and-if iterate
    is_last = (search_result['total'] - search_result['maxResults']) <= 0 or next_start > search_result['total']
    jql_action_v2(jql_string, action,
                  stop_after=stop_after, modify_own_issues=modify_own_issues,
                  _start=next_start, _break=is_last,
                  _original_total=original_total)


def _search(start, max_results, jql):
        req = {
            'startAt': start,
            'maxResults': max_results,
            'jql': jql
        }
        jql_response = raw_post_jira("/rest/api/latest/search", data=req)
        if jql_response.status_code < 200 or jql_response.status_code >= 300:
            log.error("jql_search_lin", "status code is not in 2XX")
            return []
        jql = jql_response.json()
        if 'errorMessages' in jql.keys():
            log.error("jql_action_v2", jql)
            return []
        return jql['issues']


def jql_search_lin(jql_string: str):
    result = []
    total = jql_count(jql_string)
    cache = []
    for i in range(0, total+_JQL_SEARCH_MAX_RESULTS, _JQL_SEARCH_MAX_RESULTS):
        cache.append(_pool.submit(_search, i, _JQL_SEARCH_MAX_RESULTS, jql_string))
        for ix, request in enumerate(cache):
            if request.done():
                result.extend(request.result())
                cache.pop(ix)
        log.status_line(i, total, f"loading results: {jql_string}")
        concutils.wait(0.3)
    log.info("(jql_search_lin) data loaded but still processing")
    while len(cache) != 0:
        log.rline(f"processing standing requests {len(cache)}")
        for i, request in enumerate(cache):
            if request.done():
                result.extend(request.result())
                cache.pop(i)
    log.info("(jql_search_lin) done")
    return result


def jql_count(jql_string: str) -> int:
    """
        send dummy jql search and extracts total

        jql_string: jql select
    """
    req = {
        'startAt': 0,
        'maxResults': 1,
        'jql': str(jql_string)
    }
    jql = raw_post_jira("/rest/api/latest/search", data=req).json()
    if 'errorMessages' in jql.keys():
        log.error("jql_count", f"{req}-{jql}")
        return -1
    return jql['total']


def issue(issue_key: str,
          expands_list: Optional[list[str]] = None
          ) -> Optional[dict]:
    params = ""
    if expands_list is not None:
        params = f"?{'&'.join(['expand='+expand for expand in expands_list])}"
    res = raw_get_jira(f"/rest/api/latest/issue/{issue_key}{params}")
    if res.status_code < 200 or res.status_code >= 300:
        log.error("issue", f"status is not 2XX {res.status_code} {res.text}")
        return None
    json_issue = res.json()
    if 'errorMessages' in json_issue.keys():
        log.error("issue", json_issue)
        return None
    return dict(json_issue)


def user_by_id(account_id: str) -> Optional[dict]:
    user_resp = raw_get_jira("/rest/api/3/user",
                             params={'accountId': account_id})
    if user_resp.status_code < 200 or user_resp.status_code >= 300:
        log.error("_user", f"status is not 2XX {user_resp.status_code} {user_resp.text}")
        return None
    user = user_resp.json()
    if 'errorMessages' in user.keys():
        log.error("user_by_id", user)
        return None
    return user


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
        log.error("jira_worklogs", f"status is not 2XX {resp.status_code} {resp.text}")
        return None
    data = resp.json()
    result.extend([wl['worklogId'] for wl in data['values']])
    limit_reached = until is not None and data['until'] > until
    stop = limit_reached or bool(data['lastPage'])
    while not stop:
        log.rline(f"loading worklogs {data['until']}")
        params = {
            'since': data['until']
        }
        resp = raw_get_jira("/rest/api/3/worklog/updated", params=params)
        if resp.status_code < 200 or resp.status_code >= 300:
            log.error("jira_worklogs", f"[rec] status is not 2XX {resp.status_code} {resp.text}")
            continue
        data = resp.json()
        result.extend([wl['worklogId'] for wl in data['values']])
        limit_reached = until is not None and data['until'] > until
        stop = limit_reached or bool(data['lastPage'])
    return result
