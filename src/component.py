import csv
import logging

from keboola.component.dao import TableDefinition
import approvals
import team_membership
import worklog_author
import wl_attributes
import worklogs
import tempo
import jirac as jc
import dateparser as dp
from datetime import datetime
from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException

from configuration import Configuration


class Component(ComponentBase):
    """
        Extends base class for general Python components. Initializes the CommonInterface
        and performs configuration validation.

        For easier debugging the data folder is picked up by default from `../data` path,
        relative to working directory.

        If `debug` parameter is present in the `config.json`, the default logger is set to verbose DEBUG mode.
    """

    def __init__(self):
        super().__init__()

    def run(self):
        """
        Main execution code
        """
        # check for missing configuration parameters
        params = Configuration(**self.configuration.parameters)

        # initialize modules
        auth_tpl = (params.user_email, params.jira_token)
        jc.init(params.org_name, auth_tpl)
        tempo.init(params.tempo_token)

        since_date = self._parse_since_to_datetime(params.since)

        # worklog authors
        # deprecated - should not be used
        if "worklog_authors" in params.datasets:
            since_mls = int(since_date.timestamp()) * 1_000
            data = worklog_author.run(since_mls)
            if data is not None and len(data) > 0:
                coldef = worklog_author.column_definitions()
                table = self.create_out_table_definition(
                    worklog_author.FILENAME,
                    incremental=params.incremental,
                    schema=coldef
                )
                self.write_out_data(table, list(coldef.keys()), data)
            else:
                logging.warning("no worklog_author")

        # Worklogs
        worklogs_data = []
        if "worklogs" in params.datasets:
            data = worklogs.run(since_date)
            worklogs_data = data
            if data is not None and len(data) > 0:
                coldef = worklogs.column_definitions()
                table = self.create_out_table_definition(
                    worklogs.FILENAME,
                    incremental=params.incremental,
                    schema=coldef
                )
                self.write_out_data(table, list(coldef.keys()), data)
            else:
                logging.warning("no worklogs")

        # Worklog attributes
        if "worklogs" in params.datasets and "worklog_attributes" in params.datasets:
            logging.info("worklog attributes")
            data = wl_attributes.run(worklogs_data)
            coldefs = wl_attributes.column_definitions()
            # attribute data
            attributes = data[wl_attributes._TABLE_WL_ATTR]
            logging.info(f"[debug] - attrs: {attributes}")
            logging.info(f"[debug] - coldefs: {coldefs[wl_attributes._TABLE_WL_ATTR]}")
            if attributes is not None and len(attributes) > 0:
                table = self.create_out_table_definition(
                    wl_attributes.FILENAME_WL_ATTR,
                    incremental=params.incremental,
                    schema=coldefs[wl_attributes._TABLE_WL_ATTR]
                )
                self.write_out_data(
                        table=table,
                        fieldnames=list(coldefs[wl_attributes._TABLE_WL_ATTR].keys()),
                        data=attributes
                )
            else:
                logging.warning("no worklog attributes")
            # attribute configs
            configs = data[wl_attributes._TABLE_WL_ATTR_CONFIG]
            logging.info(f"[debug] - configs: {configs}")
            logging.info(f"[debug] - coldefs: {coldefs[wl_attributes._TABLE_WL_ATTR_CONFIG]}")
            if configs is not None and len(configs) > 0:
                table = self.create_out_table_definition(
                    wl_attributes.FILENAME_WL_ATTR_CONFIG,
                    incremental=params.incremental,
                    schema=coldefs[wl_attributes._TABLE_WL_ATTR_CONFIG]
                )
                self.write_out_data(
                        table=table,
                        fieldnames=list(coldefs[wl_attributes._TABLE_WL_ATTR_CONFIG].keys()),
                        data=configs
                )
            else:
                logging.warning("no attribute configs")

        # Approvals (Jira)
        if "approvals_jira" in params.datasets:
            logging.debug("approvals")
            approvals_data, appr_worklogs_data = approvals.run(since_date, approvals.LOAD_JIRA_WORKLOGS)
            coldefs = approvals.table_column_definitions()
            if approvals_data is not None and len(approvals_data) > 0:
                table = self.create_out_table_definition(
                    approvals.FILENAME_APPROVALS,
                    incremental=params.incremental,
                    schema=coldefs[approvals._TABLE_APPROVALS]
                )
                self.write_out_data(table, list(coldefs[approvals._TABLE_APPROVALS].keys()), approvals_data)
            else:
                logging.warning("no approvals")
            if appr_worklogs_data is not None and len(appr_worklogs_data) > 0:
                table = self.create_out_table_definition(
                    approvals.FILENAME_APPROVAL_WORKLOGS,
                    incremental=params.incremental,
                    schema=coldefs[approvals._TABLE_APPROVAL_WORKLOGS]
                )
                self.write_out_data(
                    table,
                    list(coldefs[approvals._TABLE_APPROVAL_WORKLOGS].keys()),
                    appr_worklogs_data
                )
            else:
                logging.warning("no appr_worklogs_data")

        # Approvals (Tempo)
        if "approvals_tempo" in params.datasets:
            logging.debug("approvals tempo")
            approvals_data, appr_worklogs_data = approvals.run(since_date, approvals.LOAD_TEMPO_WORKLOGS)
            coldefs = approvals.table_column_definitions()
            if approvals_data is not None and len(approvals_data) > 0:
                table = self.create_out_table_definition(
                    approvals.FILENAME_APPROVALS,
                    incremental=params.incremental,
                    schema=coldefs[approvals._TABLE_APPROVALS]
                )
                self.write_out_data(table, list(coldefs[approvals._TABLE_APPROVALS].keys()), approvals_data)
            else:
                logging.warning("no approvals")
            if appr_worklogs_data is not None and len(appr_worklogs_data) > 0:
                table = self.create_out_table_definition(
                    approvals.FILENAME_APPROVAL_WORKLOGS,
                    incremental=params.incremental,
                    schema=coldefs[approvals._TABLE_APPROVAL_WORKLOGS]
                )
                self.write_out_data(
                    table,
                    list(coldefs[approvals._TABLE_APPROVAL_WORKLOGS].keys()),
                    appr_worklogs_data
                )
            else:
                logging.warning("no appr_worklogs_data")

        # Teams & Membership
        if "teams" in params.datasets:
            logging.debug("teams")
            teams_data = team_membership.run()
            coldefs = team_membership.table_column_definitions()
            teams = teams_data[team_membership._TABLE_TEAMS]
            if teams is not None and len(teams) > 0:
                table = self.create_out_table_definition(
                    team_membership.FILENAME_TEAMS,
                    incremental=params.incremental,
                    schema=coldefs[team_membership._TABLE_TEAMS]
                )
                self.write_out_data(
                    table=table,
                    fieldnames=list(coldefs[team_membership._TABLE_TEAMS].keys()),
                    data=teams
                )
            else:
                logging.warning("no teams")
            membership = teams_data[team_membership._TABLE_TEAM_MEMBERSHIPS]
            if membership is not None and len(membership) > 0:
                table = self.create_out_table_definition(
                    team_membership.FILENAME_TEAM_MEMBERSHIPS,
                    incremental=params.incremental,
                    schema=coldefs[team_membership._TABLE_TEAM_MEMBERSHIPS]
                )
                self.write_out_data(
                    table=table,
                    fieldnames=list(coldefs[team_membership._TABLE_TEAM_MEMBERSHIPS].keys()),
                    data=membership
                )
            else:
                logging.warning("no team membership")

    def _parse_since_to_datetime(self, raw_since: str) -> datetime:
        parser = dp.date.DateDataParser(languages=["en"])
        date_data = parser.get_date_data(raw_since)
        if date_data is None:
            raise UserException("Invalid date 'since'")
        date_from = date_data.date_obj
        if date_from is None:
            raise UserException("datetime is empty")
        return date_from

    def write_out_data(self,
                       table: TableDefinition,
                       fieldnames: list[str],
                       data: list[dict]):
        with open(table.full_path, "wt", newline="", encoding="utf-8") as out_file:
            out = csv.DictWriter(out_file, fieldnames=fieldnames)
            out.writerows(data)
        self.write_manifest(table)


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
