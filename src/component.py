import csv
import logging
import approvals
import worklog_author
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

        since_date = self.parse_since_to_datetime(params.since)

        # worklog authors
        if "worklog_authors" in params.datasets:
            since_mls = int(since_date.timestamp()) * 1_000
            data = worklog_author.run(since_mls)
            if data is not None and len(data) > 0:
                coldef = worklog_author.column_definitions()
                table = self.create_out_table_definition(worklog_author.FILENAME,
                                                         incremental=params.incremental,
                                                         schema=coldef
                                                         )
                with open(table.full_path, "wt", newline="", encoding="utf-8") as out_file:
                    out = csv.DictWriter(out_file, fieldnames=coldef.keys())
                    out.writerows(data)
                self.write_manifest(table)
        # Approvals for A830_04
        if "approvals_A830_04" in params.datasets:
            approvals.run(since_date)


    def parse_since_to_datetime(self, raw_since: str) -> datetime:
        parser = dp.date.DateDataParser(languages=["en"])
        date_data = parser.get_date_data(raw_since)
        if date_data is None:
            raise UserException("Invalid date 'since'")
        date_from = date_data.date_obj
        if date_from is None:
            raise UserException("datetime is empty")
        return date_from


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
