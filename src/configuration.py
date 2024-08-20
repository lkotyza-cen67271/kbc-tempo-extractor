from pydantic import BaseModel, ValidationError  # , Field, field_validator
from keboola.component.exceptions import UserException


class Configuration(BaseModel):
    debug: bool = False
    incremental: bool = True
    org_name: str = ""
    tempo_token: str = ""
    user_email: str = ""
    jira_token: str = ""
    since: str = ""

    def __init__(self, **data):
        try:
            super().__init__(**data)
        except ValidationError as e:
            error_messages = [f"{err['loc'][0]}: {err['msg']}" for err in e.errors()]
            raise UserException(f"Validation Error: {', '.join(error_messages)}")
