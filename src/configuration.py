from pydantic import BaseModel, ValidationError, Field
from keboola.component.exceptions import UserException


class Configuration(BaseModel):
    debug: bool = False
    incremental: bool = True
    org_name: str = Field()
    user_email: str = Field()
    tempo_token: str = Field(alias="#tempo_token")
    jira_token: str = Field(alias="#jira_token")
    since: str = Field()

    def __init__(self, **data):
        try:
            super().__init__(**data)
        except ValidationError as e:
            error_messages = [f"{err['loc'][0]}: {err['msg']}" for err in e.errors()]
            raise UserException(f"Validation Error: {', '.join(error_messages)}")
