from requests import Response


class TempoResponseException(Exception):
    def __init__(self, endpoint: str, resp: Response):
        assert endpoint is not None and resp is not None
        self.endpoint: str = endpoint
        self.httpcode: int = resp.status_code
        self.message: str = resp.text

    def __str__(self):
        return f"ERROR TEMPO-API {self.endpoint} [{self.httpcode}] - {self.message}"
