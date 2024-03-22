import json
import logging
from enum import Enum
from functools import lru_cache, wraps
from typing import Any, List

import requests
from pydantic import BaseModel, Field
from requests import adapters as request_adapters
from requests import codes
from requests.exceptions import HTTPError


class BrightspaceDatasetType(Enum):
    ADVANCED = "AdvancedDataSets"
    BRIGHTSPACE = "BrightspaceDataset"


class BSDataSet(BaseModel):
    data_set_id: str = Field(alias="DataSetId")
    name: str = Field(alias="Name")
    category: str = Field(alias="Category")


class ExportJobStatus(Enum):
    Queued = 0
    Processing = 1
    Complete = 2
    Error = 3
    Deleted = 4


class BSExportJob(BaseModel):
    export_job_id: str = Field(alias="ExportJobId")
    data_set_id: str = Field(alias="DataSetId")
    name: str = Field(alias="Name")
    status: ExportJobStatus = Field(alias="Status")


def handle_http_errors(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except HTTPError as error:
            if error.response.status_code in [codes.TOO_MANY_REQUESTS]:
                message = "API call-rate limit exceeded."
                args[0].logger.error(message)
            if error.response.status_code == codes.UNAUTHORIZED:
                # message = error.response.me"API call-rate limit"
                # args[0].logger.error(message)
                raise error
            else:
                raise error
        return None

    return wrapper


class BrightspaceClient:
    logger = logging.getLogger("airbyte")
    version = "1.43"

    def __init__(
            self,
            instance_url: str,
            refresh_token: str = None,
            token: str = None,
            client_id: str = None,
            client_secret: str = None,
            access_token: str = None,
            **kwargs: Any,
    ) -> None:
        self.refresh_token = refresh_token
        self.token = token
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token = access_token
        self.instance_url = instance_url
        self.session = requests.Session()
        # Change the connection pool size. Default value is not enough for parallel tasks
        adapter = request_adapters.HTTPAdapter()
        self.session.mount("https://", adapter)

    @property
    def url_base(self) -> str:
        return f"{self.instance_url}/d2l/api/lp/{self.version}"

    def _make_request(
            self, http_method: str, url: str, headers: dict = None, body: dict = None, stream: bool = False, params: dict = None
    ) -> requests.models.Response:
        self.logger.debug(f"Making {http_method} request to URL: {url}")
        self.logger.debug(f"Headers: {headers}")
        self.logger.debug(f"Body: {body}")
        self.logger.debug(f"Stream: {stream}")
        self.logger.debug(f"Params: {params}")
        try:
            if http_method == "GET":
                resp = self.session.get(url, headers=headers, stream=stream, params=params)
            elif http_method == "POST":
                resp = self.session.post(url, headers=headers, data=json.dumps(body))
            resp.raise_for_status()
        except HTTPError as err:
            self.logger.warning(f"http error body: {err.response.text}")
            raise
        return resp

    def login(self):
        login_url = f"https://auth.brightspace.com/core/connect/token"
        login_body = {
            "grant_type": "refresh_token",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "refresh_token": self.refresh_token,
        }
        try:
            resp = self._make_request("POST", login_url, body=login_body, headers={"Content-Type": "application/x-www-form-urlencoded"})
        except HTTPError as err:
            # if err.response.status_code == requests.codes.BAD_REQUEST:
                # if error_message := AUTHENTICATION_ERROR_MESSAGE_MAPPING.get(err.response.json().get("error_description")):
                # raise AirbyteTracedException(message=err.response.json().get("error_description"), failure_type=FailureType.config_error)
            raise err
        auth = resp.json()
        self.access_token = auth["access_token"]
        self.instance_url = auth["instance_url"]

    @lru_cache(maxsize=None)
    @handle_http_errors
    def get_list_of_data_set(self) -> List[BSDataSet]:
        url = f"{self.url_base}/dataExport/list"
        headers = {"Authorization": "Bearer {}".format(self.access_token)}
        response = self._make_request(http_method="GET", url=url, headers=headers)
        data = response.json()
        datasets = [BSDataSet(**dataset_data) for dataset_data in data]
        return datasets

    @handle_http_errors
    def create_export_job(self, payload: dict) -> BSExportJob:
        url = f"{self.url_base}/dataExport/create"
        headers = {"Authorization": "Bearer {}".format(self.access_token), "Content-Type": "application/json"}
        response = self._make_request(http_method="POST", url=url, headers=headers, body=payload)
        return BSExportJob(**response.json())

    @handle_http_errors
    def get_export_job_details(self, export_job_id: str) -> BSExportJob:
        url = f"{self.url_base}/dataExport/jobs/{export_job_id}"
        headers = {"Authorization": "Bearer {}".format(self.access_token)}
        response = self._make_request(http_method="GET", url=url, headers=headers)
        return BSExportJob(**response.json())

    @handle_http_errors
    def download_export_job(self, export_job_id: str):
        url = f"{self.url_base}/dataExport/download/{export_job_id}"
        headers = {"Authorization": "Bearer {}".format(self.access_token)}
        response = self._make_request(http_method="GET", url=url, headers=headers)
        return response.content
