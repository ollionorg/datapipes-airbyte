import logging
from enum import Enum
from functools import wraps
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
            else:
                raise error

    return wrapper


def token_manager(func):
    """Decorator to manage tokens for API access."""

    @wraps(func)
    def wrapper(self, *args, **kwargs):
        try:
            # Check if access token exists
            if not self.access_token:
                # Generate a new token if it doesn't exist
                self.get_token()
                self.logger.info("Generated a new access token.")

            # Call the original function with updated token
            return func(self, *args, **kwargs)
        except HTTPError as error:
            if error.response.status_code == codes.UNAUTHORIZED:
                self.logger.info("Unauthorized access token caught.")
                self.get_token()
                self.logger.info("Generated a new access token.")
                return func(self, *args, **kwargs)

    return wrapper


class BrightspaceClient:
    logger = logging.getLogger("airbyte")
    version = "1.43"

    def get_token(self):
        payload = {
            'client_auth_type': "CREDENTIALS_IN_BODY",
            'token_url': self._refresh_endpoint,
            'ingestor_source_update_url': self.ingestor_source_update_url,
        }
        try:
            resp = self._make_request("POST", self.airflow_get_token, body=payload)
        except Exception as err:
            self.logger.error("Get token failed: %s", err)
            raise err
        auth = resp.json()
        self.access_token = auth["access_token"]

    def __init__(
            self,
            instance_url: str,
            airflow_get_token: str,
            ingestor_source_update_url: str,
            refresh_token: str = None,
            token: str = None,
            client_id: str = None,
            client_secret: str = None,
            access_token: str = None,
            **kwargs: Any,
    ) -> None:
        self._refresh_endpoint = "https://auth.brightspace.com/core/connect/token"
        self.refresh_token = refresh_token
        self.token = token
        self.airflow_get_token = airflow_get_token
        self.ingestor_source_update_url = ingestor_source_update_url
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
                resp = self.session.post(url, headers=headers, json=body)
            resp.raise_for_status()
        except HTTPError as err:
            self.logger.warning(f"http error body: {err.response.text}")
            raise
        return resp

    @token_manager
    @handle_http_errors
    def get_list_of_data_set(self) -> List[BSDataSet]:
        url = f"{self.url_base}/dataExport/list"
        headers = {"Authorization": "Bearer {}".format(self.access_token)}
        response = self._make_request(http_method="GET", url=url, headers=headers)
        data = response.json()
        datasets = [BSDataSet(**dataset_data) for dataset_data in data]
        return datasets

    @token_manager
    @handle_http_errors
    def create_export_job(self, payload: dict) -> BSExportJob:
        url = f"{self.url_base}/dataExport/create"
        headers = {"Authorization": "Bearer {}".format(self.access_token), "Content-Type": "application/json"}
        response = self._make_request(http_method="POST", url=url, headers=headers, body=payload)
        return BSExportJob(**response.json())

    @token_manager
    @handle_http_errors
    def get_export_job_details(self, export_job_id: str) -> BSExportJob:
        url = f"{self.url_base}/dataExport/jobs/{export_job_id}"
        headers = {"Authorization": "Bearer {}".format(self.access_token)}
        response = self._make_request(http_method="GET", url=url, headers=headers)
        return BSExportJob(**response.json())

    @token_manager
    @handle_http_errors
    def download_export_job(self, export_job_id: str):
        url = f"{self.url_base}/dataExport/download/{export_job_id}"
        headers = {"Authorization": "Bearer {}".format(self.access_token)}
        response = self._make_request(http_method="GET", url=url, headers=headers)
        return response.content
