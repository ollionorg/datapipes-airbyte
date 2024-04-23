import logging
import time
import traceback
from datetime import datetime
from enum import Enum
from functools import wraps
from typing import Any, List, Optional

import requests
from airbyte_protocol.models import AirbyteMessage, Type, AirbyteTraceMessage, TraceType, AirbyteErrorTraceMessage
from pydantic import BaseModel, Field, validator
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


class BrightspaceDataSetPluginInfo(BaseModel):
    plugin_id: str = Field(alias="PluginId")
    name: str = Field(alias="Name")
    differential: str = Field(alias="Description")
    extracts_link: str = Field(alias="ExtractsLink")


class BrightspaceDataSetInfo(BaseModel):
    schema_id: str = Field(alias="SchemaId")
    full: Optional[BrightspaceDataSetPluginInfo] = Field(alias="Full", default=None)
    differential: Optional[BrightspaceDataSetPluginInfo] = Field(alias="Differential", default=None)
    extracts_link: str = Field(alias="ExtractsLink")


class BdsType(str, Enum):
    Full = "Full"
    Differential = "Differential"


class BrightspaceDataSetExtractInfo(BaseModel):
    # ref: https://docs.valence.desire2learn.com/res/dataExport.html#BrightspaceDataSets.BrightspaceDataSetExtractInfo
    schema_id: str = Field(alias="SchemaId")
    plugin_id: str = Field(alias="PluginId")
    bds_type: BdsType = Field(alias="BdsType")
    created_date: datetime = Field(alias="CreatedDate")
    download_link: Optional[str] = Field(alias="DownloadLink", default=None)

    # Validator to parse date string to datetime object
    @validator('created_date', pre=True)
    def parse_created_date(cls, value):
        return datetime.fromisoformat(value.replace('Z', '+00:00'))

    @property
    def to_state(self) -> str:
        return self.created_date.isoformat()


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

    def get_token(self, max_retries=3, retry_delay=10):
        error = None
        for attempt in range(max_retries):
            try:
                payload = {
                    'client_auth_type': "CREDENTIALS_IN_BODY",
                    'token_url': self._refresh_endpoint,
                    'ingestor_source_update_url': self.ingestor_source_update_url,
                }
                resp = self._make_request("POST", self.airflow_get_token, body=payload)
                auth = resp.json()
                self.access_token = auth["access_token"]
                return  # Token successfully retrieved, exit the loop
            except HTTPError as err:
                error = err
                self.logger.info("Get token failed (Attempt %d): %s", attempt + 1, err.response.text)
                if attempt < max_retries - 1:
                    self.logger.info("Retrying in %d seconds...", retry_delay)
                    time.sleep(retry_delay)
                if attempt == max_retries - 1:
                    self.logger.error("Get token failed with error %s", err.response.text)
                    error_msg = f"This could be due to an invalid configuration. Please contact Support for assistance. Error: {err.response.text}"
                    error_log_msg = AirbyteMessage(
                        type=Type.TRACE,
                        trace=AirbyteTraceMessage(
                            type=TraceType.ERROR,
                            emitted_at=int(datetime.now().timestamp() * 1000),
                            error=AirbyteErrorTraceMessage(
                                internal_message=f"{err.response.text}",
                                message=error_msg,
                                stack_trace=traceback.format_exc(),
                            ),
                        ),
                    ).json(exclude_none=True)
                    print(error_log_msg)

        # If all attempts failed, raise the last exception
        raise error

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
    def get_list_of_ads_data_set(self) -> List[BSDataSet]:
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

    @token_manager
    @handle_http_errors
    def get_list_of_bds_data_set(self) -> List[BrightspaceDataSetInfo]:
        url = f"{self.url_base}/datasets/bds"
        headers = {"Authorization": f"Bearer {self.access_token}"}
        datasets = []

        while url:
            response = self._make_request(http_method="GET", url=url, headers=headers)
            if response.status_code != 200:
                raise Exception("Failed to fetch data: " + response.text)

            data = response.json()
            datasets.extend([BrightspaceDataSetInfo(**obj) for obj in data.get("Objects", [])])

            url = data.get("Next")  # Continue to next page if available

        return datasets

    @token_manager
    @handle_http_errors
    def get_bds_extracts(self, schema_id: str, plugin_id: str) -> List[BrightspaceDataSetExtractInfo]:
        url = f"{self.url_base}/datasets/bds/{schema_id}/plugins/{plugin_id}/extracts"
        headers = {"Authorization": f"Bearer {self.access_token}"}
        datasets = []

        while url:
            response = self._make_request(http_method="GET", url=url, headers=headers)
            if response.status_code != 200:
                raise Exception("Failed to fetch data: " + response.text)

            data = response.json()
            datasets.extend([BrightspaceDataSetExtractInfo(**obj) for obj in data.get("Objects", [])])

            url = data.get("Next")  # Continue to next page if available

        return datasets

    @token_manager
    @handle_http_errors
    def download_bds_extracts(self, download_link: str):
        headers = {"Authorization": "Bearer {}".format(self.access_token)}
        response = self._make_request(http_method="GET", url=download_link, headers=headers)
        return response.content
