import json
import logging
from functools import lru_cache, wraps
from typing import Any, List

from airbyte_cdk.utils import AirbyteTracedException
from airbyte_protocol.models import FailureType
from pydantic import BaseModel, Field
from requests import adapters as request_adapters
from requests.exceptions import HTTPError, RequestException
from requests import codes
from enum import Enum

import requests


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
        return None

    return wrapper


class BrightspaceClient:
    logger = logging.getLogger("airbyte")
    version = "1.43"
    parallel_tasks_size = 100

    def __init__(
            self,
            instance_url: str,
            refresh_token: str = None,
            token: str = None,
            client_id: str = None,
            client_secret: str = None,
            **kwargs: Any,
    ) -> None:
        self.refresh_token = refresh_token
        self.token = token
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token = None
        self.instance_url = instance_url
        self.session = requests.Session()
        # Change the connection pool size. Default value is not enough for parallel tasks
        adapter = request_adapters.HTTPAdapter(pool_connections=self.parallel_tasks_size, pool_maxsize=self.parallel_tasks_size)
        self.session.mount("https://", adapter)
        # remove
        self.access_token = "eyJhbGciOiJSUzI1NiIsImtpZCI6IjJkMjEwZjk1LTY4NmItNGFjYy04OWE3LWVlMGE2NDUzYmU1NyIsInR5cCI6IkpXVCJ9.eyJuYmYiOjE3MTA0MjA0MzQsImV4cCI6MTcxMDQyNDAzNCwiaXNzIjoiaHR0cHM6Ly9hcGkuYnJpZ2h0c3BhY2UuY29tL2F1dGgiLCJhdWQiOiJodHRwczovL2FwaS5icmlnaHRzcGFjZS5jb20vYXV0aC90b2tlbiIsInN1YiI6IjY1NDY1IiwidGVuYW50aWQiOiIyMjk2YmQ5Mi1jMWUzLTQ5MzQtOTg0NC03MjkxMTcyNjFiMDIiLCJhenAiOiI3Zjk3NWI1ZC1jZDllLTQ1ODctYjg3OC1lMDMwMzNkYmUzNTUiLCJzY29wZSI6ImRhdGFodWI6ZGF0YWV4cG9ydHM6ZG93bmxvYWQscmVhZCBkYXRhc2V0czpiZHM6bGlzdCxyZWFkIHJlcG9ydGluZzpkYXRhc2V0OmZldGNoLGxpc3QgcmVwb3J0aW5nOmpvYjpjcmVhdGUsZG93bmxvYWQsZmV0Y2gsbGlzdCIsImp0aSI6IjQ4MGQ3MDE5LWM1YmUtNDMwOC05YzlmLTlhZDY2Yzg4MDNlYSJ9.SC2EYtJH1t8nb4SctLVA9KvdJhGf8bOcP8_sBuFVJedpS1-55984t03DsxgVX3eSsWvxUjUJBp2Vs0tCXJWUgg3zwpvkHWBvXdzQLDu5da7ie8O0KmwG57WKlYxIbk__eMuyXIYh1lUWXCRiPG7AkVOMWkvuCuEbR-oanXZmACU6LBUiGpSvliueo6_N1a9diASC7JHLuMwV8oj0N1JTgrzQjvOqpiVeE1h58tW_KFa59EvlhWWwL3aIZDw_DR9E02EUcI7--O5mWwTLXgiFvt_-nU6ECrNzgywACtu_ZLmGziFr2lGb_Zekp3X4RkkyJcADK1LvErANdAvA3BlBlQ"

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
