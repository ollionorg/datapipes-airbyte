import io
import math
import time
import zipfile
from abc import ABC, abstractmethod
from datetime import datetime
from functools import lru_cache
from typing import Mapping, Optional, Any, Iterable, Union, List, Tuple, MutableMapping

import pandas as pd
import pendulum
from airbyte_cdk.sources.streams import Stream
from airbyte_protocol.models import SyncMode
from numpy import nan
from pandas.api.types import is_datetime64_any_dtype as is_datetime
from pandas.api.types import is_timedelta64_dtype as is_timedelta
from pendulum import DateTime

from source_brightspace.api import BrightspaceClient, BSExportJob, ExportJobStatus, BrightspaceDataSetInfo, BdsType


class BrightspaceStream(Stream, ABC):

    @property
    def availability_strategy(self) -> Optional["AvailabilityStrategy"]:
        return None

    def read_with_chunks(self, response_content, chunk_size: int = 100) -> Iterable[Tuple[int, Mapping[str, Any]]]:
        try:
            with io.BytesIO(response_content) as response_stream:
                with zipfile.ZipFile(response_stream) as zipped_data_set:
                    files = zipped_data_set.namelist()
                    csv_name = files[0]

                    with zipped_data_set.open(csv_name) as csv_file:
                        chunks = pd.read_csv(csv_file, chunksize=chunk_size, iterator=True, dialect="unix", dtype=object)
                        for chunk in chunks:
                            chunk = chunk.replace({nan: None}).to_dict(orient="records")
                            for row in chunk:
                                # print(row)
                                yield row
        except pd.errors.EmptyDataError as e:
            self.logger.info(f"Empty data received. {e}")
            yield from []


class ADSStream(BrightspaceStream, ABC):
    DEFAULT_WAIT_TIMEOUT_SECONDS = 60 * 60 * 12  # 12-hour job running time
    MAX_CHECK_INTERVAL_SECONDS = 2.0
    MAX_RETRY_NUMBER = 3  # maximum number of retries for creating successful jobs

    def __init__(
            self, bs_api: BrightspaceClient, start_date: str, end_date: Optional[str] = None, **kwargs
    ):
        super().__init__(**kwargs)
        self.bs_api = bs_api
        self.start_date = start_date
        self.end_date = end_date

    @property
    def cursor_field(self) -> str:
        return "last_modified"

    @abstractmethod
    def create_export_job(self) -> BSExportJob:
        pass

    def read_records(
            self,
            sync_mode: SyncMode,
            cursor_field: List[str] = None,
            stream_slice: Mapping[str, Any] = None,
            stream_state: Mapping[str, Any] = None,
    ) -> Iterable[Mapping[str, Any]]:
        if sync_mode == SyncMode.incremental:
            self._incremental_read(stream_state)
        self.end_date = self.end_date or pendulum.now(tz="UTC").to_iso8601_string()
        export_job, job_status = self.execute_job()
        zip_file_stream = self.bs_api.download_export_job(export_job_id=export_job.export_job_id)
        for record in self.read_with_chunks(zip_file_stream):
            record["stream_last_sync_time"] = self.end_date
            yield record

    def execute_job(self) -> Tuple[Optional[BSExportJob], Optional[ExportJobStatus]]:
        job_status = ExportJobStatus.Error
        export_job = None
        for i in range(0, self.MAX_RETRY_NUMBER):
            export_job = self.create_export_job()
            if not export_job:
                return None, job_status
            job_status = self.wait_for_job(export_job.export_job_id)
            if job_status in [ExportJobStatus.Complete, ExportJobStatus.Error]:
                break

        if job_status in [ExportJobStatus.Error, ExportJobStatus.Deleted]:
            return None, job_status
        return export_job, job_status

    def wait_for_job(self, export_job_id: str) -> ExportJobStatus:
        expiration_time: DateTime = pendulum.now().add(seconds=self.DEFAULT_WAIT_TIMEOUT_SECONDS)
        job_status = ExportJobStatus.Processing
        delay_timeout = 0.0
        delay_cnt = 0
        job_info = None
        # minimal starting delay is 0.5 seconds.
        # this value was received empirically
        time.sleep(0.5)
        while pendulum.now() < expiration_time:
            job_info = self.bs_api.get_export_job_details(export_job_id)
            job_status = job_info.status
            if job_status in [ExportJobStatus.Complete, ExportJobStatus.Error, ExportJobStatus.Deleted]:
                if job_status != ExportJobStatus.Complete:
                    self.logger.error(f"JobStatus: {self.name}/{job_status}'")

                return job_status

            if delay_timeout < self.MAX_CHECK_INTERVAL_SECONDS:
                delay_timeout = 0.5 + math.exp(delay_cnt) / 1000.0
                delay_cnt += 1

            time.sleep(delay_timeout)
            job_id = job_info.export_job_id
            self.logger.info(
                f"Sleeping {delay_timeout} seconds while waiting for Job: {self.name}/{job_id} to complete. Current state: {job_status}"
            )

        self.logger.warning(f"Not wait the {self.name} data for {self.DEFAULT_WAIT_TIMEOUT_SECONDS} seconds, data: {job_info}!!")
        return job_status

    def get_updated_state(
            self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]
    ) -> MutableMapping[str, Any]:
        return {"last_modified": latest_record["stream_last_sync_time"]}

    def _incremental_read(self, stream_state: Mapping[str, Any] = None):
        last_modified = stream_state.get('last_modified') if stream_state else None
        if not last_modified:
            self.logger.info('No last_modified field found for stream. Running full read')
            return
        self.start_date = last_modified


class FinalGradesStream(ADSStream, ABC):

    @property
    def primary_key(self) -> Optional[Union[str, List[str], List[List[str]]]]:
        return "User Id"

    @property
    def name(self) -> str:
        return "Final Grades"

    def create_export_job(self) -> BSExportJob:
        data_sets = self.bs_api.get_list_of_ads_data_set()
        data_set = next(filter(lambda x: x.name == self.name, data_sets), None)
        payload = {
            "DataSetId": data_set.data_set_id,
            "Filters": [
                {
                    "name": "startDate",
                    "value": self.start_date
                },
                {
                    "name": "endDate",
                    "value": self.end_date
                }
            ]
        }
        return self.bs_api.create_export_job(payload=payload)


class EnrollmentsAndWithdrawalsStream(ADSStream, ABC):
    def __init__(
            self, org_unit_id: str, **kwargs
    ):
        super().__init__(**kwargs)
        self.org_unit_id = org_unit_id

    @property
    def primary_key(self) -> Optional[Union[str, List[str], List[List[str]]]]:
        return ""

    @property
    def name(self) -> str:
        # don't change typo here because D2L Brightspace has misspelled this
        return "Enrolments and Withdrawals"

    def create_export_job(self) -> BSExportJob:
        data_sets = self.bs_api.get_list_of_ads_data_set()
        data_set = next(filter(lambda x: x.name == self.name, data_sets), None)
        payload = {
            "DataSetId": data_set.data_set_id,
            "Filters": [
                {
                    "name": "parentOrgUnitId",
                    "value": self.org_unit_id
                },
                {
                    "name": "startDate",
                    "value": self.start_date
                },
                {
                    "name": "endDate",
                    "value": self.end_date
                }
            ]
        }
        return self.bs_api.create_export_job(payload=payload)


class AllGradesStream(ADSStream, ABC):
    def __init__(
            self, org_unit_id: str, **kwargs
    ):
        super().__init__(**kwargs)
        self.org_unit_id = org_unit_id

    @property
    def primary_key(self) -> Optional[Union[str, List[str], List[List[str]]]]:
        return ""

    @property
    def name(self) -> str:
        return "All Grades"

    def create_export_job(self) -> BSExportJob:
        data_sets = self.bs_api.get_list_of_ads_data_set()
        data_set = next(filter(lambda x: x.name == self.name, data_sets), None)
        payload = {
            "DataSetId": data_set.data_set_id,
            "Filters": [
                {
                    "name": "parentOrgUnitId",
                    "value": self.org_unit_id
                },
                {
                    "name": "startDate",
                    "value": self.start_date
                },
                {
                    "name": "endDate",
                    "value": self.end_date
                }
            ]
        }
        return self.bs_api.create_export_job(payload=payload)


class LearnerUsageStream(ADSStream, ABC):
    def __init__(
            self, org_unit_id: str, roles: str, **kwargs
    ):
        super().__init__(**kwargs)
        self.org_unit_id = org_unit_id
        self.roles = roles

    @property
    def primary_key(self) -> Optional[Union[str, List[str], List[List[str]]]]:
        return ""

    @property
    def name(self) -> str:
        return "Learner Usage"

    def create_export_job(self) -> BSExportJob:
        data_sets = self.bs_api.get_list_of_ads_data_set()
        data_set = next(filter(lambda x: x.name == self.name, data_sets), None)
        payload = {
            "DataSetId": data_set.data_set_id,
            "Filters": [
                {
                    "name": "parentOrgUnitId",
                    "value": self.org_unit_id
                },
                {
                    "name": "startDate",
                    "value": self.start_date
                },
                {
                    "name": "endDate",
                    "value": self.end_date
                },
                {
                    "name": "roles",
                    "value": ",".join(map(str, self.roles))
                }
            ]
        }
        return self.bs_api.create_export_job(payload=payload)


class CLOEStream(ADSStream, ABC):
    def __init__(
            self, org_unit_id: str, roles: str, **kwargs
    ):
        super().__init__(start_date="", **kwargs)
        self.org_unit_id = org_unit_id
        self.roles = roles

    @property
    def primary_key(self) -> Optional[Union[str, List[str], List[List[str]]]]:
        return ""

    @property
    def name(self) -> str:
        return "CLOE"

    def create_export_job(self) -> BSExportJob:
        data_sets = self.bs_api.get_list_of_ads_data_set()
        data_set = next(filter(lambda x: x.name == self.name, data_sets), None)
        payload = {
            "DataSetId": data_set.data_set_id,
            "Filters": [
                {
                    "name": "parentOrgUnitId",
                    "value": self.org_unit_id
                },
                {
                    "name": "roles",
                    "value": ",".join(map(str, self.roles))
                }
            ]
        }
        return self.bs_api.create_export_job(payload=payload)

    def read_records(
            self,
            sync_mode: SyncMode,
            cursor_field: List[str] = None,
            stream_slice: Mapping[str, Any] = None,
            stream_state: Mapping[str, Any] = None,
    ) -> Iterable[Mapping[str, Any]]:
        export_job, job_status = self.execute_job()
        zip_file_stream = self.bs_api.download_export_job(export_job_id=export_job.export_job_id)
        for record in self.read_with_chunks(zip_file_stream):
            record["stream_last_sync_time"] = self.end_date
            yield record


class InstructorUsageStream(ADSStream, ABC):
    def __init__(
            self, org_unit_id: str, roles: str, **kwargs
    ):
        super().__init__(**kwargs)
        self.org_unit_id = org_unit_id
        self.roles = roles

    @property
    def primary_key(self) -> Optional[Union[str, List[str], List[List[str]]]]:
        return ""

    @property
    def name(self) -> str:
        return "Instructor Usage"

    def create_export_job(self) -> BSExportJob:
        data_sets = self.bs_api.get_list_of_ads_data_set()
        data_set = next(filter(lambda x: x.name == self.name, data_sets), None)
        payload = {
            "DataSetId": data_set.data_set_id,
            "Filters": [
                {
                    "name": "parentOrgUnitId",
                    "value": self.org_unit_id
                },
                {
                    "name": "startDate",
                    "value": self.start_date
                },
                {
                    "name": "endDate",
                    "value": self.end_date
                },
                {
                    "name": "roles",
                    "value": ",".join(map(str, self.roles))
                }
            ]
        }
        return self.bs_api.create_export_job(payload=payload)


class AwardsIssuedStream(ADSStream, ABC):
    def __init__(
            self, org_unit_id: str, **kwargs
    ):
        super().__init__(**kwargs)
        self.org_unit_id = org_unit_id

    @property
    def primary_key(self) -> Optional[Union[str, List[str], List[List[str]]]]:
        return ""

    @property
    def name(self) -> str:
        return "Awards Issued"

    def create_export_job(self) -> BSExportJob:
        data_sets = self.bs_api.get_list_of_ads_data_set()
        data_set = next(filter(lambda x: x.name == self.name, data_sets), None)
        payload = {
            "DataSetId": data_set.data_set_id,
            "Filters": [
                {
                    "name": "parentOrgUnitId",
                    "value": self.org_unit_id
                },
                {
                    "name": "startDate",
                    "value": self.start_date
                },
                {
                    "name": "endDate",
                    "value": self.end_date
                }
            ]
        }
        return self.bs_api.create_export_job(payload=payload)


class RubricAssessmentsStream(ADSStream, ABC):
    def __init__(
            self, org_unit_id: str, **kwargs
    ):
        super().__init__(**kwargs)
        self.org_unit_id = org_unit_id

    @property
    def primary_key(self) -> Optional[Union[str, List[str], List[List[str]]]]:
        return ""

    @property
    def name(self) -> str:
        return "Rubric Assessments"

    def create_export_job(self) -> BSExportJob:
        data_sets = self.bs_api.get_list_of_ads_data_set()
        data_set = next(filter(lambda x: x.name == self.name, data_sets), None)
        payload = {
            "DataSetId": data_set.data_set_id,
            "Filters": [
                {
                    "name": "parentOrgUnitId",
                    "value": self.org_unit_id
                },
                {
                    "name": "startDate",
                    "value": self.start_date
                },
                {
                    "name": "endDate",
                    "value": self.end_date
                }
            ]
        }
        return self.bs_api.create_export_job(payload=payload)


class ProgrammeLearningOutcomeEvaluationStream(ADSStream, ABC):
    def __init__(
            self, org_unit_id: str, include_not_achieved_learners: str, **kwargs
    ):
        super().__init__(**kwargs)
        self.org_unit_id = org_unit_id
        self.include_not_achieved_learners = include_not_achieved_learners

    @property
    def primary_key(self) -> Optional[Union[str, List[str], List[List[str]]]]:
        return ""

    @property
    def name(self) -> str:
        return "Programme Learning Outcome Evaluation"

    def create_export_job(self) -> BSExportJob:
        data_sets = self.bs_api.get_list_of_ads_data_set()
        data_set = next(filter(lambda x: x.name == self.name, data_sets), None)
        payload = {
            "DataSetId": data_set.data_set_id,
            "Filters": [
                {
                    "name": "parentOrgUnitId",
                    "value": self.org_unit_id
                },
                {
                    "name": "startDate",
                    "value": self.start_date
                },
                {
                    "name": "endDate",
                    "value": self.end_date
                },
                {
                    "name": "includeNotAchievedLearners",
                    "value": self.include_not_achieved_learners
                },
            ]
        }
        return self.bs_api.create_export_job(payload=payload)


class ContentProgressStream(ADSStream, ABC):
    def __init__(
            self, org_unit_id: str, roles: str, **kwargs
    ):
        super().__init__(**kwargs)
        self.org_unit_id = org_unit_id
        self.roles = roles

    @property
    def primary_key(self) -> Optional[Union[str, List[str], List[List[str]]]]:
        return ""

    @property
    def name(self) -> str:
        return "Content Progress"

    def create_export_job(self) -> BSExportJob:
        data_sets = self.bs_api.get_list_of_ads_data_set()
        data_set = next(filter(lambda x: x.name == self.name, data_sets), None)
        payload = {
            "DataSetId": data_set.data_set_id,
            "Filters": [
                {
                    "name": "parentOrgUnitId",
                    "value": self.org_unit_id
                },
                {
                    "name": "startDate",
                    "value": self.start_date
                },
                {
                    "name": "endDate",
                    "value": self.end_date
                },
                {
                    "name": "roles",
                    "value": ",".join(map(str, self.roles))
                }
            ]
        }
        return self.bs_api.create_export_job(payload=payload)


class SurveyResultsStream(ADSStream, ABC):
    def __init__(
            self, org_unit_id: str, roles: str, **kwargs
    ):
        super().__init__(**kwargs)
        self.org_unit_id = org_unit_id
        self.roles = roles

    @property
    def primary_key(self) -> Optional[Union[str, List[str], List[List[str]]]]:
        return ""

    @property
    def name(self) -> str:
        return "Survey Results"

    def create_export_job(self) -> BSExportJob:
        data_sets = self.bs_api.get_list_of_ads_data_set()
        data_set = next(filter(lambda x: x.name == self.name, data_sets), None)
        payload = {
            "DataSetId": data_set.data_set_id,
            "Filters": [
                {
                    "name": "parentOrgUnitId",
                    "value": self.org_unit_id
                },
                {
                    "name": "startDate",
                    "value": self.start_date
                },
                {
                    "name": "endDate",
                    "value": self.end_date
                },
                {
                    "name": "roles",
                    "value": ",".join(map(str, self.roles))
                }
            ]
        }
        return self.bs_api.create_export_job(payload=payload)


class CourseOfferingEnrollmentsStream(ADSStream, ABC):
    def __init__(
            self, org_unit_id: str, **kwargs
    ):
        super().__init__(**kwargs)
        self.org_unit_id = org_unit_id

    @property
    def primary_key(self) -> Optional[Union[str, List[str], List[List[str]]]]:
        return ""

    @property
    def name(self) -> str:
        return "Course Offering Enrolments"

    def create_export_job(self) -> BSExportJob:
        data_sets = self.bs_api.get_list_of_ads_data_set()
        data_set = next(filter(lambda x: x.name == self.name, data_sets), None)
        payload = {
            "DataSetId": data_set.data_set_id,
            "Filters": [
                {
                    "name": "parentOrgUnitId",
                    "value": self.org_unit_id
                },
                {
                    "name": "startDate",
                    "value": self.start_date
                },
                {
                    "name": "endDate",
                    "value": self.end_date
                },
            ]
        }
        return self.bs_api.create_export_job(payload=payload)


class AttendanceStream(ADSStream, ABC):
    def __init__(
            self, org_unit_id: str, roles: str, **kwargs
    ):
        super().__init__(**kwargs)
        self.org_unit_id = org_unit_id
        self.roles = roles

    @property
    def primary_key(self) -> Optional[Union[str, List[str], List[List[str]]]]:
        return ""

    @property
    def name(self) -> str:
        return "Attendance"

    def create_export_job(self) -> BSExportJob:
        data_sets = self.bs_api.get_list_of_ads_data_set()
        data_set = next(filter(lambda x: x.name == self.name, data_sets), None)
        payload = {
            "DataSetId": data_set.data_set_id,
            "Filters": [
                {
                    "name": "parentOrgUnitId",
                    "value": self.org_unit_id
                },
                {
                    "name": "startDate",
                    "value": self.start_date
                },
                {
                    "name": "endDate",
                    "value": self.end_date
                },
                {
                    "name": "roles",
                    "value": ",".join(map(str, self.roles))
                }
            ]
        }
        return self.bs_api.create_export_job(payload=payload)


class BDSStream(BrightspaceStream, ABC):

    def __init__(
            self, bs_api: BrightspaceClient, bds: BrightspaceDataSetInfo, **kwargs
    ):
        super().__init__(**kwargs)
        self.bs_api = bs_api
        self.bds_info = bds

    @property
    def name(self) -> str:
        return self.bds_info.full.name

    @property
    def cursor_field(self) -> str:
        return "last_modified"

    @property
    def primary_key(self) -> Optional[Union[str, List[str], List[List[str]]]]:
        return ""

    @lru_cache(maxsize=None)
    def get_json_schema(self) -> Mapping[str, Any]:
        extracts = self.bs_api.get_bds_extracts(
            schema_id=self.bds_info.schema_id,
            plugin_id=self.bds_info.full.plugin_id
        )
        full_extract = next(filter(lambda extract: extract.bds_type == BdsType.Full, extracts), None)
        bds_http_stream = self.bs_api.download_bds_extracts(full_extract.download_link)
        try:
            self.logger.info(f"Fetching schema started for bds {self.name} ................")
            fields = {}
            for df in self.read_csv_files(http_stream=bds_http_stream):
                for col in df.columns:
                    if df[col].isnull().values.all():
                        if not fields.get(col):
                            fields[col] = {"type": None}
                        continue
                    # if data type of the same column differs in dataframes, we choose the broadest one
                    prev_frame_column_type = fields.get(col, {}).get("type")
                    fields[col] = {"type": self.dtype_to_json_type(prev_frame_column_type, df[col].dtype),
                                   "dtype": df[col].dtype}

                    if is_timedelta(df[col]):
                        fields[col]["format"] = "date-time"
                        fields[col]["airbyte_type"] = "timestamp_with_timezone"
                    elif is_datetime(df[col]):
                        fields[col]["format"] = "date-time"
            schema = {}
            for field in fields:
                schema[field] = {"type": [fields[field]["type"] if fields[field]["type"] else "string", "null"]}
                if "format" in fields[field]:
                    schema[field]["format"] = fields[field]["format"]

            self.logger.info(f"Fetching schema completed for bds {self.name}.")
            return {
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "properties": schema
            }

        except pd.errors.EmptyDataError as e:
            self.logger.error(f"Empty data received. {e}")

    def read_csv_files(self, http_stream,
                       chunk_size: int = 1000,
                       sample_read: bool = False,
                       sample_read_count: int = 1000):
        with io.BytesIO(http_stream) as response_stream:
            with zipfile.ZipFile(response_stream) as zipped_data_set:
                files = zipped_data_set.namelist()
                if not files:
                    return
                csv_name = files[0]
                with zipped_data_set.open(csv_name) as csv_file:
                    chunks = pd.read_csv(csv_file, chunksize=chunk_size)
                    record_count = 0
                    for df in chunks:
                        yield df
                        record_count += len(df)
                        if sample_read and record_count >= sample_read_count:
                            break

    @staticmethod
    def dtype_to_json_type(current_type: str, dtype) -> str:
        """Convert Pandas Dataframe types to Airbyte Types.

        :param current_type: str - one of the following types based on previous dataframes
        :param dtype: Pandas Dataframe type
        :return: Corresponding Airbyte Type
        """
        number_types = ("double", "float64")
        integer_types = ("int32", "int64", "int96")
        if current_type == "string":
            # previous column values was of the string type, no sense to look further
            return current_type
        if dtype == object:
            return "string"
        if str(dtype).lower() in number_types:
            return "number"
        if str(dtype).lower() in integer_types:
            return "integer" if not current_type else current_type
        if str(dtype).lower() == "bool" and (not current_type or current_type == "boolean"):
            return "boolean"
        if dtype == "datetime64[ns]":
            return "date-time"
        return "string"

    def read_records(
            self,
            sync_mode: SyncMode,
            cursor_field: List[str] = None,
            stream_slice: Mapping[str, Any] = None,
            stream_state: Mapping[str, Any] = None,
    ) -> Iterable[Mapping[str, Any]]:

        if sync_mode == SyncMode.full_refresh:
            for record in self._full_read():
                yield record
        elif sync_mode == SyncMode.incremental:
            for record in self._incremental_read(stream_state):
                yield record

    def _full_read(self):
        extracts = self.bs_api.get_bds_extracts(
            schema_id=self.bds_info.schema_id,
            plugin_id=self.bds_info.full.plugin_id
        )
        full_extract = next(filter(lambda extract: extract.bds_type == BdsType.Full, extracts), None)
        if not full_extract.download_link:
            return
        extracted_stream = self.bs_api.download_bds_extracts(full_extract.download_link)
        for record in self.read_with_chunks(extracted_stream):
            record["stream_last_sync_time"] = full_extract.to_state
            yield record

    def _incremental_read(self, stream_state):
        last_modified_str = stream_state.get('last_modified') if stream_state else None
        if not last_modified_str:
            yield from self._full_read()
            return

        extracts = self.bs_api.get_bds_extracts(
            schema_id=self.bds_info.schema_id,
            plugin_id=self.bds_info.differential.plugin_id
        )
        # Convert last_modified string to datetime object
        last_modified = datetime.fromisoformat(last_modified_str.replace('Z', '+00:00'))

        # Using a lambda function to filter extracts based on the datetime comparison and then sort them
        filtered_extracts = filter(lambda extract: extract.bds_type == BdsType.Differential and extract.created_date > last_modified,
                                   extracts)
        sorted_extracts = sorted(filtered_extracts, key=lambda extract: extract.created_date)

        for extracted in sorted_extracts:
            if not extracted.download_link:
                continue
            extracted_stream = self.bs_api.download_bds_extracts(extracted.download_link)
            for record in self.read_with_chunks(extracted_stream):
                record["stream_last_sync_time"] = extracted.to_state
                yield record

    def get_updated_state(
            self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]
    ) -> MutableMapping[str, Any]:
        return {"last_modified": latest_record["stream_last_sync_time"]}
