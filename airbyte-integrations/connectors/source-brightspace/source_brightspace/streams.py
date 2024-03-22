import io
import math
import time
import zipfile
from abc import ABC, abstractmethod
from typing import Mapping, Optional, Any, Iterable, Union, List, Tuple

import pandas as pd
import pendulum
from airbyte_cdk.sources.streams import Stream
from airbyte_protocol.models import SyncMode
from numpy import nan
from pendulum import DateTime

from source_brightspace.api import BrightspaceClient, BSExportJob, ExportJobStatus


class ADSStream(Stream, ABC):
    DEFAULT_WAIT_TIMEOUT_SECONDS = 60 * 60 * 12  # 12-hour job running time
    MAX_CHECK_INTERVAL_SECONDS = 2.0
    MAX_RETRY_NUMBER = 3  # maximum number of retries for creating successful jobs

    def __init__(
            self, bs_api: BrightspaceClient, **kwargs
    ):
        super().__init__(**kwargs)
        self.bs_api = bs_api

    @abstractmethod
    def create_export_job(self) -> BSExportJob:
        pass

    @property
    def availability_strategy(self) -> Optional["AvailabilityStrategy"]:
        return None

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


class FinalGradesStream(ADSStream, ABC):

    def __init__(
            self, start_date: str, end_date: str, **kwargs
    ):
        super().__init__(**kwargs)
        self.start_date = start_date
        self.end_date = end_date

    @property
    def primary_key(self) -> Optional[Union[str, List[str], List[List[str]]]]:
        return "User Id"

    @property
    def name(self) -> str:
        return "Final Grades"

    def create_export_job(self) -> BSExportJob:
        data_sets = self.bs_api.get_list_of_data_set()
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
            self, org_unit_id: str, start_date: str, end_date: str, **kwargs
    ):
        super().__init__(**kwargs)
        self.org_unit_id = org_unit_id
        self.start_date = start_date
        self.end_date = end_date

    @property
    def primary_key(self) -> Optional[Union[str, List[str], List[List[str]]]]:
        return ""

    @property
    def name(self) -> str:
        # don't change typo here because D2L Brightspace has misspelled this
        return "Enrolments and Withdrawals"

    def create_export_job(self) -> BSExportJob:
        data_sets = self.bs_api.get_list_of_data_set()
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
            self, org_unit_id: str, start_date: str, end_date: str, **kwargs
    ):
        super().__init__(**kwargs)
        self.org_unit_id = org_unit_id
        self.start_date = start_date
        self.end_date = end_date

    @property
    def primary_key(self) -> Optional[Union[str, List[str], List[List[str]]]]:
        return ""

    @property
    def name(self) -> str:
        return "All Grades"

    def create_export_job(self) -> BSExportJob:
        data_sets = self.bs_api.get_list_of_data_set()
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
            self, org_unit_id: str, start_date: str, end_date: str, roles: str, **kwargs
    ):
        super().__init__(**kwargs)
        self.org_unit_id = org_unit_id
        self.start_date = start_date
        self.end_date = end_date
        self.roles = roles

    @property
    def primary_key(self) -> Optional[Union[str, List[str], List[List[str]]]]:
        return ""

    @property
    def name(self) -> str:
        return "Learner Usage"

    def create_export_job(self) -> BSExportJob:
        data_sets = self.bs_api.get_list_of_data_set()
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
                    "value": self.roles
                }
            ]
        }
        return self.bs_api.create_export_job(payload=payload)


class CLOEStream(ADSStream, ABC):
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
        return "CLOE"

    def create_export_job(self) -> BSExportJob:
        data_sets = self.bs_api.get_list_of_data_set()
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
                    "value": self.roles
                }
            ]
        }
        return self.bs_api.create_export_job(payload=payload)


class InstructorUsageStream(ADSStream, ABC):
    def __init__(
            self, org_unit_id: str, start_date: str, end_date: str, roles: str, **kwargs
    ):
        super().__init__(**kwargs)
        self.org_unit_id = org_unit_id
        self.start_date = start_date
        self.end_date = end_date
        self.roles = roles

    @property
    def primary_key(self) -> Optional[Union[str, List[str], List[List[str]]]]:
        return ""

    @property
    def name(self) -> str:
        return "Instructor Usage"

    def create_export_job(self) -> BSExportJob:
        data_sets = self.bs_api.get_list_of_data_set()
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
                    "value": self.roles
                }
            ]
        }
        return self.bs_api.create_export_job(payload=payload)


class AwardsIssuedStream(ADSStream, ABC):
    def __init__(
            self, org_unit_id: str, start_date: str, end_date: str, **kwargs
    ):
        super().__init__(**kwargs)
        self.org_unit_id = org_unit_id
        self.start_date = start_date
        self.end_date = end_date

    @property
    def primary_key(self) -> Optional[Union[str, List[str], List[List[str]]]]:
        return ""

    @property
    def name(self) -> str:
        return "Awards Issued"

    def create_export_job(self) -> BSExportJob:
        data_sets = self.bs_api.get_list_of_data_set()
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
            self, org_unit_id: str, start_date: str, end_date: str, **kwargs
    ):
        super().__init__(**kwargs)
        self.org_unit_id = org_unit_id
        self.start_date = start_date
        self.end_date = end_date

    @property
    def primary_key(self) -> Optional[Union[str, List[str], List[List[str]]]]:
        return ""

    @property
    def name(self) -> str:
        return "Rubric Assessments"

    def create_export_job(self) -> BSExportJob:
        data_sets = self.bs_api.get_list_of_data_set()
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
            self, org_unit_id: str, start_date: str, end_date: str, include_not_achieved_learners: str, **kwargs
    ):
        super().__init__(**kwargs)
        self.org_unit_id = org_unit_id
        self.start_date = start_date
        self.end_date = end_date
        self.include_not_achieved_learners = include_not_achieved_learners

    @property
    def primary_key(self) -> Optional[Union[str, List[str], List[List[str]]]]:
        return ""

    @property
    def name(self) -> str:
        return "Programme Learning Outcome Evaluation"

    def create_export_job(self) -> BSExportJob:
        data_sets = self.bs_api.get_list_of_data_set()
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
            self, org_unit_id: str, start_date: str, end_date: str, roles: str, **kwargs
    ):
        super().__init__(**kwargs)
        self.org_unit_id = org_unit_id
        self.start_date = start_date
        self.end_date = end_date
        self.roles = roles

    @property
    def primary_key(self) -> Optional[Union[str, List[str], List[List[str]]]]:
        return ""

    @property
    def name(self) -> str:
        return "Content Progress"

    def create_export_job(self) -> BSExportJob:
        data_sets = self.bs_api.get_list_of_data_set()
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
                    "value": self.roles
                }
            ]
        }
        return self.bs_api.create_export_job(payload=payload)


class SurveyResultsStream(ADSStream, ABC):
    def __init__(
            self, org_unit_id: str, start_date: str, end_date: str, roles: str, **kwargs
    ):
        super().__init__(**kwargs)
        self.org_unit_id = org_unit_id
        self.start_date = start_date
        self.end_date = end_date
        self.roles = roles

    @property
    def primary_key(self) -> Optional[Union[str, List[str], List[List[str]]]]:
        return ""

    @property
    def name(self) -> str:
        return "Survey Results"

    def create_export_job(self) -> BSExportJob:
        data_sets = self.bs_api.get_list_of_data_set()
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
                    "value": self.roles
                }
            ]
        }
        return self.bs_api.create_export_job(payload=payload)


class CourseOfferingEnrollmentsStream(ADSStream, ABC):
    def __init__(
            self, org_unit_id: str, start_date: str, end_date: str, **kwargs
    ):
        super().__init__(**kwargs)
        self.org_unit_id = org_unit_id
        self.start_date = start_date
        self.end_date = end_date

    @property
    def primary_key(self) -> Optional[Union[str, List[str], List[List[str]]]]:
        return ""

    @property
    def name(self) -> str:
        return "Course Offering Enrolments"

    def create_export_job(self) -> BSExportJob:
        data_sets = self.bs_api.get_list_of_data_set()
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
            self, org_unit_id: str, start_date: str, end_date: str, roles: str, **kwargs
    ):
        super().__init__(**kwargs)
        self.org_unit_id = org_unit_id
        self.start_date = start_date
        self.end_date = end_date
        self.roles = roles

    @property
    def primary_key(self) -> Optional[Union[str, List[str], List[List[str]]]]:
        return ""

    @property
    def name(self) -> str:
        return "Attendance"

    def create_export_job(self) -> BSExportJob:
        data_sets = self.bs_api.get_list_of_data_set()
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
                    "value": self.roles
                }
            ]
        }
        return self.bs_api.create_export_job(payload=payload)
