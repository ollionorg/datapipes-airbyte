import json
import logging

from airbyte_cdk.sources import AbstractSource
from requests import HTTPError, codes

from source_brightspace.streams import *


class SourceBrightspace(AbstractSource):

    @staticmethod
    def _get_bs_object(config: Mapping[str, Any]) -> BrightspaceClient:
        bs = BrightspaceClient(**config)
        return bs

    def check_connection(self, logger: logging.Logger, config: Mapping[str, Any]) -> Tuple[bool, Optional[Any]]:
        try:
            bs_client = self._get_bs_object(config)
            bs_client.get_list_of_data_set()
        except HTTPError as error:
            if error.response.status_code == codes.UNAUTHORIZED:
                error_res = json.loads(error.response.content) or {}
                return False, f"{error_res.get('detail', 'UNAUTHORIZED')}, No permission -- see authorization schemes"
            else:
                return False, f"{error.response.text}"
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        bs_client = self._get_bs_object(config)
        streams = []
        stream_configs = {
            "final_grades": FinalGradesStream,
            "enrollments_and_withdrawals": EnrollmentsAndWithdrawalsStream,
            "all_grades": AllGradesStream,
            "learner_usage": LearnerUsageStream,
            "CLOE": CLOEStream,
            "instructor_usage": InstructorUsageStream,
            "awards_issued": AwardsIssuedStream,
            "rubric_assessments": RubricAssessmentsStream,
            "programme_learning_outcome_evaluation": ProgrammeLearningOutcomeEvaluationStream,
            "content_progress": ContentProgressStream,
            "survey_results": SurveyResultsStream,
            "course_offering_enrollments": CourseOfferingEnrollmentsStream,
            "attendance": AttendanceStream,
        }

        for stream_name, stream_class in stream_configs.items():
            stream_config = config.get(stream_name, None)
            if stream_config:
                streams.append(stream_class(bs_api=bs_client, **stream_config))

        return streams
