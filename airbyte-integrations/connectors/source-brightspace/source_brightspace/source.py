import logging
from typing import Mapping, Any, List, Tuple, Optional

from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream

from source_brightspace.api import BrightspaceClient
from source_brightspace.streams import FinalGradesStream


class SourceBrightspace(AbstractSource):

    @staticmethod
    def _get_bs_object(config: Mapping[str, Any]) -> BrightspaceClient:
        bs = BrightspaceClient(**config)
        # sf.login()
        return bs

    def check_connection(self, logger: logging.Logger, config: Mapping[str, Any]) -> Tuple[bool, Optional[Any]]:
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        bs_client = self._get_bs_object(config)
        streams = []
        if config["final_grades"]:
            streams.append(FinalGradesStream(bs_api=bs_client, **config["final_grades"]))
        return streams
