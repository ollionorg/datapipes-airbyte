from source_brightspace import SourceBrightspace
import logging
from airbyte_cdk.models import (
    AirbyteCatalog,
    AirbyteConnectionStatus,
    AirbyteMessage,
    AirbyteStateMessage,
    AirbyteStreamStatus,
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    SyncMode,
    DestinationSyncMode,
    Status,
    StreamDescriptor,
)
from airbyte_cdk.logger import init_logger

logger = init_logger("airbyte")

if __name__ == '__main__':
    # logger = init_logger()
    config = {
        "instance_url": "https://nyptest.brightspace.com",
        "airflow_get_token": "",
        "ingestor_source_update_url": "",
        "client_id": "7f975b5d-cd9e-4587-b878-e03033dbe355",
        "client_secret": "FGqZr4qG6qYDSs8AwFHSZuXm3YLVp4qRjsoJCkFJLxY",
        "refresh_token": "rt.ap-southeast-1.xz9lVabwIs58qJs0DOwsmpnhTHJzsBYnw3mkyUQcZqQ",
        # "attendance": {
        #     "dataset_type": "ads",
        #     "org_unit_id": 6664,
        #     "start_date": "2010-07-21",
        #     # "end_date": "2024-05-21",
        #     "roles": [117, 140, 138, 178, 135, 186, 188, 192, 136, 185, 195, 265, 267, 282, 286, 297, 187, 266, 311, 141, 303, 330]
        # },
        "survey_results": {
            "dataset_type": "ads",
            "org_unit_id": 6664,
            "start_date": "2010-07-21",
            # "end_date": "2024-05-21",
            "roles": [117, 140, 138, 178, 135, 186, 188, 192, 136, 185, 195, 265, 267, 282, 286, 297, 187, 266, 311, 141, 303, 330]
        },
        "access_token": "eyJhbGciOiJSUzI1NiIsImtpZCI6ImFkNTQzY2QxLWRiYTktNGUwNS04MzA4LTlhYzBlNGUxOGM0MyIsInR5cCI6IkpXVCJ9.eyJuYmYiOjE3MjQ2NzQxMTQsImV4cCI6MTcyNDY3NzcxNCwiaXNzIjoiaHR0cHM6Ly9hcGkuYnJpZ2h0c3BhY2UuY29tL2F1dGgiLCJhdWQiOiJodHRwczovL2FwaS5icmlnaHRzcGFjZS5jb20vYXV0aC90b2tlbiIsInN1YiI6IjY1NDY1IiwidGVuYW50aWQiOiIyMjk2YmQ5Mi1jMWUzLTQ5MzQtOTg0NC03MjkxMTcyNjFiMDIiLCJhenAiOiI3Zjk3NWI1ZC1jZDllLTQ1ODctYjg3OC1lMDMwMzNkYmUzNTUiLCJzY29wZSI6ImRhdGFodWI6ZGF0YWV4cG9ydHM6ZG93bmxvYWQscmVhZCBkYXRhc2V0czpiZHM6bGlzdCxyZWFkIHJlcG9ydGluZzpkYXRhc2V0OmZldGNoLGxpc3QgcmVwb3J0aW5nOmpvYjpjcmVhdGUsZG93bmxvYWQsZmV0Y2gsbGlzdCIsImp0aSI6IjBjMWQwMWMxLTdkYWUtNDc4OS05ZGZmLWQ1Y2RhMTI3YWI4YiJ9.oMV44uCvPnfl-tOy1nqSKRzC70hb5YVPvdw9jWVnHY_0SxTvpquhqw-wJ_ohk7Tr8gNOATotjHJzmElCf3nV3UPxS-kITy0-Mym1uzF6bfN7KfX7cPa6nb5j9KfqH2mDt3IlTh_stuE3CPZpiF1GjaTYJewTjr18XGW1tjl94s4zscY6zr6hTlVIwO_yiZirDbyLTUXpI_FDnHKLDJhWV8aC__lAo_n2nfWSM5iG1FWegSnuHHEj0KzEXfONPRB0WZJbl8rAiyiC2Pnl2VMcjVni0rhoWC0aA_L27dJVCHI3DVidzjRC3v4gLB0sHTZ8DWhsvb51fkLfJQQDU1G64w"
    }
    source = SourceBrightspace()
    check = source.check_connection(logger, config=config)
    print(check)
    dis = source.discover(logger, config=config)
    print(dis)
    catalog = ConfiguredAirbyteCatalog(streams=[])

    for stream in dis.streams:
        config_stream = ConfiguredAirbyteStream(
            stream=stream,
            sync_mode=SyncMode.full_refresh,
            destination_sync_mode=DestinationSyncMode.overwrite,
        )
        catalog.streams.append(config_stream)

    for msg in source.read(logger, config=config, catalog=catalog, state=None):
        print(msg)
