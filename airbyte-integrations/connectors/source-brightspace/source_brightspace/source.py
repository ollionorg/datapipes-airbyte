#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from abc import ABC
from email import message
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator
from airbyte_cdk.models import AirbyteMessage, AirbyteErrorTraceMessage
import re
from urllib.parse import unquote
import zipfile
import os
import csv
import json

# Basic full refresh stream
class BrightspaceStream(HttpStream, ABC):
    url_base = "https://nyptest.brightspace.com/"
    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def set_base_url(self, url):
        self.url_base = url

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return {}

    def create_folder_if_not_exists(folder_path):
        try:
            if not os.path.exists(folder_path):
                os.makedirs(folder_path)

        except PermissionError as pe:
            # print(f"Error: PermissionError - {pe}")
            raise AirbyteErrorTraceMessage(message=f"Error: PermissionError - {pe}")
        except OSError as ose:
            # print(f"Error: OSError - {ose}")
            raise AirbyteErrorTraceMessage(message=f"Error: OSError - {ose}")
            
        except Exception as e:
            # print(f"An unexpected error occurred while creating the folder: {e}")
            raise AirbyteErrorTraceMessage(message=f"An unexpected error occurred while creating the folder: {e}")

    
    def url_decode(input_string):
        try:
            decoded_string = unquote(input_string)
            return decoded_string

        except UnicodeDecodeError as ude:
            # print(f"Error: UnicodeDecodeError - {ude}")
            raise AirbyteErrorTraceMessage(message=f"Error: UnicodeDecodeError - {ude}")
        except Exception as e:
            # print(f"An unexpected error occurred during URL decoding: {e}")
            raise AirbyteErrorTraceMessage(message=f"An unexpected error occurred during URL decoding: {e}")
            # return None

    def read_csv_file(file_path):
        try:
            with open(file_path, 'r', newline='') as csvfile:
                csv_reader = csv.reader(csvfile)
                data = ""
                for row in csv_reader:
                    data += ', '.join(row) + '\n'
                return data
        except FileNotFoundError:
            # return f"File '{file_path}' not found."
            raise AirbyteErrorTraceMessage(message=f"File '{file_path}' not found.")
        except Exception as e:
            # return f"An error occurred while reading the file '{file_path}': {e}"
            raise AirbyteErrorTraceMessage(message=e)
    
    def unzip_file(zip_file_path, extraction_path):
        extracted_files = []
        try:
            with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                zip_ref.extractall(extraction_path)
                extracted_files = zip_ref.namelist()
        except zipfile.BadZipFile as bzfe:
            print(f"Error: Bad Zip File - {bzfe}")
            raise AirbyteErrorTraceMessage(message=bzfe)
        except zipfile.LargeZipFile as lzfe:
            print(f"Error: Large Zip File - {lzfe}")
            raise AirbyteErrorTraceMessage(message=lzfe)
        except zipfile.PatoolError as pe:
            print(f"Error: Patool Error - {pe}")
            raise AirbyteErrorTraceMessage(message=pe)
        except Exception as e:
            print(f"An unexpected error occurred during extraction: {e}")
            raise AirbyteErrorTraceMessage(message=e)

        return extracted_files

    def delete_file(file_path):
        try:
            os.remove(file_path)
            print(f"File '{file_path}' has been deleted.")
        except FileNotFoundError:
            # print(f"File '{file_path}' not found.")
            raise AirbyteErrorTraceMessage(message=f"File '{file_path}' not found.")
        except Exception as e:
            # print(f"An error occurred while deleting the file '{file_path}': {e}")
            raise AirbyteErrorTraceMessage(message=f"An error occurred while deleting the file '{file_path}': {e}")
    
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        # Check if 'content-disposition' header exists in the response headers
        if 'content-disposition' in response.headers:
            url = re.findall("filename=(.+)", response.headers["Content-Disposition"])[0]
            url = url.replace('"', '')
            url = BrightspaceStream.url_decode(url)
            url = self.file_path + url
            try:
                with open(url, 'wb') as out:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            out.write(chunk)
            except Exception as e:
                # print(f"An error occurred while writing to the file: {e}")
                raise AirbyteErrorTraceMessage(message=f"An error occurred while writing to the file: {e}")
            
            print(" Zip file downloaded successfully ")
            
            # unzip the file
            extracted_files = BrightspaceStream.unzip_file(url, self.file_path)
            
            # delete the zip file now
            BrightspaceStream.delete_file(url)
            
            result = ''
            for file in extracted_files:
                result += BrightspaceStream.read_csv_file(self.file_path + "/" + file)
                BrightspaceStream.delete_file(self.file_path + "/" + file)

            data = {"result" : result}
            json_like = json.dumps(data)
            
            response._content = json_like.encode('utf-8')
            return [response.json()]
        else:
            print("'content-disposition' header not found in response headers.")
            return []

class DownloadBdsPluginid(BrightspaceStream):

    def __init__(self, config: Mapping[str, Any], **kwargs):
        super().__init__()
        BrightspaceStream.set_base_url(self, config.get('url_base', ''))
        self.version = config.get('version', '')
        self.api_key = config.get('api_key', '')
        self.plugin_id = config.get('plugin_id', '')
        self.file_path = config.get('file_path', '/tmp/')
        BrightspaceStream.create_folder_if_not_exists(self.file_path)

    primary_key = None
    
    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        url = "d2l/api/lp/"+self.version + "/dataExport/bds/download/" + self.plugin_id
        return url
    def request_headers(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> Mapping[str, Any]:
        # The api requires that we include apikey as a header so we do that in this method
        return {'Authorization': 'Bearer ' + self.api_key}

    
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        return super().parse_response(response, **kwargs)

class DownloadDataSet(BrightspaceStream):
    primary_key = None
    
    def __init__(self, config: Mapping[str, Any], **kwargs):
        super().__init__()
        BrightspaceStream.set_base_url(self, config['url_base'])
        self.version = config.get('version', '')
        self.api_key = config.get('api_key', '')
        self.plugin_id = config.get('plugin_id', '')
        self.file_path = config.get('file_path', '/tmp/')
        self.schema_id = config.get('schema_id', '')
        self.extract_id = config.get('extract_id', '')
        BrightspaceStream.create_folder_if_not_exists(self.file_path)
    
    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        #https://nyptest.brightspace.com/d2l/api/lp/1.43/datasets/bds/435ee960-871f-484f-8e66-44886dea08f8/plugins/d18ed567-e0a3-4fb7-912f-84d294620830/extracts/MTcwNTAwMzUwMA
        url = "d2l/api/lp/" + self.version + "/datasets/bds/"+ self.schema_id + "/plugins/" + self.plugin_id + "/extracts/" + self.extract_id
        return url
    def request_headers(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> Mapping[str, Any]:
        return {'Authorization': 'Bearer ' + self.api_key}

    
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        return super().parse_response(response, **kwargs)


class DownloadExportJobs(BrightspaceStream):

    def __init__(self, config: Mapping[str, Any], **kwargs):
        super().__init__()
        BrightspaceStream.set_base_url(self, config.get('url_base', ''))
        self.api_key = config.get('api_key', '')
        self.version = config.get('version', '')
        self.export_job_id = config.get('export_job_id', '')
        self.file_path = config.get('file_path', '/tmp/')
        BrightspaceStream.create_folder_if_not_exists(self.file_path)

    primary_key = None
            
    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        url = "d2l/api/lp/" + self.version + "/dataExport/download/"+ self.export_job_id
        return url
    def request_headers(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> Mapping[str, Any]:
        # The api requires that we include apikey as a header so we do that in this method
        return {'Authorization': 'Bearer ' + self.api_key}

    
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        return super().parse_response(response, **kwargs)

# Source
class SourceBrightspace(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        try:
            required_parameters = ['url_base', 'version', 'api_key']
            missing_parameters = [param for param in required_parameters if param not in config]
            if missing_parameters:
                raise AirbyteMessage(
                    message=f"Missing required parameters: {', '.join(missing_parameters)}"
                )
            url = config['url_base'] + "d2l/api/lp/" + config['version'] + "/dataExport/list"
            session = requests.Session()
            response = session.get(url, headers={'Authorization': 'Bearer ' + config['api_key']})
            if response.status_code == 200:
                return True, None 
            else:
                error_message = f"API request failed with status code: {response.status_code}"
                AirbyteMessage(message=error_message)
                return False, error_message

        except Exception as e:
            error_message = f"An error occurred while checking the connection: {str(e)}"
            AirbyteMessage(message=error_message)
            return False, error_message

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        return [ DownloadBdsPluginid(config=config), DownloadExportJobs(config=config), DownloadDataSet(config=config)]
