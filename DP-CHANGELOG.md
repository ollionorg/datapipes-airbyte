# Change Log
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to [Semantic Versioning](http://semver.org/).

## [ Source-MSSQL 3.7.0-v1.0.0, Source-Mysql 3.3.7-v1.0.0, Source-Oracle 0.5.2-v1.0.0, Source-Postgres 3.3.10-v1.0.0 ] - 2024-03-06

### Chore
Rebased all the changes into datapipe-rdbms from main with latest changes


## [ Source-Salesforce 2.3.1-v1.0.0 ] - 2024-02-27

### Added
DPA-1988 End Date for Salesforce Stream

## [Source-File 1.2.4] - 2024-01-18

### Changed

- Added the test case for [test_dtype_to_json_type](https://github.com/ollionorg/datapipes-airbyte/blob/ce7554021f15fb981aacbf74031f9dfea5cf2143/airbyte-integrations/connectors/source-file/unit_tests/test_client.py#L102) .

### Fixed
- fixed the mapping between pandas dtype to airbyte types.