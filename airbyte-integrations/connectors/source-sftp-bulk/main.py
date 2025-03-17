#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
import logging

from source_sftp_bulk.run import run

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info("This is an INFO log.")
    logging.debug("This is a DEBUG log.")
    run()
