#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
import logging

import gnupg
from airbyte_cdk.entrypoint import logger

logging.getLogger("gnupg").setLevel(logging.DEBUG)


class Pgp:
    def __init__(self, private_key: str, encryption_method: str, passphrase: str = None) -> None:
        self.private_key = private_key
        self.passphrase = passphrase
        self.encryption_method = encryption_method

    def decrypt(self, fp, output):
        logger.info("start decrypt")
        gpg = gnupg.GPG()

        logger.info("GPG FOUND")
        logger.info(f"gnupg Version: {gnupg.__version__}")
        logger.info(f"version:{gpg.version}")

        import_result = gpg.import_keys(self.private_key)
        logger.info("PRIVATE KEY IMPORTED")

        gpg.trust_keys(import_result.fingerprints, "TRUST_ULTIMATE")
        logger.info("Trust key added")

        with fp as encrypted_file:
            _ = gpg.decrypt_file(encrypted_file, passphrase=self.passphrase, always_trust=True, output=output)
