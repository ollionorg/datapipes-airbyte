#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from io import BytesIO

import gnupg


class Pgp:
    def __init__(self, private_key: str, encryption_method: str, passphrase: str = None) -> None:
        self.private_key = private_key
        self.passphrase = passphrase
        self.encryption_method = encryption_method

    def decrypt(self, fp):
        gpg = gnupg.GPG("/usr/bin/gpg")
        import_result = gpg.import_keys(self.private_key)
        gpg.trust_keys(import_result.fingerprints, "TRUST_ULTIMATE")
        decrypt = gpg.decrypt_file(fp, passphrase=self.passphrase)
        return BytesIO(bytes(str(decrypt), "utf-8"))
