FROM python:3.9-slim as base

# Install libassuan0 package
RUN apt-get clean
RUN apt-get update && apt-get install -y libldap2-dev rng-tools libbz2-dev zlib1g-dev libsqlite3-dev libreadline-dev pcscd scdaemon
RUN apt-get install -y make wget file pinentry-tty ca-certificates lbzip2 bzip2 gcc

ARG GPG_VERSION=2.4.3
ENV GPG_VERSION "gnupg-$GPG_VERSION"
RUN wget "https://gnupg.org/ftp/gcrypt/gnupg/${GPG_VERSION}.tar.bz2"
RUN	wget "https://gnupg.org/ftp/gcrypt/gnupg/${GPG_VERSION}.tar.bz2.sig"
RUN	tar xf ${GPG_VERSION}.tar.bz2
RUN	ln -s ${GPG_VERSION} gpg
RUN	cd ${GPG_VERSION} && make -f build-aux/speedo.mk native INSTALL_PREFIX=/usr/local -j4
RUN	ldconfig
RUN gpg -K

FROM base as builder

WORKDIR /airbyte/integration_code
COPY setup.py ./
RUN pip install --prefix=/install .

# Copy GPG from the builder stage to the final base stage
FROM base
#COPY --from=builder /usr/bin/gpg /usr/bin/gpg
COPY --from=builder /install /usr/local

WORKDIR /airbyte/integration_code
COPY main.py ./
COPY source_file ./source_file

ENV AIRBYTE_ENTRYPOINT "python /airbyte/integration_code/main.py"
ENTRYPOINT ["python", "/airbyte/integration_code/main.py"]

LABEL io.airbyte.version=0.2.33-v4.4.3
LABEL io.airbyte.name=airbyte/source-file