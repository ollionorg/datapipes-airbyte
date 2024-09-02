FROM python:3.9-slim as base

# Install required packages
RUN apt-get clean && \
    apt-get update && \
    apt-get install -y \
    libldap2-dev \
    rng-tools \
    libbz2-dev \
    zlib1g-dev \
    libsqlite3-dev \
    libreadline-dev \
    pcscd \
    scdaemon \
    make \
    wget \
    file \
    pinentry-tty \
    ca-certificates \
    lbzip2 \
    bzip2 \
    gcc

ARG GPG_VERSION=2.4.3
ENV GPG_VERSION "gnupg-$GPG_VERSION"

# Download and install GnuPG
RUN wget "https://gnupg.org/ftp/gcrypt/gnupg/${GPG_VERSION}.tar.bz2" && \
    wget "https://gnupg.org/ftp/gcrypt/gnupg/${GPG_VERSION}.tar.bz2.sig" && \
    tar xf ${GPG_VERSION}.tar.bz2 && \
    cd ${GPG_VERSION} && \
    ./configure --prefix=/usr/local && \
    make -j$(nproc) && \
    make install

# Verify GPG installation
RUN gpg --version

FROM base as builder

WORKDIR /airbyte/integration_code
COPY setup.py ./
RUN pip install --prefix=/install .

FROM base

# Copy installed Python dependencies from the builder stage
COPY --from=builder /install /usr/local

WORKDIR /airbyte/integration_code
COPY main.py ./
COPY source_file ./source_file

ENV AIRBYTE_ENTRYPOINT "python /airbyte/integration_code/main.py"
ENTRYPOINT ["python", "/airbyte/integration_code/main.py"]

LABEL io.airbyte.version=0.2.33-v4.4.3
LABEL io.airbyte.name=airbyte/source-file
