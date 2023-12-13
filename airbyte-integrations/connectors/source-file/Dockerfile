FROM python:3.9-slim as base

# Install libassuan0 package
RUN apt-get update && apt-get install -y gnupg libassuan0

FROM base as builder

WORKDIR /airbyte/integration_code
COPY setup.py ./
RUN pip install --prefix=/install .

# Copy GPG from the builder stage to the final base stage
FROM base
COPY --from=builder /usr/bin/gpg /usr/bin/gpg
COPY --from=builder /install /usr/local

WORKDIR /airbyte/integration_code
COPY main.py ./
COPY source_file ./source_file

ENV AIRBYTE_ENTRYPOINT "python /airbyte/integration_code/main.py"
ENTRYPOINT ["python", "/airbyte/integration_code/main.py"]

LABEL io.airbyte.version=0.2.33-v4.4.1
LABEL io.airbyte.name=airbyte/source-file