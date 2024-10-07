# Start from the official Python 3.11 slim image based on Debian Bullseye
FROM python:3.9-slim

ARG GPG_VERSION=2.4.3
ENV GPG_VERSION "gnupg-$GPG_VERSION"

# Update package lists
RUN apt-get update && apt-get install -y \
  gnupg \
  dirmngr \
  ca-certificates

# Set the working directory
WORKDIR /airbyte/integration_code

# Copy the application files
COPY setup.py ./
COPY main.py ./
COPY source_file ./source_file

# Install Python dependencies
RUN pip install --no-cache-dir .

# Set the entrypoint
ENV AIRBYTE_ENTRYPOINT="python /airbyte/integration_code/main.py"
ENTRYPOINT ["python", "/airbyte/integration_code/main.py"]

# Set labels for Airbyte integration
LABEL io.airbyte.version=0.2.33-v4.4.3
LABEL io.airbyte.name=airbyte/source-file