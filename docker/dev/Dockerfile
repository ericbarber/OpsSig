#/docker/dev/Dockerfile
# Copyright (2023) The Delta Lake Project Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# ------------------------------------------------
# Dockerfile for Delta Lake quickstart
# ------------------------------------------------

# This docker image uses the official Docker image of [OSS] Apache Spark v3.5.0 as the base container
# Note: Python version in this image is 3.9.2 and is available as `python3`.
# Note: PySpark v3.5.0 (https://spark.apache.org/docs/latest/api/python/getting_started/install.html#dependencies)
ARG BASE_CONTAINER=spark:3.5.1-scala2.12-java17-python3-ubuntu
FROM $BASE_CONTAINER AS spark
FROM spark AS delta

# Authors (add you name when updating the Dockerfile)
LABEL authors="Eric Barber"

# Stage 1: Delta Table, Spark, Python, Jupyter install
# Docker image was created and tested with the versions of following packages.
USER root

ARG ENV
ARG OPSSIG_VERSION
ARG DELTA_SPARK_VERSION
# Note: for 3.0.0 https://pypi.org/project/deltalake/
ARG DELTALAKE_VERSION
ARG JUPYTERLAB_VERSION
# requires pandas >1.0.5, py4j>=0.10.9.7, pyarrow>=4.0.0
ARG PANDAS_VERSION
ARG ROAPI_VERSION
ARG MATPLOTLIB_VERSION
ARG PYTEST_VERSION

# We are explicitly pinning the versions of various libraries which this Docker image runs on.
RUN pip install --quiet --no-cache-dir delta-spark==${DELTA_SPARK_VERSION} \
  deltalake==${DELTALAKE_VERSION} \
  jupyterlab==${JUPYTERLAB_VERSION} \
  pandas==${PANDAS_VERSION} \
  roapi==${ROAPI_VERSION} \
  matplotlib==${MATPLOTLIB_VERSION} \
  pytest==${PYTEST_VERSION} 

# Environment variables
FROM delta AS startup
ARG NBuser=NBuser
ARG GROUP=NBuser
ARG WORKDIR=/opt/spark/work-dir
ENV DELTA_PACKAGE_VERSION=delta-spark_2.12:${DELTA_SPARK_VERSION}

# OS Installations Configurations
RUN groupadd -r ${GROUP} && useradd -r -m -g ${GROUP} ${NBuser}
RUN apt -qq update
RUN apt -qq -y install vim curl tree iputils-ping netcat

# COPY --chown=${NBuser} quickstart.ipynb "${WORKDIR}"
COPY --chown=${NBuser} docker/rs/ "${WORKDIR}/rs"
RUN chown -R ${NBuser}:${GROUP} /home/${NBuser}/ \
  && chown -R ${NBuser}:${GROUP} ${WORKDIR}

# Stage 2: Rust install
USER ${NBuser}
RUN curl https://sh.rustup.rs -sSf | sh -s -- -y

# moved the source command into the bash process in the entrypoint docker_startup.sh
#RUN source "$HOME/.cargo/env"

FROM startup AS migrate
# Stage 3: Add application code

# Configure ownership
COPY --chown=${NBuser} docker/${ENV}/docker_startup.sh "${WORKDIR}"
COPY --chown=${NBuser} docker/${ENV}/spark_setup_test.sh "${WORKDIR}"

# Add config to startup
RUN mkdir -p ${WORKDIR}/config
COPY --chown=${NBuser} ../config "${WORKDIR}/config"

# Add examples to startup
RUN mkdir -p ${WORKDIR}/examples
COPY --chown=${NBuser} ../examples "${WORKDIR}/examples"

# Add backend to startup
RUN mkdir -p ${WORKDIR}/backend
COPY --chown=${NBuser} ../backend "${WORKDIR}/backend"

# Add OpsSig to startup
RUN mkdir -p ${WORKDIR}/services
COPY --chown=${NBuser} ../services "${WORKDIR}/services"

# Establish test environment entry point
ENTRYPOINT ["bash", "docker_startup.sh"]

