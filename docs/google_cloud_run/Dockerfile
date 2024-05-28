# Copyright 2024 The GOE Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM google/cloud-sdk:slim
RUN apt-get update
# Oracle client
RUN apt-get -y install libaio1 libaio-dev unzip wget
RUN wget -q https://download.oracle.com/otn_software/linux/instantclient/218000/instantclient-sdk-linux.x64-21.8.0.0.0dbru.zip && \
    wget -q https://download.oracle.com/otn_software/linux/instantclient/218000/instantclient-basic-linux.x64-21.8.0.0.0dbru.zip && \
    wget -q https://download.oracle.com/otn_software/linux/instantclient/218000/instantclient-tools-linux.x64-21.8.0.0.0dbru.zip && \
    mkdir /opt/oracle && \
    unzip instantclient-sdk-linux.x64-21.8.0.0.0dbru.zip -d /opt/oracle/ && \
    unzip instantclient-basic-linux.x64-21.8.0.0.0dbru.zip -d /opt/oracle/ && \
    unzip instantclient-tools-linux.x64-21.8.0.0.0dbru.zip -d /opt/oracle/
ENV ORACLE_HOME=/opt/oracle/instantclient_21_8
ENV LD_LIBRARY_PATH=/opt/oracle/instantclient_21_8
# Python pre-reqs (gcc for cx_Oracle)
RUN apt-get install python3.11 python3.11-venv gcc -y
# Install GOE
ENV OFFLOAD_HOME=/opt/goe/offload
RUN mkdir /opt/goe
COPY goe.tar.gz /opt/goe/
RUN cd /opt/goe && tar xf goe.tar.gz
RUN cd ${OFFLOAD_HOME} && python3.11 -m venv ./.venv
COPY offload.env ${OFFLOAD_HOME}/conf/
RUN . ${OFFLOAD_HOME}/.venv/bin/activate && pip install --root-user-action=ignore --upgrade pip
RUN . ${OFFLOAD_HOME}/.venv/bin/activate && pip install --root-user-action=ignore ${OFFLOAD_HOME}/lib/goe-$(cat ${OFFLOAD_HOME}/version)-py3-none-any.whl
# Entrypoint
COPY goe.sh /opt/goe/
RUN chmod 500 /opt/goe/goe.sh
ENTRYPOINT ["/opt/goe/goe.sh"]
