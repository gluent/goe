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
RUN . ${OFFLOAD_HOME}/.venv/bin/activate && pip install --root-user-action=ignore ${OFFLOAD_HOME}/lib/goe-$(cat ${OFFLOAD_HOME}/version| tr 'A-Z-' 'a-z.')-py3-none-any.whl
# Entrypoint
COPY goe.sh /opt/goe/
RUN chmod 555 /opt/goe/goe.sh
ENTRYPOINT ["/opt/goe/goe.sh"]
