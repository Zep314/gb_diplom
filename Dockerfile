FROM apache/airflow:2.8.3
LABEL maintainer="mordanov.da@rkc43.ru"
COPY fbclient/libfbclient.so.2.5.9 /opt/airflow/
COPY fbclient/libncurses.so.5.9 /opt/airflow/
COPY fbclient/libtinfo.so.5.9 /opt/airflow/
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         libaio1 alien wget \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/* \
  && wget https://download.oracle.com/otn_software/linux/instantclient/1922000/oracle-instantclient19.22-basiclite-19.22.0.0.0-1.x86_64.rpm \
  && alien -i --scripts oracle-instantclient19.22-basiclite-19.22.0.0.0-1.x86_64.rpm \
  && rm -f oracle-instantclient19.22-basiclite-19.22.0.0.0-1.x86_64.rpm \
  && apt-get remove alien wget -y \
  && mv /opt/airflow/libfbclient.so.2.5.9 /usr/lib \
  && mv /opt/airflow/libncurses.so.5.9 /usr/lib \
  && mv /opt/airflow/libtinfo.so.5.9 /usr/lib \
  && chown root:root /usr/lib/libfbclient.so.2.5.9 \
  && chown root:root /usr/lib/libncurses.so.5.9 \
  && chown root:root /usr/lib/libtinfo.so.5.9 \
  && ln -s libfbclient.so.2.5.9 /usr/lib/libfbclient.so \
  && ln -s libncurses.so.5.9 /usr/lib/libncurses.so.5 \
  && ln -s libtinfo.so.5.9 /usr/lib/libtinfo.so.5
USER airflow
ADD requirements.txt .
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt
RUN rm ./requirements.txt