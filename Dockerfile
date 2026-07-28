FROM python:3.11-alpine

ARG CASSANDRA_VERSION=5.0.1
ENV PYTHONPATH=/opt/cassandra/pylib:/opt/cassandra/bin
ENV PATH="/opt/cassandra/bin:${PATH}"

COPY ./src/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

RUN pip install --no-cache-dir ruff watchfiles pytest requests

RUN wget https://github.com/apache/cassandra/archive/refs/tags/cassandra-${CASSANDRA_VERSION}.tar.gz && \
    mkdir -p /opt/cassandra && \
    tar -xzf cassandra-*.tar.gz --strip-components=1 -C /opt/cassandra && \
    rm cassandra-*.tar.gz

RUN echo "version = \"${CASSANDRA_VERSION}\"" > /opt/cassandra/pylib/cqlshlib/serverversion.py

RUN if [ ! -f /opt/cassandra/pylib/cqlshlib/cqlshmain.py ]; then \
        cp /opt/cassandra/bin/cqlsh.py /opt/cassandra/pylib/cqlshlib/cqlshmain.py; \
    fi

CMD ["sh", "-c", "watchfiles 'ruff format /app/app.py' /app & python /app/app.py"]