FROM python:3.11-alpine

ARG CASSANDRA_VERSION=5.0.1
ENV PYTHONPATH=/opt/cassandra/pylib:/opt/cassandra/bin
ENV PATH="/opt/cassandra/bin:${PATH}"

COPY ./src /app
RUN pip install --no-cache-dir -r /app/requirements.txt

RUN wget https://github.com/apache/cassandra/archive/refs/tags/cassandra-${CASSANDRA_VERSION}.tar.gz && \
    mkdir -p /opt/cassandra && \
    tar -xzf cassandra-*.tar.gz --strip-components=1 -C /opt/cassandra && \
    rm cassandra-*.tar.gz

RUN echo "version = \"${CASSANDRA_VERSION}\"" > /opt/cassandra/pylib/cqlshlib/serverversion.py

RUN if [ ! -f /opt/cassandra/pylib/cqlshlib/cqlshmain.py ]; then \
        cp /opt/cassandra/bin/cqlsh.py /opt/cassandra/pylib/cqlshlib/cqlshmain.py; \
    fi

CMD ["python", "/app/app.py"]
