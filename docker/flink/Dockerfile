# syntax=docker/dockerfile:1
#
# Flink image for Floe's experimental "same Iceberg tables, different engine"
# profile. It is a stock Apache Flink image with three jars added to the
# classpath so Flink SQL can talk to Floe's catalog and warehouse:
#
#   - iceberg-flink-runtime : the Iceberg <-> Flink SQL connector
#   - iceberg-aws-bundle    : Iceberg S3FileIO + bundled AWS SDK (for MinIO)
#   - postgresql            : JDBC driver so Flink's JdbcCatalog can read the
#                             same Postgres-backed catalog PyIceberg writes
#   - flink-shaded-hadoop   : Iceberg core hard-references org.apache.hadoop.conf
#                             .Configuration, so a Hadoop classpath is required
#                             even though IO goes through S3FileIO (not S3A)
#
# Versions follow the Apache Iceberg "Flink getting started" guide. Flink 2.1
# with the Scala 2.12 build is the combination the iceberg-flink-runtime jar is
# published for.
FROM flink:2.1-scala_2.12-java17

# Iceberg release that ships the Flink 2.1 runtime + AWS bundle.
ARG ICEBERG_VERSION=1.11.0
# Iceberg publishes the Flink connector per Flink minor version.
ARG FLINK_MINOR=2.1
ARG PG_JDBC_VERSION=42.7.4
# Self-contained Hadoop classpath (provides org.apache.hadoop.conf.Configuration
# that Iceberg core requires to initialise any catalog).
ARG HADOOP_UBER_VERSION=2.8.3-10.0
ARG MAVEN=https://repo1.maven.org/maven2

USER root

# curl is used both to download the jars and by the healthcheck/readiness probe.
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends curl ca-certificates gettext-base; \
    rm -rf /var/lib/apt/lists/*

RUN set -eux; \
    cd /opt/flink/lib; \
    curl -fSL -o "iceberg-flink-runtime-${FLINK_MINOR}-${ICEBERG_VERSION}.jar" \
      "${MAVEN}/org/apache/iceberg/iceberg-flink-runtime-${FLINK_MINOR}/${ICEBERG_VERSION}/iceberg-flink-runtime-${FLINK_MINOR}-${ICEBERG_VERSION}.jar"; \
    curl -fSL -o "iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
      "${MAVEN}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar"; \
    curl -fSL -o "postgresql-${PG_JDBC_VERSION}.jar" \
      "${MAVEN}/org/postgresql/postgresql/${PG_JDBC_VERSION}/postgresql-${PG_JDBC_VERSION}.jar"; \
    curl -fSL -o "flink-shaded-hadoop-2-uber-${HADOOP_UBER_VERSION}.jar" \
      "${MAVEN}/org/apache/flink/flink-shaded-hadoop-2-uber/${HADOOP_UBER_VERSION}/flink-shaded-hadoop-2-uber-${HADOOP_UBER_VERSION}.jar"

COPY demo.sql /opt/floe/demo.sql
COPY run-demo.sh /opt/floe/run-demo.sh
# Normalize line endings (the build context may come from a Windows checkout) and
# make the entry point executable, so the container shebang never sees a stray CR.
RUN sed -i 's/\r$//' /opt/floe/run-demo.sh /opt/floe/demo.sql \
    && chmod +x /opt/floe/run-demo.sh

USER flink
