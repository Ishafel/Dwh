ARG GREENPLUM_IMAGE=woblerr/greenplum:6.27.1
FROM ${GREENPLUM_IMAGE}

ARG POSTGRES_JDBC_VERSION=42.7.10

USER root

ADD --chmod=0644 \
  https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar \
  /usr/local/pxf/lib/postgresql.jar

RUN chmod 755 /usr/local/pxf/lib \
    && chown gpadmin:gpadmin /usr/local/pxf/lib/postgresql.jar
