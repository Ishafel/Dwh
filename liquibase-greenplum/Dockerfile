ARG LIQUIBASE_VERSION=5.0.2
FROM liquibase/liquibase:${LIQUIBASE_VERSION}

ARG POSTGRES_JDBC_VERSION=42.7.10

ADD --chmod=0644 \
  https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar \
  /liquibase/lib/postgresql.jar

COPY changelog /liquibase/changelog
