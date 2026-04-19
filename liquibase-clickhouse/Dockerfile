ARG LIQUIBASE_VERSION=4.33.0
FROM liquibase/liquibase:${LIQUIBASE_VERSION}

ARG CLICKHOUSE_JDBC_VERSION=0.9.7
ARG CLICKHOUSE_LIQUIBASE_EXTENSION_VERSION=0.9.1

ADD --chmod=0644 \
  https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/${CLICKHOUSE_JDBC_VERSION}/clickhouse-jdbc-${CLICKHOUSE_JDBC_VERSION}-all.jar \
  /liquibase/lib/clickhouse-jdbc.jar

ADD --chmod=0644 \
  https://repo1.maven.org/maven2/io/goodforgod/liquibase-clickhouse/${CLICKHOUSE_LIQUIBASE_EXTENSION_VERSION}/liquibase-clickhouse-${CLICKHOUSE_LIQUIBASE_EXTENSION_VERSION}.jar \
  /liquibase/lib/liquibase-clickhouse.jar

COPY changelog /liquibase/changelog
