# Greenplum 6 + NiFi + ClickHouse

Локальный Docker Compose стек с Greenplum 6, Apache NiFi 2.8.0 и ClickHouse.
Миграции Greenplum накатываются отдельным контейнером Liquibase после готовности Greenplum.
Миграции ClickHouse накатываются отдельным контейнером Liquibase после готовности ClickHouse.
В образ `liquibase-greenplum` добавлен PostgreSQL JDBC-драйвер.
В образ `liquibase-clickhouse` добавлены ClickHouse JDBC-драйвер и ClickHouse extension.
В образ NiFi добавлены PostgreSQL и ClickHouse JDBC-драйверы.
Для загрузки файлов в Greenplum добавлен `gpfdist`, который раздает локальную landing-зону
`data/landing`.

## Запуск

```bash
docker compose up -d --build
```

При запуске `gpdb` сначала проходит healthcheck, затем `liquibase-greenplum` выполняет Greenplum-миграции и завершается.
ClickHouse стартует отдельным сервисом, затем `liquibase-clickhouse` выполняет ClickHouse-миграции и завершается.
NiFi стартует после успешного завершения обеих миграций.

Greenplum в этом стеке инициализируется как однонодовый кластер с 4 primary-сегментами.
Если меняешь число сегментов, нужно пересоздать volume `gpdata`, иначе уже созданный каталог
кластера останется со старой конфигурацией:

```bash
docker compose down
docker volume rm dwh_greenplum_gpdata
docker compose up -d --build
```

## Управление Greenplum

Остановить только Greenplum:

```bash
docker compose stop gpdb
```

Запустить только Greenplum:

```bash
docker compose up -d gpdb
```

Перезапустить только Greenplum:

```bash
docker compose restart gpdb
```

Пересоздать только контейнер Greenplum без удаления данных:

```bash
docker compose up -d --force-recreate gpdb
```

Посмотреть статус сервиса:

```bash
docker compose ps gpdb
```

Посмотреть логи Greenplum:

```bash
docker compose logs -f gpdb
```

## Управление ClickHouse

HTTP-интерфейс ClickHouse:

```text
http://localhost:8123
```

Native-порт ClickHouse:

```text
localhost:9000
```

Дефолтные параметры подключения из `.env.example`:

```text
Database: dwh
User: dwh
Password: dwhpw
```

Проверить ClickHouse через HTTP:

```bash
curl 'http://localhost:8123/?user=dwh&password=dwhpw' --data-binary 'SELECT 1'
```

Зайти в `clickhouse-client` внутри контейнера:

```bash
docker compose exec clickhouse clickhouse-client \
  --user dwh \
  --password dwhpw \
  --database dwh
```

Посмотреть статус сервиса:

```bash
docker compose ps clickhouse
```

Посмотреть логи ClickHouse:

```bash
docker compose logs -f clickhouse
```

## Миграции Liquibase для Greenplum

Миграции Greenplum лежат в:

```text
liquibase-greenplum/changelog/migrations/
```

Корневой changelog:

```text
liquibase-greenplum/changelog/root.yaml
```

Добавляй новые миграции отдельными YAML-файлами в `liquibase-greenplum/changelog/migrations/`.
Например:

```text
0002-create-some-table.yaml
```

Накатить миграции вручную:

```bash
docker compose build liquibase-greenplum
docker compose run --rm liquibase-greenplum
```

Посмотреть логи последнего запуска:

```bash
docker compose logs liquibase-greenplum
```

Если меняешь `GREENPLUM_DATABASE_NAME` или `GREENPLUM_PASSWORD`, Liquibase возьмет те же значения
из `.env`.

Зайти в `psql` внутри контейнера:

```bash
docker compose exec -u gpadmin gpdb /usr/local/greenplum-db/bin/psql -d gpdb
```

## Миграции Liquibase для ClickHouse

Миграции ClickHouse лежат в:

```text
liquibase-clickhouse/changelog/migrations/
```

Корневой changelog:

```text
liquibase-clickhouse/changelog/root.yaml
```

Добавляй новые миграции отдельными YAML-файлами в `liquibase-clickhouse/changelog/migrations/`.
Для таблиц ClickHouse указывай движок явно, например `ENGINE = MergeTree ORDER BY (...)`.

Накатить миграции ClickHouse вручную:

```bash
docker compose build liquibase-clickhouse
docker compose run --rm liquibase-clickhouse
```

Посмотреть логи последнего запуска:

```bash
docker compose logs liquibase-clickhouse
```

Проверить пример таблицы после миграций:

```bash
docker compose exec clickhouse clickhouse-client \
  --user dwh \
  --password dwhpw \
  --database dwh \
  --query 'DESCRIBE TABLE example_events'
```

UI NiFi:

```text
https://localhost:8443/nifi
```

Дефолтные учетные данные из `.env.example`:

```text
admin / GreenplumNiFi123
```

В образ NiFi уже добавлен PostgreSQL JDBC-драйвер:

```text
/opt/nifi/jdbc/postgresql.jar
```

И ClickHouse JDBC-драйвер:

```text
/opt/nifi/jdbc/clickhouse.jar
```

## Подключение к Greenplum из NiFi

Для controller service `DBCPConnectionPool` в NiFi используй:

```text
Database Connection URL: jdbc:postgresql://gpdb:5432/gpdb
Database Driver Class Name: org.postgresql.Driver
Database Driver Location(s): /opt/nifi/jdbc/postgresql.jar
Database User: gpadmin
Password: gpadminpw
```

Если поменяешь `GREENPLUM_DATABASE_NAME` или `GREENPLUM_PASSWORD`, укажи те же значения в NiFi.

## Подключение к ClickHouse из NiFi

Для controller service `DBCPConnectionPool` в NiFi используй:

```text
Database Connection URL: jdbc:clickhouse://clickhouse:8123/dwh
Database Driver Class Name: com.clickhouse.jdbc.ClickHouseDriver
Database Driver Location(s): /opt/nifi/jdbc/clickhouse.jar
Database User: dwh
Password: dwhpw
```

Если поменяешь `CLICKHOUSE_DB`, `CLICKHOUSE_USER` или `CLICKHOUSE_PASSWORD`, укажи те же значения в NiFi.

## External tables через gpfdist

`gpfdist` публикует каталог:

```text
data/landing
```

Внутри Docker-сети Greenplum читает файлы по адресу:

```text
gpfdist://gpfdist:8081/<folder>/*.csv
```

Для локальной проверки уже добавлен пример:

```text
data/landing/example_customers/sample_customers.csv
```

Liquibase создает схемы `ext` и `stg`, external table `ext.example_customers_raw`
и внутреннюю staging-таблицу `stg.example_customers`.

Проверить чтение external table:

```bash
docker compose exec -u gpadmin gpdb /usr/local/greenplum-db/bin/psql -d gpdb \
  -c "SELECT * FROM ext.example_customers_raw;"
```

Загрузить пример во внутреннюю staging-таблицу:

```bash
docker compose exec -u gpadmin gpdb /usr/local/greenplum-db/bin/psql -d gpdb \
  -c "TRUNCATE stg.example_customers; INSERT INTO stg.example_customers SELECT customer_id::bigint, full_name, email, created_at::timestamp FROM ext.example_customers_raw;"
```

Для NiFi каталог доступен внутри контейнера как:

```text
/data/landing
```

Например, файлы для новой сущности можно складывать в `/data/landing/orders/`,
а external table создавать с `LOCATION ('gpfdist://gpfdist:8081/orders/*.csv')`.
