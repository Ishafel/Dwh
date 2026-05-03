# Greenplum 6 + PostgreSQL + Hive + NiFi + ClickHouse + Superset

Локальный Docker Compose стек с Greenplum 6, PostgreSQL, Hive Metastore, Apache NiFi 2.8.0
ClickHouse и Apache Superset.
Миграции PostgreSQL накатываются отдельным контейнером Liquibase после готовности PostgreSQL.
Миграции Greenplum накатываются отдельным контейнером Liquibase после готовности Greenplum.
Миграции ClickHouse накатываются отдельным контейнером Liquibase после готовности ClickHouse.
В образ `liquibase-postgres` добавлен PostgreSQL JDBC-драйвер.
В образ `liquibase-greenplum` добавлен PostgreSQL JDBC-драйвер.
В образ `liquibase-clickhouse` добавлены ClickHouse JDBC-драйвер и ClickHouse extension.
В образ NiFi добавлены PostgreSQL и ClickHouse JDBC-драйверы.
В образ Superset добавлены PostgreSQL/Greenplum и ClickHouse Python-драйверы.
Для загрузки файлов в Greenplum добавлен `gpfdist`, который через
`greenplum/start-gpfdist.sh` раздает локальную landing-зону `data/landing`.
PXF включен в Greenplum по умолчанию и настроен на минимальный Hive-пример через
`PROFILE=Hive`.

Для AI-ассистентов и автоматизированных правок есть компактная рабочая инструкция
`AGENTS.md`. При изменении операций, миграций, портов, credentials или тестов обновляй
и `README.md`, и `AGENTS.md`, чтобы человекочитаемая документация и tool-facing правила
не расходились.

## Запуск

```bash
docker compose up -d --build
```

При запуске `postgres` сначала проходит healthcheck, затем `liquibase-postgres` выполняет
PostgreSQL-миграции и завершается. `gpdb` после этого проходит healthcheck, затем
`liquibase-greenplum` выполняет Greenplum-миграции, включая PXF external tables, и завершается.
`hive-metastore` и `hive-init` подготавливают минимальную Hive sample-таблицу через
упорядоченные `.hql`-миграции из `hive/changelog/migrations/`. ClickHouse стартует отдельным сервисом, затем `liquibase-clickhouse`
выполняет ClickHouse-миграции и завершается. NiFi стартует после успешного завершения
трех контуров миграций. Superset использует отдельную PostgreSQL metadata database
`superset-db`, стартует после готовности PostgreSQL, Greenplum и ClickHouse migrations и инициализирует
локального admin-пользователя.

Greenplum в этом стеке инициализируется как однонодовый кластер с 4 primary-сегментами.
Если меняешь число сегментов, нужно пересоздать volume `gpdata`, иначе уже созданный каталог
кластера останется со старой конфигурацией. Это удалит данные Greenplum, поэтому выполняй
такой reset только после явного подтверждения, что данные можно потерять:

```bash
docker compose down
docker volume rm dwh_greenplum_gpdata
docker compose up -d --build
```

PXF тоже инициализируется внутри Greenplum volume. При каждом старте `gpdb` каталог
`/data/pxf/servers/` пересобирается из `greenplum/pxf/servers/` и синхронизируется через
`pxf cluster sync`, если PXF уже был подготовлен. На первом старте wrapper не создает
`/data/pxf/servers/` до `pxf cluster prepare`, чтобы не ломать неидемпотентную подготовку
PXF на persistent volume. Если после неудачного старта в `/data/pxf` остались только
server-конфиги без `/data/pxf/conf/pxf-env.sh`, wrapper удалит только эти server-конфиги
и даст штатному `pxf cluster prepare` выполниться заново. Если `/data/pxf` содержит другой
частичный state без `pxf-env.sh`, PXF будет отключен только на текущий старт, чтобы
Greenplum остался доступен без удаления данных.

Если PXF уже запущен и ты меняешь PXF-конфиги, перезапусти `gpdb` или вручную выполни внутри контейнера:

```bash
docker compose exec -u gpadmin gpdb bash -lc 'source ~/.bashrc && rm -rf /data/pxf/servers && mkdir -p /data/pxf/servers && cp -R /greenplum/pxf/servers/. /data/pxf/servers/ && pxf cluster sync && pxf cluster restart'
```

## Управление PostgreSQL

PostgreSQL в этом стеке предназначен для source/OLTP-подобных таблиц, из которых потом удобно
забирать данные в NiFi или перекладывать в Greenplum и ClickHouse.

Остановить только PostgreSQL:

```bash
docker compose stop postgres
```

Запустить только PostgreSQL:

```bash
docker compose up -d postgres
```

Перезапустить только PostgreSQL:

```bash
docker compose restart postgres
```

Пересоздать только контейнер PostgreSQL без удаления данных:

```bash
docker compose up -d --force-recreate postgres
```

Посмотреть статус сервиса:

```bash
docker compose ps postgres
```

Посмотреть логи PostgreSQL:

```bash
docker compose logs -f postgres
```

Зайти в `psql` внутри контейнера:

```bash
docker compose exec postgres psql -U app -d app
```

Дефолтные параметры подключения из `.env.example`:

```text
Host: localhost
Port: 5434
Database: app
User: app
Password: apppw
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

PXF-сервис запускается тем же контейнером `gpdb`, потому отдельного сервиса в Compose нет.
По умолчанию порт PXF доступен на хосте как:

```text
localhost:5888
```

Проверить PXF-процесс внутри Greenplum:

```bash
docker compose exec -u gpadmin gpdb bash -lc 'source ~/.bashrc && pxf cluster status'
```

Примерная PXF external table создается Greenplum Liquibase-миграцией
`liquibase-greenplum/changelog/migrations/tables/s_adb_as_services_csoko_stg/customers_ext.sql`.
После успешного запуска миграций и `hive-init` в Greenplum доступна:

```sql
SELECT count(*) FROM s_adb_as_services_csoko_stg.customers_ext;
```

## Управление Hive Metastore

Минимальный Hive-контур состоит из:

- `hive-metastore` - Hive Metastore 3.1.3 с embedded Derby metadata DB.
- `hive-init` - одноразовый init-контейнер, который готовит sample-файл и применяет
  Hive `.hql`-миграции из `hive/changelog/migrations/`.
- `hive_warehouse` - общий Docker volume с файлами Hive table, смонтированный в
  `hive-init`, `hive-metastore` и `gpdb`.

Hive image официально доступен как `linux/amd64`, поэтому в Compose задана platform
`${HIVE_PLATFORM:-linux/amd64}`.

Hive Metastore доступен на:

```text
localhost:9083
```

Серверный конфиг Hive Metastore:

```text
hive/conf/hive-site.xml
```

Скрипт запуска Hive Metastore:

```text
hive/start-metastore.sh
```

Клиентский конфиг для `hive-init`:

```text
hive/client-conf/hive-site.xml
```

Скрипт подготовки sample-таблицы Hive:

```text
hive/init-example.sh
```

Hive migrations:

```text
hive/changelog/migrations/
```

PXF Hive server config:

```text
greenplum/pxf/servers/hive/
```

Greenplum PXF external tables создаются через Liquibase migrations:

```text
liquibase-greenplum/changelog/migrations/
```

Проверить таблицу через Hive CLI:

```bash
docker compose run --rm --no-deps --entrypoint /opt/hive/bin/hive hive-init \
  --skiphadoopversion \
  --skiphbasecp \
  -e 'SELECT count(*) FROM demo.example_hive_customers;'
```

Проверить Hive-таблицу через Greenplum PXF:

```bash
docker compose exec -u gpadmin gpdb /usr/local/greenplum-db/bin/psql \
  -d gpdb \
  -c 'SELECT customer_id, full_name, email, created_at, ctl_loading FROM s_adb_as_services_csoko_stg.customers_ext ORDER BY customer_id;'
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

## Управление Superset

UI Superset:

```text
http://localhost:8088
```

Дефолтные учетные данные из `.env.example`:

```text
admin / SupersetAdmin123
```

Superset хранит собственные metadata в отдельном сервисе `superset-db`, а не в source
PostgreSQL `postgres`. Данные metadata лежат в volume `superset_db_data`, пользовательские
артефакты Superset - в volume `superset_home`.

Посмотреть статус сервисов:

```bash
docker compose ps superset superset-db
```

Посмотреть логи Superset:

```bash
docker compose logs -f superset
```

Проверить health endpoint:

```bash
curl -fsS http://localhost:8088/health
```

Подключение Greenplum в Superset:

```text
SQLAlchemy URI: postgresql+psycopg2://gpadmin:gpadminpw@gpdb:5432/gpdb
```

Подключение PostgreSQL source database в Superset:

```text
SQLAlchemy URI: postgresql+psycopg2://app:apppw@postgres:5432/app
```

Подключение ClickHouse в Superset:

```text
SQLAlchemy URI: clickhousedb+connect://dwh:dwhpw@clickhouse:8123/dwh
```

Если меняешь credentials в `.env`, обнови URI в Superset UI на такие же значения.

## Миграции Liquibase для PostgreSQL

Миграции PostgreSQL лежат в:

```text
liquibase-postgres/changelog/migrations/
```

Корневой changelog:

```text
liquibase-postgres/changelog/root.yaml
```

Добавляй новые миграции отдельными SQL-файлами в формате Liquibase Formatted SQL
в одну из подпапок `schemas/dm`, `functions/dm`, `tables/dm` или `views/dm`.
Порядок применения: сначала схемы, затем функции, затем таблицы и после них представления.
Например:

```text
liquibase-postgres/changelog/migrations/tables/dm/some_source_table.sql
```

Накатить миграции вручную:

```bash
docker compose build liquibase-postgres
docker compose run --rm liquibase-postgres
```

Посмотреть логи последнего запуска:

```bash
docker compose logs liquibase-postgres
```

Текущий базовый слой PostgreSQL:

- схема `dm` как стартовая точка для прикладных объектов промышленного стенда.

## Миграции Liquibase для Greenplum

Миграции Greenplum лежат в:

```text
liquibase-greenplum/changelog/migrations/
```

Корневой changelog:

```text
liquibase-greenplum/changelog/root.yaml
```

Добавляй новые миграции отдельными SQL-файлами в формате Liquibase Formatted SQL
в одну из подпапок `schemas`, `extensions`, `functions`, `tables` или `views`, затем в папку нужной схемы, если категория привязана к схеме.
Порядок применения: сначала схемы, затем расширения, затем функции, затем таблицы и после них представления.
Файлы называются точно как объект, над которым работает миграция, без числовых префиксов.
`changeSet.id` должен совпадать с именем файла без `.sql`.

Правило для таблиц и представлений Greenplum:

- для всех миграций создания таблиц по умолчанию указывай `runOnChange:true`, если явно не договорились иначе;
- external table можно пересоздавать через `DROP EXTERNAL TABLE IF EXISTS ... CASCADE`, потому зависимые представления должны восстанавливаться после table-блока;
- для представлений по умолчанию указывай `runOnChange:true runAlways:true`, чтобы они пересоздавались после каскадного удаления зависимостей;
- если порядок внутри одной папки важен, фиксируй его явными `include` в `root.yaml`, а не полагайся на сортировку `includeAll`.

Например:

```text
liquibase-greenplum/changelog/migrations/tables/s_adb_as_services_csoko_stg/some_table.sql
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

Проверить пример Hive PXF-таблицы после Greenplum Liquibase-миграций и `hive-init`:

```bash
docker compose exec -u gpadmin gpdb /usr/local/greenplum-db/bin/psql \
  -d gpdb \
  -c 'SELECT customer_id, full_name, email, created_at, ctl_loading FROM s_adb_as_services_csoko_stg.customers_ext ORDER BY customer_id;'
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

Добавляй новые миграции отдельными SQL-файлами в формате Liquibase Formatted SQL
в одну из подпапок `schemas/dwh`, `functions/dwh`, `tables/dwh` или `views/dwh`.
Порядок применения: сначала схемы, затем функции, затем таблицы и после них представления.
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

## Подключение к PostgreSQL из NiFi

Для controller service `DBCPConnectionPool` в NiFi используй:

```text
Database Connection URL: jdbc:postgresql://postgres:5432/app
Database Driver Class Name: org.postgresql.Driver
Database Driver Location(s): /opt/nifi/jdbc/postgresql.jar
Database User: app
Password: apppw
```

Если поменяешь `POSTGRES_DB`, `POSTGRES_USER` или `POSTGRES_PASSWORD`, укажи те же значения в NiFi.

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

`gpfdist` запускается через `greenplum/start-gpfdist.sh` и публикует каталог:

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

Liquibase создает external table `s_adb_as_services_csoko_stg.example_customers_ext`,
ODS-таблицу `s_adb_as_services_csoko_ods.example_customers` и view
`s_adb_as_services_csoko_ods.v_src_example_customers` для загрузочного слоя.

Проверить чтение external table:

```bash
docker compose exec -u gpadmin gpdb /usr/local/greenplum-db/bin/psql -d gpdb \
  -c "SELECT * FROM s_adb_as_services_csoko_stg.example_customers_ext;"
```

Загрузить пример во внутреннюю staging-таблицу:

```bash
docker compose exec -u gpadmin gpdb /usr/local/greenplum-db/bin/psql -d gpdb \
  -c "TRUNCATE s_adb_as_services_csoko_ods.example_customers; INSERT INTO s_adb_as_services_csoko_ods.example_customers SELECT customer_id, full_name, email, created_at FROM s_adb_as_services_csoko_ods.v_src_example_customers;"
```

Для NiFi каталог доступен внутри контейнера как:

```text
/data/landing
```

Например, файлы для новой сущности можно складывать в `/data/landing/orders/`,
а external table создавать с `LOCATION ('gpfdist://gpfdist:8081/orders/*.csv')`.

## Тесты

Быстрые проверки репозитория запускаются без поднятия всего Docker Compose стека:

```bash
make test
```

`./scripts/test.sh` оставлен как короткая обертка над `make test`.

Тесты проверяют:

- валидность `docker-compose.yml` через `docker compose config`;
- что переменные из Compose описаны в `.env.example`;
- порядок и стабильность Liquibase migrations;
- обязательный `DISTRIBUTED BY` для Greenplum-таблиц;
- обязательные `ENGINE` и `ORDER BY` для ClickHouse-таблиц;
- что `gpfdist` external tables указывают на committed sample CSV в `data/landing`;
- executable bit у shell entrypoints.

Когда стек уже запущен через `docker compose up -d --build`, можно выполнить интеграционные
проверки живых сервисов:

```bash
make test-stack
```

Эта цель делает реальные запросы к PostgreSQL, Greenplum, ClickHouse, NiFi и Superset. Если контейнеры
не запущены, `make test-stack` должен упасть. Для подключения используются значения из `.env`
с такими же defaults, как в `docker-compose.yml`.

На GitHub эти проверки запускаются workflow `.github/workflows/tests.yml` для pull request,
push в `main` и ручного `workflow_dispatch`.

## Автодеплой через GitHub Actions

Workflow `.github/workflows/deploy.yml` запускает деплой при push в `main`,
а также вручную через `workflow_dispatch`. Запуска на `pull_request` нет.

Сервер деплоя находится в приватной сети, поэтому GitHub-hosted runner из облака не сможет
подключиться к нему напрямую. Для автодеплоя нужен self-hosted runner на сервере с label:

```text
dwh-greenplum
```

Минимальная схема:

1. В GitHub открой `Settings -> Actions -> Runners -> New self-hosted runner`.
2. Выбери Linux x64 и выполни команды установки на сервере деплоя.
3. При настройке runner добавь label `dwh-greenplum`.
4. Установи runner как service и запусти его.

После этого при вливании в `main` workflow выполнит:

```bash
/mnt/bulk/dwh_greenplum/scripts/deploy.sh
```

Скрипт перед деплоем проверяет, что Docker использует `/mnt/bulk/docker`, выводит
`git status` и `docker compose ps`, затем выполняет:

```bash
git pull --ff-only origin main
```

После этого он запускает:

```bash
docker compose up -d --build
```

Скрипт не делает `docker compose down`, не удаляет volume, не удаляет Docker backup, не
трогает локальные untracked-файлы вроде `VM_REVIEW_NOTES_2026-04-19.md` и после деплоя
проверяет `docker compose ps`, health NiFi и health Superset.
