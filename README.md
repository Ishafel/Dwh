# Greenplum 6 + NiFi

Локальный Docker Compose стек с Greenplum 6 и Apache NiFi 2.8.0.
Миграции БД накатываются отдельным контейнером Liquibase после готовности Greenplum.
В образ Liquibase добавлен PostgreSQL JDBC-драйвер.

## Запуск

```bash
docker compose up -d --build
```

При запуске `gpdb` сначала проходит healthcheck, затем `liquibase` выполняет миграции и завершается,
после этого стартует NiFi.

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

## Миграции Liquibase

Миграции лежат в:

```text
liquibase/changelog/migrations/
```

Корневой changelog:

```text
liquibase/changelog/root.yaml
```

Добавляй новые миграции отдельными YAML-файлами в `liquibase/changelog/migrations/`.
Например:

```text
0002-create-some-table.yaml
```

Накатить миграции вручную:

```bash
docker compose run --rm liquibase
```

Посмотреть логи последнего запуска:

```bash
docker compose logs liquibase
```

Если меняешь `GREENPLUM_DATABASE_NAME` или `GREENPLUM_PASSWORD`, Liquibase возьмет те же значения
из `.env`.

Зайти в `psql` внутри контейнера:

```bash
docker compose exec -u gpadmin gpdb /usr/local/greenplum-db/bin/psql -d gpdb
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
