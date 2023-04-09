from clickhouse_connect.driver.exceptions import ClickHouseError, InterfaceError, DatabaseError, ProgrammingError


def get_accounts(client, logger):
    """Получить аккаунты"""

    query = f"""
             SELECT 
             al.id as api_id,
             asd.attribute_value as client_id,
             asd2.attribute_value as client_secret
             FROM account_service_data asd 
             JOIN account_list al ON asd.account_id  = al.id
             JOIN(SELECT *
                  FROM account_service_data asd 
                  JOIN account_list al ON asd.account_id = al.id
                  WHERE al.mp_id = 14) asd2 ON asd2.mp_id = al.mp_id AND asd2.account_id = asd.account_id 
             WHERE asd2.attribute_id != asd.attribute_id AND al.mp_id = 14 and al.status_1 = 'Active' AND asd.attribute_id = 9 
             ORDER BY al.id
             """

    try:
        res = client.query_df(query)
    except (ClickHouseError, InterfaceError, DatabaseError) as ex:
        logger.error(f"database error: {ex}")
        res = None

    return res


def get_last_dates(table_name: str, client, logger):
    """Получить последние даты статистики по аккаунтам"""

    query = f"""
             SELECT 
             api_id, 
             max(date) as max_date 
             FROM {table_name} 
             GROUP BY (api_id)
             """

    try:
        res = client.query_df(query)
    except (ClickHouseError, InterfaceError, DatabaseError) as ex:
        logger.error(f"database error: {ex}")
        res = None

    return res


def insert_data(dataset, table_name: str, client, logger):
    """Записывает датасет в таблицу"""

    try:
        client.insert(table=table_name,
                      data=list(dataset.to_dict(orient='list').values()),
                      column_names=list(dataset.columns),
                      column_oriented=True
                      )

        logger.info(f"successfully {dataset.shape[0]} rows")
        return 'ok'

    except (ProgrammingError, KeyError) as ex:
        logger.error(f"database error: {ex}")
        return None

