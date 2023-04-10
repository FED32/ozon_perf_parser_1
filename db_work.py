import pandas as pd
import numpy as np
import time
from datetime import datetime, date, timedelta
import os
import glob
from sqlalchemy import exc


# class DbWorking(host, port, name, user, password):
def sql_query(query, engine, logger, type_='dict'):
    """Выполнить SQL запрос на чтение"""

    with engine.connect() as connection:
        # with connection.begin() as transaction:
        try:
            data = pd.read_sql(query, con=connection)

            if data is None:
                logger.error("database error")
                result = None
            elif data.shape[0] == 0:
                logger.info(f"no data")
                if type_ == 'dict':
                    result = []
                elif type_ == 'df':
                    result = data
            else:
                if type_ == 'dict':
                    result = data.to_dict(orient='records')
                elif type_ == 'df':
                    result = data

        except (exc.DBAPIError, exc.SQLAlchemyError) as ex:
            logger.error(f"db error: {ex}")
            # transaction.rollback()
            raise
        except BaseException as ex:
            logger.error(f"{ex}")
            # transaction.rollback()
            raise
        finally:
            connection.close()

    return result


def get_last_date(table_name: str, account_id: str, engine, logger):
    """Получить последнюю дату статистики по аккаунту"""

    query = f"""
             SELECT max(date) 
             FROM {table_name} 
             WHERE account_id = {account_id}
             """

    return sql_query(query, engine, logger, type_='dict')


def get_last_dates(table_name: str, engine, logger):
    """Получить последние даты статистики по аккаунтам"""

    query = f"""
             SELECT 
             api_id, 
             max(date) as max_date 
             FROM {table_name} 
             GROUP BY (api_id)
             """

    return sql_query(query, engine, logger, type_='df')


def get_accounts(engine, logger):
    """Получить таблицу с аккаунтами"""

    query = """SELECT al.id, asd.attribute_value key_attribute_value, asd2.attribute_value FROM account_service_data 
    asd JOIN account_list al ON asd.account_id = al.id JOIN (SELECT al.mp_id, asd.account_id, asd.attribute_id, 
    asd.attribute_value FROM account_service_data asd JOIN account_list al ON asd.account_id = al.id WHERE al.mp_id = 
    14) asd2 ON asd2.mp_id = al.mp_id AND asd2.account_id= asd.account_id AND asd2.attribute_id <> asd.attribute_id 
    WHERE al.mp_id = 14 AND asd.attribute_id = 9 AND al.status_1 = 'Active' GROUP BY asd.attribute_id, 
    asd.attribute_value, asd2.attribute_id, asd2.attribute_value, al.id ORDER BY id"""

    return sql_query(query, engine, logger, type_='df')


def make_dataset(path):
    """Собирает датасет из загруженных данных"""

    columns = {
        'ID': 'campaign_id',
        'Название': 'campaign_name',
        'Дата': 'date',
        'Показы': 'views',
        'Клики': 'clicks',
        'Расход, ₽': 'expense',
        'Средняя ставка, ₽': 'avrg_bid',
        'Заказы, шт.': 'orders',
        'Заказы, ₽': 'revenue'
    }

    dtypes = {
        'api_id': 'int',
        'account_id': 'int',
        'campaign_id': 'int',
        'campaign_name': 'str',
        'date': 'datetime',
        'views': 'int',
        'clicks': 'int',
        'expense': 'float',
        'avrg_bid': 'float',
        'orders': 'int',
        'revenue': 'float'
    }

    csv_files = []
    for folder in os.listdir(path):
        csv_files += (glob.glob(os.path.join(path + folder + r'/daily', "*.csv")))

    if len(csv_files) == 0:
        return None

    else:
        stat_data = []
        for file in csv_files:
            data = pd.read_csv(file, sep=';')

            account_id = os.path.dirname(file).split('/')[-2].split('-')[0]
            api_id = os.path.dirname(file).split('/')[-2].split('-')[1]

            data['api_id'] = api_id
            data['account_id'] = account_id

            data.rename(columns=columns, inplace=True)

            stat_data.append(data)

        dataset = pd.concat(stat_data, axis=0)

        for col in dataset.columns:
            if dtypes[col] == 'int':
                dataset[col] = dataset[col].astype('int', copy=False, errors='ignore')
            elif dtypes[col] == 'float':
                dataset[col] = dataset[col].astype(str).str.replace(',', '.')
                dataset[col] = dataset[col].astype('float', copy=False, errors='ignore')
            elif dtypes[col] == 'datetime':
                # dataset[col] = pd.to_datetime(dataset[col], unit='D', errors='ignore')
                dataset[col] = dataset[col].apply(lambda x: datetime.strptime(x, '%Y-%m-%d').date())

        return dataset


def add_into_table(dataset, table_name: str, engine, logger, attempts=1):
    """Выполнить запись датасета в таблицу БД"""

    with engine.begin() as connection:
        n = 0
        while n < attempts:
            try:
                res = dataset.to_sql(name=table_name, con=connection, if_exists='append', index=False)
                logger.info(f"Upload to {table_name} - ok")
                return 'ok'
            except BaseException as ex:
                logger.error(f"data to db: {ex}")
                time.sleep(5)
                n += 1
        logger.error("data to db error")
        return None



