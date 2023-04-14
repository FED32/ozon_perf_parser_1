import pandas as pd
import numpy as np
import os
from datetime import datetime, date, timedelta
from threading import Thread
from sqlalchemy import create_engine
import clickhouse_connect
import shutil

import config
import logger
import db_work
import db_work_ch
from ozon_performance import OzonPerformance
# from ozon_performance import DbWorking


logger = logger.init_logger()


def get_reports(*args):

    api_id = args[1].split('-')[0]

    try:
        last_date = last_dates[last_dates['api_id'] == int(api_id)]['max_date'].values[0]
        date_from = str(np.datetime64(last_date, 'D') + np.timedelta64(1, 'D'))
        # date_from = str(last_date + timedelta(days=1))

    except (IndexError, KeyError, ValueError):
        date_from = str(date.today() - timedelta(days=90))

    date_to = str(date.today() - timedelta(days=1))

    ozon = OzonPerformance(account_id=args[0], client_id=args[1], client_secret=args[2])

    if ozon.auth is not None:
        ozon.collect_data(date_from, date_to, daily=True)
        ozon.save_data(path_=config.path_, daily=True)


if config.using_db == 'postgres':
    engine = create_engine(config.PG_DB_PARAMS)
    accounts = db_work.get_accounts(engine, logger).drop_duplicates(subset=['key_attribute_value', 'attribute_value'], keep='last')
    last_dates = db_work.get_last_dates(table_name=config.stat_table, engine=engine, logger=logger)

elif config.using_db == 'clickhouse':

    client = clickhouse_connect.get_client(
        interface='https',
        host=config.CH_HOST,
        port=config.CH_PORT,
        username=config.CH_USER,
        password=config.CH_PASSWORD,
        database=config.CH_DB_NAME,
        secure=True,
        verify=True,
        ca_cert=config.CH_CA_CERTS
    )

    # client = db_work_ch.get_client(logger)

    accounts = db_work_ch.get_accounts(client=client, logger=logger).drop_duplicates(subset=['client_id', 'client_secret'], keep='last')
    last_dates = db_work_ch.get_last_dates(table_name=config.stat_table, client=client, logger=logger)

else:
    raise Exception("Incorrect database")

threads = []
for index, keys in accounts.iterrows():
    client_id = keys[1]
    client_secret = keys[2]
    account_id = keys[0]

    threads.append(Thread(target=get_reports, args=(account_id, client_id, client_secret)))


print(threads)

# запускаем потоки
for thread in threads:
    thread.start()

# останавливаем потоки
for thread in threads:
    thread.join()


df = db_work.make_dataset(path=config.path_)

if df is None:
    logger.info("no downloaded files")

else:
    if df.shape[0] == 0:
        logger.info("no stat data for period")

    else:
        if config.upl_into_db == 1:
            if config.using_db == 'postgres':
                upload = db_work.add_into_table(dataset=df, table_name=config.stat_table, engine=engine,
                                                logger=logger, attempts=1)
                if upload is not None:
                    logger.info("Upload to postgres_db successful")
                else:
                    logger.error('Upload to postgres_db error')
            elif config.using_db == 'clickhouse':
                upload = db_work_ch.insert_data(dataset=df, table_name=config.stat_table, client=client, logger=logger)
                if upload is not None:
                    logger.info("Upload to ch_db successful")
                else:
                    logger.error('Upload to ch_db error')
        else:
            logger.info('Upl to db canceled')

    if config.delete_files == 1:
        try:
            shutil.rmtree(config.path_)
            logger.info('Files (folder) deleted')
        except OSError as e:
            logger.error("Error: %s - %s." % (e.filename, e.strerror))
    else:
        logger.info('Delete canceled')




