import numpy as np
import pandas as pd
import sys
import logging
from pathlib import Path
from logging import StreamHandler, Formatter
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

DATABASE_NAME = 'Task_06_12.sqlite'


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = StreamHandler(stream=sys.stdout)
handler.setFormatter(Formatter(fmt='[%(asctime)s: %(levelname)s] %(message)s'))
logger.addHandler(handler)

type_dict = {'FCR': 'РПЧ', 'aFRR': 'аРВЧ', 'mFRR': 'рРВЧ'}
result_columns = ['Market Participant', 'Service Type', 'Resource', 'Date', 'Settlement Period', 'Direction']


def create_index(df):
    df['Datetime'] = df.apply(lambda x: str(x['Date'])[:10] + ' ' + x['Settlement Period'], axis=1)
    df.index = df['Datetime']
    df.index = pd.to_datetime(df.index.str[:-6], format='%Y-%m-%d %H:%M')
    logger.debug('Index column was created using Date and Settlement Period')
    df = df.drop(columns=['Datetime'])
    return df


def select(df):
    df_new = df.groupby(result_columns, as_index=False)[['Quantity (MWh)', 'Ancillary Service Payment (₴)']].sum()
    create_index(df_new)
    logger.debug(f'New dataframe was created with columns {result_columns}')
    # df_new['Date'] = pd.to_datetime(str(df['Date']), format='%Y-%m-%d') # cant reformat into datetime
    df_new = df_new.drop(columns=['Datetime'])  # while am deleting column in create_index() it is still appear in
    # result*.xlsx
    df_new['Service Type'] = df_new['Service Type'].replace(type_dict)
    logger.debug(f'Service Type column was replaced using dictionary {type_dict}')
    return df_new


def create_df(path):
    df = pd.read_excel(path, sheet_name='Sheet1')
    logger.debug(f'Dataframe was created using file by {path}')
    create_index(df)
    return df


def main():
    num = 0
    dir_list = Path.cwd().rglob('input*.xlsx')
    output = Path.cwd() / 'output'
    logger.info(f'Founded {len(list(dir_list))} file/-s')
    output.mkdir(parents=True, exist_ok=True)
    logger.info('Output dir is created ')
    engine = create_engine(f'sqlite:///{output}\\{DATABASE_NAME}', echo=False)
    logger.info(f'Database {DATABASE_NAME} is created by path {output}')
    for item in dir_list:
        df = create_df(item)
        df = select(df)
        num += 1
        file_name = f'result{num}'
        df.to_sql(file_name, index=False, con=engine, if_exists='replace')
        engine.execute(f'SELECT * FROM {file_name}').fetchall()
        logger.debug(f'SQL table named {file_name} is created')
        df.to_excel(f'{output}\\{file_name}.xlsx', index=False)
        logger.debug(f'Excel table named {file_name}.xlsx is created')


if __name__ == '__main__':
    main()
