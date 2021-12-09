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

y_code_dict = {'10Y1001C--000182': 'CA_UA_BEI', '10YUA-WEPS-----0': 'CA_UA_IPS'}
service_type_dict = {'FCR': 'РПЧ', 'aFRR': 'аРВЧ', 'mFRR': 'рРВЧ'}
df_n_columns = ['company_alias', 'y_code', 'service_type', 'w_code', 'date', 'time', 'direction', 'volume', 'payment']
result_columns = ['company_alias', 'y_code', 'service_type', 'w_code', 'datetime', 'direction']


def grouper(df):
    df_new = df.groupby(result_columns, as_index=False)[['volume', 'payment']].sum()
    df_new['monitoring'], df_new['activation'], df_new['multiplier'], df_new['penalty'] = [True, True, '', '']
    logger.debug(f'New dataframe cleared \n{df_new}')
    df_log = df.groupby(result_columns)[['volume', 'payment']].sum()
    logger.info(f'New dataframe cleared type 2 \n{df_log.head().to_string()}')
    return df_new


def create_df(path):
    df = pd.read_excel(path)
    df = df.drop(columns=['Formula Label', 'Product Type', 'Auction ID', 'Availability Flag', 'Price (₴/MWh)',
                          'Transaction Type'])
    df.columns = df_n_columns
    df['service_type'] = df['service_type'].replace(service_type_dict)
    df['y_code'] = df['y_code'].replace(y_code_dict)
    df['datetime'] = pd.to_datetime(df['date'].dt.strftime('%Y-%m-%d') + ' ' + df['time'].str[:2] + ':00')
    logger.debug(f'Dataframe from file {path} after def create_df\n{df} ')
    return df


def main():
    num = 0
    dir_list = list(Path.cwd().rglob('input*.xlsx'))
    output = Path.cwd() / 'output'
    logger.info(f'Founded {len(dir_list)} file/-s:{dir_list}')
    output.mkdir(parents=True, exist_ok=True)
    engine = create_engine(f'sqlite:///{output}\\{DATABASE_NAME}', echo=False)
    for item in dir_list:
        df = create_df(item)
        df = grouper(df)
        num += 1
        file_name = f'result{num}'
        df.to_sql(file_name, index=False, con=engine, if_exists='replace')
        logger.info(f'SQL table named {file_name} is created')
        df.to_excel(f'{output / (file_name + ".xlsx")}', index=False)
        logger.info(f'Excel {file_name}.xlsx is created')


if __name__ == '__main__':
    main()
