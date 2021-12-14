import numpy as np
import pandas as pd
import sys
import logging
from pathlib import Path
from logging import StreamHandler, Formatter
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, String, ForeignKey, DateTime, Float
from sqlalchemy.orm import relationship

Base = declarative_base()


class Records(Base):
    __tablename__ = 'records'

    id = Column(Integer, primary_key=True)
    w_code = Column(String)
    id_resourse = Column(Integer, ForeignKey('resourse.id'))
    id_type_direct = Column(Integer, ForeignKey('models.id'))
    datetime = Column(DateTime)
    volume = Column(Integer)
    payment = Column(Float)

    def __init__(self, w_code: str,  id_resourse: int, id_type_direct: int, datetime: DateTime, volume: int, payment: float):
        self.w_code = w_code
        self.id_resourse = id_resourse
        self.id_type_direct = id_type_direct
        self.datetime = datetime
        self.volume = volume
        self.payment = payment


class Models(Base):
    __tablename__ = 'models'

    id = Column(Integer, primary_key=True)
    service_type = Column(String)
    direction = Column(String)
    children = relationship('Records')

    def __init__(self, service_type: str, direction: str):
        self.service_type = service_type
        self.direction = direction


class Resourse(Base):
    __tablename__ = 'resourse'

    id = Column(Integer, primary_key=True)
    w_code = Column(String)
    company_alias = Column(String)
    y_code = Column(String)
    children = relationship('Records')

    def __init__(self, w_code: str, company_alias: str, y_code: str):
        self.w_code = w_code
        self.company_alias = company_alias
        self.y_code = y_code


engine2 = create_engine(f'sqlite:///output\\test_db.sqlite', echo=False)
Base.metadata.create_all(engine2)
Session = sessionmaker(bind=engine2)
session = Session()

DATABASE_NAME = 'Task_06_12.sqlite'


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = StreamHandler(stream=sys.stdout)
handler.setFormatter(Formatter(fmt='[%(asctime)s: %(levelname)s] %(message)s'))
logger.addHandler(handler)

direction_dict = {'Symmetric': 'Симетрично', 'Up': 'Вгору', 'Down': 'Вниз'}
y_code_dict = {'10Y1001C--000182': 'CA_UA_IPS', '10YUA-WEPS-----0': 'CA_UA_BEI'}
service_type_dict = {'FCR': 'РПЧ', 'aFRR': 'аРВЧ', 'mFRR': 'рРВЧ'}
df_n_columns = ['company_alias', 'y_code', 'service_type', 'w_code', 'date', 'time', 'direction', 'volume', 'payment']
result_columns = ['company_alias', 'y_code', 'service_type', 'w_code', 'datetime', 'direction']


def get_or_create(session, model, **kwargs):
    instance = session.query(model).filter_by(**kwargs).first()
    if instance:
        return instance
    else:
        instance = model(**kwargs)
        session.add(instance)
        session.commit()
        return instance


def grouper(df):
    df_new = df.groupby(result_columns, as_index=False)[['volume', 'payment']].sum()
    df_new['monitoring'], df_new['activation'], df_new['multiplier'], df_new['penalty'] = [True, True, '', '']
    logger.debug(f'New dataframe cleared \n{df_new}')
    df_log = df.groupby(result_columns)[['volume', 'payment']].sum()
    logger.info(f'New dataframe cleared type 2 \n{df_log.head().to_string()}')
    return df_new


def create_df(path):
    df = pd.read_excel(path)
    df = df.drop(columns=['Formula Label', 'Product Type', 'Auction ID', 'Availability Flag', 'Price (₴/MWh)', 'Transaction Type'])
    df.columns = df_n_columns
    df['y_code'] = df['y_code'].replace(y_code_dict)
    # df['service_type'] = df['service_type'].replace(service_type_dict) #replaced to ukr_language
    # df['direction'] = df['direction'].replace(direction_dict) #replaced to ukr_language
    df['datetime'] = pd.to_datetime(df['date'].dt.strftime('%Y-%m-%d') + ' ' + df['time'].str[:2] + ':00')
    logger.debug(f'Dataframe from file {path} after def create_df\n{df} ')
    return df


def fill_models(df):
    dict_models = {}
    check = []
    for item in df['service_type'].unique():
        temp = df.loc[df['service_type'] == item]
        dict_models[f'{item}'] = list(temp['direction'].unique())
    variants = [f'{y} {x}' for y in set(dict_models.keys()) for x in set(dict_models[y])]
    dict_models.clear()
    variants.sort(key=len)
    info_fdb = session.query(Models).all()
    for row in info_fdb:
        check.append(f'{row.service_type} {row.direction}')
    for item in variants:
        items = item.split()
        dict_models[item] = variants.index(item)+1
        if item not in check:
            incoming_type = Models(items[0], items[1])
            session.add(incoming_type)
    session.commit()
    return dict_models


def fill_resourse(df):
    variants = []
    dict_r = {}
    count = 0
    check = []
    for item in df['company_alias'].unique():
        temp = df.loc[df['company_alias'] == item]
        variants = [f'{item} {w} {y}' for w in list(temp['w_code'].unique()) for y in list(temp['y_code'].unique())]
        for i in variants:
            count += 1
            dict_r[i] = count
    info_fdb = session.query(Resourse).all()
    for row in info_fdb:
        check.append(f'{row.company_alias} {row.w_code} {row.y_code}')
    for item in dict_r.keys():
        items = item.split()
        if item not in check:
            incoming_resourse = Resourse(items[1], items[0], items[2])
            session.add(incoming_resourse)
    session.commit()
    return dict_r


def fill_records(df):
    check = []
    info_fdb = session.query(Records).all()
    for row in info_fdb:
        check.append(f'{row.w_code} {row.datetime} {row.volume}')
    for index, row in df.iterrows():
        item = str(row['w_code']) + ' ' + str(row['datetime']) + ' ' + str(row['volume'])
        # if item not in check:
        resourse = get_or_create(session, Resourse, w_code=row['w_code'])
        model = get_or_create(session, Models, service_type=row['service_type'], direction=row['direction'])
        incoming_record = Records(w_code=row['w_code'], id_resourse=resourse, id_type_direct=model, datetime=row['datetime'],
                                  volume=row['volume'], payment=row['payment'])
        session.add(incoming_record)
    session.commit()


def main():
    num = 0
    dir_list = list(Path.cwd().rglob('input*.xlsx'))
    output = Path.cwd() / 'output'
    logger.info(f'Founded {len(dir_list)} file/-s:{dir_list}')
    output.mkdir(parents=True, exist_ok=True)
    engine = create_engine(f'sqlite:///{output}\\{DATABASE_NAME}', echo=False)
    Base.metadata.create_all(engine)
    for item in dir_list:
        df = create_df(item)
        df = grouper(df)
        num += 1
        file_name = f'result{num}'
        d_models = fill_models(df)
        df['id_type_direction'] = df.apply(lambda x: str(x['service_type']) + ' ' + x['direction'], axis=1)
        df['id_type_direction'] = df['id_type_direction'].replace(d_models)
        d_resourse = fill_resourse(df)
        df['id_resourse'] = df.apply(lambda x: str(x['company_alias']) + ' ' + x['w_code'] + ' ' + x['y_code'], axis=1)
        df['id_resourse'] = df['id_resourse'].replace(d_resourse)
        # df = df.drop(columns=['company_alias', 'y_code', 'service_type', 'direction'])
        logger.debug(f'Dataframe added ids \n{df.head().to_string()}')
        fill_records(df)
        # save = session.query(Records).filter_by(user_id=user_id).order_by(asc(UserSetting.name)).all()
        # df.to_sql(file_name, index=False, con=engine, if_exists='replace')
        # logger.info(f'SQL table named {file_name} is created')
        # df.to_excel(f'{output / (file_name + ".xlsx")}', index=False)
        # logger.info(f'Excel {file_name}.xlsx is created')


if __name__ == '__main__':
    main()
