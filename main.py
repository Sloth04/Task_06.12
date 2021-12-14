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
    id_type_direction = Column(Integer, ForeignKey('models.id'))
    datetime = Column(DateTime)
    volume = Column(Integer)
    payment = Column(Float)

    def __init__(self, w_code: str,  id_resourse: int, id_type_direction: int, datetime: DateTime, volume: int, payment: float):
        self.w_code = w_code
        self.id_resourse = id_resourse
        self.id_type_direction = id_type_direction
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


def create_models(df):
    variants = []
    dict_models = {}
    for item in df['service_type'].unique():
        temp = df.loc[df['service_type'] == item]
        dict_models[f'{item}'] = list(temp['direction'].unique())
    for y in dict_models.keys():
        for x in dict_models[y]:
            variants.append([y, x])
    print(variants)
    return variants


def create_resourse(df):
    variants = []
    for item in df['company_alias'].unique():
        temp = df.loc[df['company_alias'] == item]
        for y in list(temp['w_code'].unique()):
            for x in list(temp['y_code'].unique()):
                variants.append([item, y, x])
    print(variants)
    return variants


def fill_db(df, l_resourse, l_models):
    for res in l_resourse:
        resourse = get_or_create(session, Resourse, company_alias=res[0], w_code=res[1], y_code=res[2])
        session.add(resourse)
    for mod in l_models:
        models = get_or_create(session, Models, service_type=mod[0], direction=mod[1])
        session.add(models)
    # for index, row in df.iterrows():
    #     r = get_or_create(session, Resourse, w_code=row['w_code'])
    #     m = get_or_create(session, Models, service_type=row['service_type'], direction=row['direction'])
    #     incoming_record = Records(w_code=row['w_code'], id_resourse=r, id_type_direction=m, datetime=row['datetime'],
    #                               volume=row['volume'], payment=row['payment'])
    #     session.add(incoming_record)
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
        models = create_models(df)
        resourse = create_resourse(df)
        fill_db(df, resourse, models)
        # save = session.query(Records).filter_by(user_id=user_id).order_by(asc(UserSetting.name)).all()
        # df.to_sql(file_name, index=False, con=engine, if_exists='replace')
        # logger.info(f'SQL table named {file_name} is created')
        # df.to_excel(f'{output / (file_name + ".xlsx")}', index=False)
        # logger.info(f'Excel {file_name}.xlsx is created')


if __name__ == '__main__':
    main()
