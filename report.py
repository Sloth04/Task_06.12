import pandas as pd
from insert import Records, Models, Resourse
from insert import session
import matplotlib
from config import *


def report_day(day):
    file_name = 'report_for_'+day
    q = session.query(Resourse.company_alias, Resourse.y_code,
                      Models.service_type, Records.w_code, Records.datetime,
                      Models.direction, Records.volume, Records.payment)\
        .join(Models)\
        .join(Resourse)\
        .filter(Records.datetime.like(f'{day}%'))
    df = pd.read_sql(q.statement, session.bind)
    df['service_type'] = df['service_type'].replace(service_type_dict)
    df['direction'] = df['direction'].replace(direction_dict)
    logger.info(f'New dataframe {file_name} \n{df.head().to_string()}')
    df.to_excel(f'{output / (file_name + ".xlsx")}', index=False)


def report_station(station):
    file_name = 'report_for_'+station
    q = session.query(Models.service_type, Records.w_code, Records.datetime,
                      Models.direction, Records.volume, Records.payment)\
        .join(Models)\
        .join(Resourse)\
        .filter(Records.w_code == station)
    df = pd.read_sql(q.statement, session.bind)
    df['service_type'] = df['service_type'].replace(service_type_dict)
    df['direction'] = df['direction'].replace(direction_dict)
    logger.info(f'New dataframe {file_name} \n{df.head().to_string()}')
    df.to_excel(f'{output / (file_name + ".xlsx")}', index=False)


if __name__ == '__main__':
    day_report = '2021-05-16'
    station_report = '62W001'
    report_day(day_report)
    report_station(station_report)
