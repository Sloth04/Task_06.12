import pandas as pd
from main import Records, Models, Resourse
from main import session
from config import *


def report(day):
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
    df.to_excel(f'{output / (file_name + ".xlsx")}', index=False)


if __name__ == '__main__':
    day_report = '2021-05-16'
    report(day_report)
