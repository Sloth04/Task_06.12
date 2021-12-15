import pandas as pd
from main import Records, Models, Resourse
from main import session
from main import *

day_report = '2021-05-16'
service_type_dict = {'FCR': 'РПЧ', 'aFRR': 'аРВЧ', 'mFRR': 'рРВЧ'}
direction_dict = {'Symmetric': 'Симетрично', 'Up': 'Вгору', 'Down': 'Вниз'}


def report():
    file_name = 'report_for_'+day_report
    q = session.query(Resourse.company_alias, Resourse.y_code, Models.service_type, Records.w_code, Records.datetime,
                      Models.direction, Records.volume, Records.payment)\
        .join(Models)\
        .join(Resourse)\
        .filter(Records.datetime.like(f'{day_report}%'))
    df = pd.read_sql(q.statement, session.bind)
    df['service_type'] = df['service_type'].replace(service_type_dict)
    df['direction'] = df['direction'].replace(direction_dict)
    df.to_excel(f'{output / (file_name + ".xlsx")}', index=False)
