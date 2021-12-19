import pandas as pd
from sqlalchemy import and_
import matplotlib.pyplot as plt
from insert import Records, Models, Resourse
from insert import session
from config import *


plt.rcParams["figure.figsize"] = (20, 10)


def report_station_day(station, day, type):
    file_name = 'report_for_'+station+'_'+day
    q = session.query(Models.service_type, Records.w_code, Records.datetime,
                      Models.direction, Records.volume, Records.payment)\
        .join(Models)\
        .join(Resourse)\
        .filter(and_(Models.service_type == type, Records.w_code == station, Records.datetime.like(f'{day}%')))
    df = pd.read_sql(q.statement, session.bind)
    logger.info(f'New dataframe, where day_report={day}, station_report={station}, {file_name} \n'
                f'{df.head().to_string()}')
    df.to_excel(f'{output / (file_name + ".xlsx")}', index=False)
    if df.empty:
        logger.info(f'DataFrame, where day_report={day}, station_report={station}, is empty!')
        return
    plt.title(f'Обсяг станції {station} за {day}')
    plt.ylabel('Обсяг - МВт')
    plt.xlabel('Мітка години')
    plt.grid()  # optional
    plt.plot(df['datetime'], df['volume'], 'g-')
    plt.xlim(df['datetime'].min(), df['datetime'].max())
    plt.ylim(df['volume'].min(), df['volume'].max())
    plt.show()
    # plt.savefig(f'{output / (file_name + ".png")}')  # cant save properly


if __name__ == '__main__':
    print(args.station, args.day, args.type)
    report_station_day(args.station, args.day, args.type)
