import numpy as np
import pandas as pd
import sys
import logging
from pathlib import Path
from logging import StreamHandler

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = StreamHandler(stream=sys.stdout)
logger.addHandler(handler)

symmetric_dict = {'FCR': 'РПЧ', 'aFRR': 'аРВЧ', 'mFRR': 'рРВЧ'}


# def manipulation(df, time_item, name, direction):
#     res = pd.DataFrame()
#     for st_type in symmetric_dict.keys():
#         temp = df[(df['Market Participant'] == name)
#                   & (df['Service Type'] == st_type)
#                   & (df.index == time_item)
#                   & (df['Direction'] == direction)]
#         result = temp.groupby(pd.Grouper(freq='H')).sum()
#         res.append(result)
#     return res


def select(df):
    df_new = pd.DataFrame()
    # for item in df_new.index.unique():
    #     for name in np.sort(df['Market Participant'].unique()):
    #         for direction in df['Direction'].unique():
    #             df_new.append(manipulation(df, item, name, direction))
    df_new = df.groupby(['Market Participant', 'Service Type', 'Date', 'Settlement Period', 'Direction'], as_index=False)[['Quantity (MWh)', 'Ancillary Service Payment (₴)']].sum()
    print(df_new)
    return df_new


def create_df(path):
    df = pd.read_excel(path, sheet_name='Sheet1')
    df['Datetime'] = df.apply(lambda x: str(x['Date'])[:10] + ' ' + x['Settlement Period'], axis=1)
    df.index = df['Datetime']
    df.index = pd.to_datetime(df.index.str[:-6], format='%Y-%m-%d %H:%M')
    df = df.drop(columns=['Datetime'])
    print(df)
    return df


def main():
    num = 0
    dir_list = Path.cwd().rglob('input*.xlsx')
    output = Path.cwd() / 'output'
    output.mkdir(parents=True, exist_ok=True)
    for item in dir_list:
        df = create_df(item)
        df = select(df)
        num += 1
        df.to_excel(f'{output}\\result{num}.xlsx')


if __name__ == '__main__':
    main()
