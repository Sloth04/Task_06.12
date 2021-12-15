import pandas as pd
from model_db import Records, Resourse, Models
from model_db import Session
from config import *

session = Session()


def get_or_create(session, model, **kwargs):
    instance = session.query(model).filter_by(**kwargs).first()
    if instance:
        return instance, False
    else:
        instance = model(**kwargs)
        session.add(instance)
        session.commit()
        return instance, True


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
    df['datetime'] = pd.to_datetime(df['date'].dt.strftime('%Y-%m-%d') + ' ' + df['time'].str[:2] + ':00')
    logger.debug(f'Dataframe from file {path} after def create_df\n{df} ')
    return df


def fill_db(df):
    for index, row in df.iterrows():
        r, r_created = get_or_create(session, Resourse, company_alias=row['company_alias'], w_code=row['w_code'], y_code=row['y_code'])
        m, m_created = get_or_create(session, Models, service_type=row['service_type'], direction=row['direction'])
        # check flag
        incoming_record = Records(w_code=row['w_code'], id_resourse=r.id, id_type_direction=m.id, datetime=row['datetime'],
                                  volume=row['volume'], payment=row['payment'])
        session.add(incoming_record)
    session.commit()


def main():
    dir_list = list(Path.cwd().rglob('input*.xlsx'))
    num = 0
    logger.info(f'Founded {len(dir_list)} file/-s:{dir_list}')
    output.mkdir(parents=True, exist_ok=True)
    for item in dir_list:
        df = create_df(item)
        df = grouper(df)
        num += 1  # swap to enumerate
        file_name = f'result{num}'
        fill_db(df)


if __name__ == '__main__':
    main()


