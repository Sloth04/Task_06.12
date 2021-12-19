import sys
import logging
from pathlib import Path
from logging import StreamHandler, Formatter
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('-st', '--station',
                    help='enter station',
                    action='store',
                    default='62W001')
parser.add_argument('-d', '--day',
                    help='enter day, format YYYY-MM-DD',
                    action='store',
                    default='2021-05-16')
parser.add_argument('-t', '--type',
                    help='enter type, FCR/aFRR/mFRR',
                    action='store',
                    default='FCR')
# ask about store_true
args = parser.parse_args()

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = StreamHandler(stream=sys.stdout)
handler.setFormatter(Formatter(fmt='[%(asctime)s: %(levelname)s] %(message)s'))
logger.addHandler(handler)

output = Path.cwd() / 'output'
output.mkdir(parents=True, exist_ok=True)

DATABASE_NAME = 'Task_06_12.sqlite'

result_columns = [
    'company_alias',
    'y_code',
    'service_type',
    'w_code',
    'datetime',
    'direction']
df_n_columns = [
    'company_alias',
    'y_code',
    'service_type',
    'w_code',
    'date',
    'time',
    'direction',
    'volume',
    'payment']
y_code_dict = {
    '10Y1001C--000182': 'CA_UA_IPS',
    '10YUA-WEPS-----0': 'CA_UA_BEI'}

service_type_dict = {
    'FCR': 'РПЧ',
    'aFRR': 'аРВЧ',
    'mFRR': 'рРВЧ'}
direction_dict = {
    'Symmetric': 'Симетрично',
    'Up': 'Вгору',
    'Down': 'Вниз'}
