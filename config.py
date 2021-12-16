import sys
import logging
from pathlib import Path
from logging import StreamHandler, Formatter

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = StreamHandler(stream=sys.stdout)
handler.setFormatter(Formatter(fmt='[%(asctime)s: %(levelname)s] %(message)s'))
logger.addHandler(handler)

output = Path.cwd() / 'output'

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
