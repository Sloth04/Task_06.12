import sys
import logging
from pathlib import Path
from logging import StreamHandler, Formatter

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = StreamHandler(stream=sys.stdout)
handler.setFormatter(Formatter(fmt='[%(asctime)s: %(levelname)s] %(message)s'))
logger.addHandler(handler)

DATABASE_NAME = 'Task_06_12.sqlite' # to config
result_columns = ['company_alias', 'y_code', 'service_type', 'w_code', 'datetime', 'direction']  # to config
df_n_columns = ['company_alias', 'y_code', 'service_type', 'w_code', 'date', 'time', 'direction', 'volume', 'payment']  # to config
y_code_dict = {'10Y1001C--000182': 'CA_UA_IPS', '10YUA-WEPS-----0': 'CA_UA_BEI'}  # to config
output = Path.cwd() / 'output'  # to config