# coding:utf-8

import pandas as pd

from core.datas.ts_requests.from_tushare import FromTushare
from core.datas.ts_requests.from_baostock import FromBaostock
from logger.logger import *


pd.pandas.set_option("display.width", 500)              # 数据显示区域的总宽度，以总字符数计算。
pd.pandas.set_option("display.max_rows", None)          # 最大显示行数，超过该值用省略号代替，为None时显示所有行。
pd.pandas.set_option("display.max_columns", None)       # 最大显示列数，超过该值用省略号代替，为None时显示所有列。
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)     # 使列名对齐
pd.set_option('display.width', 100)                         # 设置横向最多显示的字符
pd.set_option('expand_frame_repr', False)                   # 数据超过总宽度后，是否折叠显示
pd.set_option('colheader_justify', 'right')


class TsRequests(FromBaostock, FromTushare):
    """api接口，优先获取 Baostock，补充获取 tushare"""


ts_requests = TsRequests()



