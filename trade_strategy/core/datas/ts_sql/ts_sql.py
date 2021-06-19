# coding:utf-8
import pandas as pd

from core.datas.ts_sql.ts_daily import TsDaily
from core.datas.ts_sql.ts_trade_cal import TsTradeCal
from core.datas.ts_sql.ts_stock_basic import TsStockBasic
from core.datas.ts_sql.ts_industry import TsIndustry
from core.datas.ts_sql.ts_dividend import TsDividend


class TsSQL(TsDaily,
            TsTradeCal,
            TsStockBasic,
            TsIndustry,
            TsDividend):
    """api接口，从数据库中的各个表获取数据"""


ts_sql = TsSQL()



