# coding:utf-8
import traceback


from config.config import Config
from logger.logger import logger, printe
from core.strategy.strategy import Strategy
from core.backtest.backtest import backtest
from core.datas.ts_datas import ts_datas


logger.add_file_handler(Config.LogFilePath)
try:
    strategy = Strategy()
    backtest.run_backtest(strategy)
    ts_datas.windup()
except Exception:
    printe(traceback.format_exc())

