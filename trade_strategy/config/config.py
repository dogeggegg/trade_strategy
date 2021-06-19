import os
import datetime
from dateutil.relativedelta import relativedelta


class Config(object):

    RootPath = os.path.dirname(os.path.dirname(__file__))
    ResultPath = os.path.join(os.path.dirname(RootPath), 'result')
    DateTime = datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
    ResultDatetimePath = os.path.join(ResultPath, DateTime)
    if not os.path.exists(ResultDatetimePath):
        os.makedirs(ResultDatetimePath, exist_ok=True)
    LogFilePath = os.path.join(ResultDatetimePath, 'log.log')


class UserConfig(object):

    Start = '1980-01-01'                # 回测起始时间, '1980-01-01', 默认3年前
    End = None                  # 回测结束时间, '2020-01-01', 默认今天
    CapitalBase = 100000        # 初始仓位
    CommissionRate = 6/10000    # 佣金率，一般万六
    HoldingSharesNumber = 10    # 持仓个股数量，一共买几只股，默认 10只

    if not Start:
        Start = (datetime.datetime.now() - relativedelta(years=3)).strftime('%Y-%m-%d')
    if not End:
        End = datetime.datetime.now().strftime('%Y-%m-%d')


if __name__ == '__main__':
    print(Config.RootPath)
    print(Config.ResultPath)
