# coding:utf-8
import pandas as pd
import tushare as ts
import matplotlib.pyplot as plt

ts.set_token('54f042d586a61d1911fe7a8fd4f0e8324995eb37f157af0300e24cd2')
pro = ts.pro_api()

# data = pro.stock_basic(exchange='', list_status='L', fields='ts_code')
# print(data.ts_code.values)
# df = pro.daily(ts_code=','.join(data.ts_code.values), start_date='20180701', end_date='20180718')
# print(df)

# 输入参数
#
# 名称	类型	必选	描述
# ts_code	str	N	股票代码（支持多个股票同时提取，逗号分隔）
# trade_date	str	N	交易日期（YYYYMMDD）
# start_date	str	N	开始日期(YYYYMMDD)
# end_date	str	N	结束日期(YYYYMMDD)

df = pro.daily(ts_code='002120.SZ', start_date='20150101')
df.index = df.iloc[:, 1]
df.index.name = None
df = df.sort_values(by=["trade_date"], ascending=True)
df.index = pd.to_datetime(df.index, format='%Y-%m-%d')
data = pd.DataFrame(index=df.index, columns=['close', 'mean5', 'mean10', 'signal'])
data['close'] = df.close
data['mean5'] = data.close.rolling(5).mean()
data['mean10'] = data.close.rolling(10).mean()
data.mean5.iloc[0:9] = data.close.iloc[0:9]
data.mean10.iloc[0:9] = data.close.iloc[0:9]
plt.figure(figsize=(13, 6), dpi=80)
plt.plot(data.close)
plt.plot(data.mean5)    # 5日20日均线图
plt.plot(data.mean10)
plt.show()

# 输出参数
#
# 名称	类型	描述
# ts_code	str	股票代码
# trade_date	str	交易日期
# open	float	开盘价
# high	float	最高价
# low	float	最低价
# close	float	收盘价
# pre_close	float	昨收价
# change	float	涨跌额
# pct_chg	float	涨跌幅 （未复权，如果是复权请用 通用行情接口 ）
# vol	float	成交量 （手）
# amount	float	成交额 （千元）



# pyplot模块的plot函数可以接收输入参数和输出参数，还有线条粗细等参数，，例如下方的示例
squares = [1, 4, 9, 16, 25]
plt.plot(squares)  # 这里只指定了一个列表，那么就当作是输出参数，输入参数从0开始，就会发现没有正确绘制数据
plt.title("Square Numbers")  # 指定标题，并设置标题字体大小
plt.xlabel("Value")  # 指定X坐标轴的标签，并设置标签字体大小
plt.ylabel("Square of Value")  # 指定Y坐标轴的标签，并设置标签字体大小
plt.tick_params(axis='both', labelsize=14)  # 参数axis值为both，代表要设置横纵的刻度标记，标记大小为14
plt.show()  # 打开matplotlib查看器，并显示绘制的图形