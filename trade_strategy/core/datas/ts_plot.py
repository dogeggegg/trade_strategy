# coding:utf-8
import pandas as pd
import matplotlib.pyplot as plt
from pylab import mpl

mpl.rcParams['font.sans-serif'] = ['SimHei']
# mpl.rcParams['font.sans-serif'] = ['Microsoft YaHei'] # 指定默认字体：解决plot不能显示中文问题
mpl.rcParams['axes.unicode_minus'] = False      # 解决保存图像是负号'-'显示为方块的问题


class TsPlot:

    @staticmethod
    def show_line(*args, x_label=None):
        """
        多条单线画在一个画布里
        :param args:        多条线
        :param x_label:     横坐标
        :return:
        """
        df = pd.DataFrame()
        for data in args:
            if isinstance(data, pd.DataFrame):
                for colname in data.columns:
                    df[colname] = data[colname].values
            elif isinstance(data, pd.Series):
                df[data.name] = data
        df.plot(x=x_label)
        plt.show()

    @staticmethod
    def show_twinx_plot(df_1, df_2):
        fig, ax1 = plt.subplots()
        ax2 = ax1.twinx()   # 让2个子图的x轴一样，同时创建副坐标轴。

        ax1.plot(df_1['x'], df_1['y'], color='red')
        ax2.plot(df_2['x'], df_2['y'], color='green')

        plt.show()


ts_plot = TsPlot()
