# coding:utf-8
import matplotlib.pyplot as plt


class Plot:

    def show_plot(self, ):
        plt.figure(figsize=(13, 6), dpi=80)
        plt.plot(data.close)
        # plt.plot(data.mean5)    # 5日20日均线图
        # plt.plot(data.mean10)
        plt.show()
