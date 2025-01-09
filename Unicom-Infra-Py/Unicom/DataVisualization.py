# 画出 arpu x 轴，流量使用量、语音通话时长和短信条数 三个y轴
import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
from pandas import DataFrame
from Unicom.DataAnalysis import DataAnalysis

from scipy.stats import pearsonr
from pylab import mpl
# 设置显示中文字体
mpl.rcParams["font.sans-serif"] = ["SimHei"]

# ['月份', 'IMSI', '网别',
#                               '性别', '年龄值段', 'ARPU值段',
#                               '终端品牌', '终端型号', '流量使用量', '语音通话时长',
#                               '短信条数']
# 201501,decf8f2fffff42b4746b6bd02d10dd23,2G,男,6,1,None,None,1,0,0
# 201501,decf8f2fffff42b4746b6bd02d10dd23,2G,男, 6 , 1 ,None,None,1,0,0
# 可以看到arpu高 通话时间/越来越高
# 这两个可以进行挖掘是否存在关系
# TODO : 聚类分析或者 JAVABean Redis MapReduce 对这些用户实体分析
def draw_arpu_scatter(df: DataFrame):
    fig, ax1 = plt.subplots(figsize=(8, 6))
    color1 = 'tab:blue'
    ax1.set_xlabel('ARPU值段')
    ax1.set_ylabel('流量使用量', color=color1)
    min = df['流量使用量'].min()













































    max = df['流量使用量'].max()
    print(max)
    ax1.set_ylim(min, max)
    ax1.scatter(df['ARPU值段'], df['流量使用量'], color=color1)
    ax1.tick_params(axis='y', labelcolor=color1)

    min = df['语音通话时长'].min()
    max = df['语音通话时长'].max()
    print(max)
    ax2 = ax1.twinx()
    ax2.set_ylim(min, max)
    color2 = 'tab:red'
    ax2.set_ylabel('语音通话时长', color=color2)
    ax2.scatter(df['ARPU值段'], df['语音通话时长'], color=color2)
    ax2.tick_params(axis='y', labelcolor=color2)
    ax3 = ax1.twinx()
    color3 = 'tab:green'
    ax3.spines['right'].set_position(('outward', 60))
    ax3.set_ylabel('短信条数', color=color3)
    min = df['短信条数'].min()
    max = df['短信条数'].max()
    print(max)
    ax3.set_ylim(min, max)
    ax3.scatter(df['ARPU值段'], df['短信条数'], color=color3)
    ax3.tick_params(axis='y', labelcolor=color3)
    fig.tight_layout()
    plt.show()
    correlation, p_value = pearsonr(df['ARPU值段'], df['语音通话时长'])
    print(f"Pearson相关系数: {correlation}")
    # Pearson相关系数: 0.5915579685655854



if __name__ == '__main__':
    a = DataAnalysis()
    df = pd.read_csv("F:/Dev/Unicom-Moniter/Unicom-Moniter/mysql-export/part-r-00001/", sep=',',
                     encoding='utf-8', names=a.column_names, dtype=a.dtypes,
                     on_bad_lines="skip")
    draw_arpu_scatter(df)
