# py 只做一些简单数据分析
import os
import re
import pandas as pd
from pandas import DataFrame

from DataPersistence import get_dict_data


# ​		手机品牌销量占比
# ​		年龄分布
# ​		性别占比
# ​		网段占比和趋势

class DataAnalysis:
    percentage_map = {'网别': {'a': 25},
                      '性别': {'a': 25},
                      '年龄值段': {'a': 25},
                      '终端品牌': {'a': 25},

                      }
    a = 0
    b = 0
    c = 0
    d = 0
    net_month_data = {1: {}}

    def __init__(self):
        self._column_names = ['月份', 'IMSI', '网别',
                              '性别', '年龄值段', 'ARPU值段',
                              '终端品牌', '终端型号', '流量使用量', '语音通话时长',
                              '短信条数']
        self._dtypes = {  # 默认为object 也就是str
            '月份': 'int32',
            'IMSI': 'str',
            '网别': 'str',
            '性别': 'str',
            '年龄值段': 'int32',
            'ARPU值段': 'int32',
            '终端品牌': 'str',
            '终端型号': 'str',
            '流量使用量': 'int32',
            '语音通话时长': 'int32', '短信条数': 'int32', }
        self._destDir = self.current_working_directory + "\\mysql-export"
        dict_data = get_dict_data()
        #  return traffic_weight, ARPU_weight, age_weight
        self.age_weight = {value: key for key, value in dict_data[2].items()}
        self.traffic_weight = {value: key for key, value in dict_data[0].items()}
        self.ARPU_weight = {value: key for key, value in dict_data[1].items()}

    def package(self, df: DataFrame):
        print(df['语音通话时长'].max())
        print(df['短信条数'].max())
        self.get_sex_percentage(df)
        self.get_age_percentage(df)
        self.get_phone_brand_percentage(df)
        self.get_net_percentage(df)

    def get_sex_percentage(self, df: DataFrame):
        sum_rows = df.shape[0]
        male_count = df[df['性别'] == '男'].shape[0]
        self.percentage_map['性别']['男'] = int((male_count / sum_rows) * 100)
        self.percentage_map['性别']['女'] = 100 - self.percentage_map['性别']['男']
        del self.percentage_map['性别']['a']

    def get_age_percentage(self, df: DataFrame):
        tmp_map = df['年龄值段'].value_counts().to_dict()
        for k, v in tmp_map.items():
            self.percentage_map['年龄值段'][self.age_weight[k]] = v
        del self.percentage_map['年龄值段']['a']

    def get_phone_brand_percentage(self, df: DataFrame):
        tmp_map = df['终端品牌'].value_counts().head(10).to_dict()

        for k, v in tmp_map.items():
            self.percentage_map['终端品牌'][k] = v
        del self.percentage_map['终端品牌']['a']

    def get_net_percentage(self, df: DataFrame):
        sum_rows = df.shape[0]
        net_count = df[df['网别'] == '3G'].shape[0]
        self.percentage_map['网别']['3G'] = int((net_count / sum_rows) * 100)
        self.percentage_map['网别']['2G'] = 100 - self.percentage_map['网别']['3G']
        del self.percentage_map['网别']['a']

    def get_monthly_net_percentage(self, df: DataFrame, year_month: int):
        # self.a += df['流量使用量'].mean()
        self.a += df['流量使用量'].mode()[0]
        # self.b += df['语音通话时长'].mean()
        self.b += df['语音通话时长'].mode()[0]
        self.c += df['ARPU值段'].mean()
        self.d += df['短信条数'].mean()

        sum_rows = df.shape[0]
        net_count = df[df['网别'] == '3G'].shape[0]
        if year_month not in self.net_month_data:
            self.net_month_data.setdefault(year_month, {})

        self.net_month_data[year_month]['3G'] = int((net_count / sum_rows) * 100)
        self.net_month_data[year_month]['2G'] = 100 - self.net_month_data[year_month]['3G']

    @staticmethod
    def get_project_root(parent='.idea'):
        """获取项目根目录，假设根目录下存在 marker 文件或文件夹"""
        current_dir = os.path.dirname(os.path.abspath(__file__))
        while True:
            if os.path.exists(os.path.join(current_dir, parent)):
                return current_dir
            parent_dir = os.path.dirname(current_dir)
            if parent_dir == current_dir:
                raise FileNotFoundError(f"未找到项目根目录（标记为 {parent}）")
            current_dir = parent_dir

    #
    @property
    def current_working_directory(self):
        return self.get_project_root()

    @property
    def destDir(self):
        return self._destDir

    @property
    def column_names(self):
        return self._column_names

    @property
    def dtypes(self):
        return self._dtypes


if __name__ == '__main__':
    # F:\Dev\Unicom-Moniter\Unicom-Moniter\mysql-export\part-r-00001\part-r-00000
    analysis = DataAnalysis()
    pattern = re.compile(r'^part-r-000(0[1-9]|1[0-2])$')
    count = 0
    print(analysis.destDir)
    for filename in os.listdir(analysis.destDir):
        if pattern.match(filename):
            print(analysis.destDir + "\\" + filename)
            count += 1
            df = pd.read_csv(analysis.destDir + "\\" + filename, sep=',',
                             encoding='utf-8', names=analysis.column_names, dtype=analysis.dtypes,
                             on_bad_lines="skip")
            if count == 1:
                analysis.package(df)
            analysis.get_monthly_net_percentage(df, int(filename[-2:]))
    print(analysis.percentage_map)
    print(analysis.net_month_data)
    print(analysis.a / 12)
    print(analysis.b / 12)
    print(analysis.c / 12)
    print(analysis.d / 12)

# 数据挖掘
#     网频的不同 --- arpu 怎么样？ 流量使用量 ?  ..
#
