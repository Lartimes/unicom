import codecs
import os
import DataPersistence
import numpy as np
import pandas as pd
import pymysql
from pandas import DataFrame


class DataPreSolve:
    def __init__(self):
        self._datalist = []
        self._computed = False
        self._clean_map = {'key1': {'subkey1': 'value1', 'subkey2': 'value2'},
                           'key2': {'subkey1': 'value3', 'subkey2': 'value4'}}
        self._dtypes = {  # 默认为object 也就是str
            # '月份': 'str',
            # 'IMSI': 'str',
            # '网别': 'str',
            # '性别': 'str',
            # '年龄值段': 'str',
            # 'ARPU值段': 'str',
            # '终端品牌': 'str',
            # '终端型号': 'str',
            # '流量使用量': 'str',
            '语音通话时长': 'int32', '短信条数': 'int32', }

    @property
    def header_num(self):
        return 3

    @property
    def solve_num(self):
        return len(self.datalist)

    @property
    def datalist(self):
        if not self._computed:
            for root, dirs, fs in os.walk(os.path.dirname(__file__)):
                if len(dirs) > 0:
                    for datadir in dirs:
                        if datadir == 'rawdata':
                            for r, d, files in os.walk(os.path.dirname(__file__) + "\\" + datadir):
                                for datafile in files:
                                    self._datalist.append(r + "\\" + datafile)
            self._computed = True
            return self._datalist
        else:
            return self._datalist

    def code_transform(self):
        print("进行转码")
        print(self.datalist)
        prefix = 'utf-8'
        count = 0
        for data_file in self.datalist:
            file = data_file[:-len('数据大赛20150x.csv'):] + prefix + data_file[-len('数据大赛201501.csv'):]
            try:
                with (codecs.open(data_file, 'r', encoding='GBK') as infile, codecs.open(file, 'w',
                                                                                         encoding='utf-8') as outfile):
                    for line in infile:
                        outfile.write(line)
                        outfile.flush()
                count += 1
            except:
                os.remove(file)
                pass
        if count == 0:
            return
        print(self.datalist)
        for data_file in self.datalist:
            file = data_file[:-len('数据大赛20150x.csv'):] + prefix + data_file[-len('数据大赛201501.csv'):]
            try:
                with open(file, 'r', encoding='utf-8') as infile, open(data_file, 'w', encoding='utf-8') as outfile:
                    for line in infile:
                        outfile.write(line)
                        outfile.flush()
            except:
                pass
            finally:
                os.remove(file)
        print("转码成功")

    def get_next_imsi(self, begin: int, ismi: str):
        for i in range(begin, self.solve_num):
            df = pd.read_csv(self.datalist[i], encoding='utf-8', dtype=self._dtypes)
            row_data = df[df['IMSI'] == ismi]
            if not row_data.empty:
                return row_data.iloc[0].to_dict()
            print("==========")
            print("读取其他数据")
        return None

    def get_total_imsi(self, ismi: str):
        ret = []
        for i in range(0, self.solve_num):
            df = pd.read_csv(self.datalist[i], encoding='utf-8', dtype=self._dtypes)
            row_data = df[df['IMSI'] == ismi]
            if not row_data.empty:
                ret.append(row_data.iloc[0].to_dict())
        return ret

    def data_pre_solve(self):
        def fill_max_mean(row, clean_key: str):
            clean = getattr(row, 'IMSI')
            arr = self.get_total_imsi(clean)
            hashmap = dict()
            rawdata = getattr(row, clean_key)
            if clean_key == '流量使用量':

                for val in arr:
                    try:
                        hashmap[val[clean_key]] += 1
                    except:
                        pass
                if len(hashmap) == 0:
                    return rawdata
                return max(hashmap, key=hashmap.get)
            elif clean_key == 'ARPU值段':
                for val in arr:
                    try:
                        hashmap[val[clean_key]] += 1
                    except:
                        pass
                if len(hashmap) == 0:
                    return rawdata
                return max(hashmap, key=hashmap.get)
            else:
                tmp = 0
                for val in arr:
                    tmp += val[clean_key]
                if tmp == 0:
                    return 0
                return tmp / len(arr)

        def clean_package(row, clean_key: str):
            clean = getattr(row, 'IMSI')
            if self._clean_map.get(clean) is not None and self._clean_map[clean].get(clean_key) is not None:
                return self._clean_map[clean][clean_key]
            self._clean_map.setdefault(clean, {})
            result = self.get_next_imsi(i + 1, clean)
            if result is not None:
                for key in self._dtypes.keys():
                    if self._clean_map[clean].get(key) is None:
                        self._clean_map[clean][key] = result[key]
                self._clean_map[clean][clean_key] = result[clean_key]
                return result[clean_key]
            return row[clean_key]

        tmpDir = "tmp"
        for i in range(self.solve_num):
            chunksize = 10 ** 4 * 2  # 每块的大小
            chunks = pd.read_csv(self.datalist[i], encoding='utf-8', dtype=self._dtypes, chunksize=chunksize)
            merge_size = 0
            if not chunks:
                continue
            for df in chunks:
                # print(df)
                df.drop_duplicates(inplace=True)
                df.dropna(subset=['IMSI'], inplace=True)
                df['网别'].fillna('3G', inplace=True)
                df['性别'].fillna('男', inplace=True)
                df['终端型号'] = df['终端型号'].str.replace(' ', '').str.replace(',', '|')

                # map存入 修改过的数据 ，迭代下去，不会回滚再次向前读取
                # for row in df.itertuples(index=True):
                #     df.at[row.Index, '性别'] = clean_package(row, '性别') if str(
                #         getattr(row, '性别')) == '不详' or pd.isna(getattr(row, '性别')) else getattr(row, '性别')
                #     #  print("性别clean success")
                #     # df['性别'] = df.swifter.apply(
                #     #     lambda row: clean_package(row, '性别') if row['性别'] == '不详' else row['性别'], axis=1)
                #
                #     df.at[row.Index, '年龄值段'] = clean_package(row, '年龄值段') if pd.isna(
                #         getattr(row, '年龄值段')) or str(getattr(row, '年龄值段')) == '未知' else getattr(row,
                #                                                                                           '年龄值段')
                #
                #     df.at[row.Index, '终端品牌'] = clean_package(row, '终端品牌') if pd.isna(
                #         getattr(row, '终端品牌')) else getattr(row, '终端品牌')
                #
                #     df.at[row.Index, '终端型号'] = clean_package(row, '终端型号') if pd.isna(
                #         getattr(row, '终端型号')) else getattr(row, '终端型号')
                #
                #     df.at[row.Index, 'ARPU值段'] = fill_max_mean(row, 'ARPU值段') if pd.isna(
                #         getattr(row, 'ARPU值段')) else getattr(row, 'ARPU值段')
                #
                #     df.at[row.Index, '流量使用量'] = fill_max_mean(row, '流量使用量') if pd.isna(
                #         getattr(row, '流量使用量')) else getattr(row, '流量使用量')
                #
                #     df.at[row.Index, '语音通话时长'] = fill_max_mean(row, '语音通话时长') if pd.isna(
                #         getattr(row, '语音通话时长')) else getattr(row, '语音通话时长')
                #
                #     df.at[row.Index, '短信条数'] = fill_max_mean(row, '短信条数') if pd.isna(
                #         getattr(row, '短信条数')) else getattr(row, '短信条数')
                df = df[df['年龄值段'] != '未知']
                copy_df = df.copy()
                merge_size += 1
                create_temp(tmpDir, copy_df, str(merge_size))
                DataPersistence.data_batch_insert(str(self.datalist[i])[-len("yyyymm.csv"): -4], copy_df)
            #       overwrite
            print("========")
            if os.path.exists(self.datalist[i]):
                print(self.datalist)
                print("merge_csvs(tmpDir , merge_size , self.datalist[i])")
                merge_csvs(tmpDir, merge_size, self.datalist[i])
                # TODO 上传至HDFS 从mysql 导出到sqoop,

    def get_data_dic(self):
        # 流量使用量
        # ARPU值段
        # 年龄值段
        dict_data = {'流量使用量': set(), 'ARPU值段': set(), '年龄值段': set()}
        for i in range(self.header_num):
            df = pd.read_csv(self.datalist[i], encoding='utf-8', dtype=self._dtypes)
            for row in df.itertuples(index=True):
                for key in dict_data.keys():
                    dict_data[key].add(getattr(row, key))

        dict_data['流量使用量'].remove(np.nan)
        dict_data['ARPU值段'].remove(np.nan)
        dict_data['年龄值段'].remove('未知')
        return dict_data


def create_temp(tmpDir: str, df: DataFrame, sequence: str):
    tmp = os.path.dirname(__file__) + "\\" + tmpDir
    os.makedirs(tmp, mode=0o777, exist_ok=True)
    temp_filename = os.path.join(tmp, sequence + ".csv")
    print(temp_filename)
    df.to_csv(temp_filename, index=False, encoding='utf-8-sig')


def merge_csvs(tmpDir: str, merge_size: int, dest_path: str):
    print("========")
    tmpDir = os.path.dirname(__file__) + "\\" + tmpDir
    with open(dest_path, 'w', encoding='utf-8') as dest:
        for i in range(1, merge_size + 1):
            with open(tmpDir + "\\" + str(i) + ".csv", 'r', encoding='utf-8') as file:
                tmp = 0
                for line in file:
                    tmp += 1
                    dest.write(line)
                    if tmp % 5000 == 0:
                        dest.flush()
                dest.flush()


def insert_mysql(dict_data):
    # 创建连接
    conn = pymysql.connect(host='localhost', user='root', password='307314', database='unicom')
    cursor = conn.cursor()
    sql = "INSERT INTO dictionary (id , dictionary.column , weight , dictionary.range) VALUES (null , %s ,null , %s )"
    data = [(k, v) for k, vs in dict_data.items() for v in vs]
    cursor.executemany(sql, data)
    conn.commit()
    cursor.close()
    conn.close()


if __name__ == '__main__':
    dps = DataPreSolve()
    dps.code_transform()
    # dict_data = dps.get_data_dic()
    # insert_mysql(dict_data)
    dps.data_pre_solve()
