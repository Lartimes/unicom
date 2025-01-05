from datetime import datetime

import numpy as np
import pandas as pd
import pymysql

database_str = """
         create table if not exists unicom_{}
(
    id             bigint auto_increment,
    time_now       datetime     null,
    imsi           varchar(255) null,
    net            char(3)      null,
    sex            char(2)      null default '男',
    age_weight     int          null default 0,
    arpu           int          null default 0,
    brand          varchar(255) null,
    model          varchar(255) null default 0,
    traffic_weight int          null default 0,
    call_sum  int          null default 0,
    sms_total     int          null default 0,
    constraint unicom_{}_pk
        primary key (id)
)
    comment '联通用户月表';
        """
traffic_weight = {'0': 0}
ARPU_weight = {'0': 0}
age_weight = {'0': 0}


def get_dict_data():
    connection = pymysql.connect(host='localhost', user='root', password='307314', db='unicom', charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)
    with connection.cursor() as cursor:
        cursor.execute("""select * from dictionary""")
        while True:
            row = cursor.fetchone()
            if row is None:
                break
            if row['column'] == '流量使用量':
                traffic_weight[row['range']] = row['weight']
            elif row['column'] == 'ARPU值段':
                ARPU_weight[row['range']] = row['weight']
            elif row['column'] == '年龄值段':
                age_weight[row['range']] = row['weight']
        connection.close()
    return traffic_weight, ARPU_weight, age_weight



def data_batch_insert(table: str, df: pd.DataFrame):
    get_dict_data()
    connection = pymysql.connect(host='localhost', user='root', password='307314', db='unicom',
                                 cursorclass=pymysql.cursors.DictCursor)
    with connection.cursor() as cursor:
        cursor.execute(database_str.format(table, table))
        localdate = datetime.strptime(table, "%Y%m")
        # (datetime.datetime(2015, 1, 1, 0, 0), 'd8ccc2441daabc76628b8ce9ffc9446e', '3G', '男', 5, 2, 'Xiaomi', 'MI 2013029', 1, 377, 0)
        sql = "INSERT INTO unicom_" + table + "  VALUES (null, %s , %s , %s ,%s ,%s , %s  ,%s,%s ,%s , %s ,%s)"
        print( len(df))
        df['流量使用量'].fillna('0', inplace=True)
        df['ARPU值段'].fillna('0', inplace=True)
        df['年龄值段'].fillna('0', inplace=True)
        try:
            df['流量使用量'] = df['流量使用量'].map(traffic_weight)
            df['ARPU值段'] = df['ARPU值段'].map(ARPU_weight)
            df['年龄值段'] = df['年龄值段'].map(age_weight)
        except:
            pass
        finally:
            df['年龄值段'] = df['年龄值段'].astype(dtype=np.int32)
            df['流量使用量'] = df['流量使用量'].astype(dtype=np.int32)
            df['ARPU值段'] = df['ARPU值段'].astype(dtype=np.int32)
        df = df.fillna(value='None')
        data_to_insert = [(localdate,) + tuple(x)[1:] for x in df.to_numpy()]
        cursor.executemany(sql, data_to_insert)
        connection.commit()
        print("commit")
        connection.close()  # 20000 1mb最大插入这么多条




# 查询手机型号  品牌等 。。。。
# 手机型号, 分析的时候再说吧
if __name__ == '__main__':
    get_dict_data()



