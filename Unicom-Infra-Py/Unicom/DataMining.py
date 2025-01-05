import pandas as pd
from pandas import DataFrame
from sklearn.cluster import KMeans
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

from Unicom.DataAnalysis import DataAnalysis


class DataMining:
    # 挖掘
    # 2. 换机用户{年龄太小 / 年龄大过滤掉，
    #               除非经常换 进行集合运算1高消费群体}  流量怎么变化 ？ arpu 怎么变化？
    # 3.用户业务量随时间变化
    # 4.用户换机时间预测？

    # ['月份', 'IMSI', '网别',
    #                               '性别', '年龄值段', 'ARPU值段',
    #                               '终端品牌', '终端型号', '流量使用量', '语音通话时长',
    #                               '短信条数']
    # 5.网别为3G的 消费多一点 不做了... 类似
    def net_consume_diff(self, df: DataFrame):
        # 0 1 2
        sum_rows = df.shape[0]
        print(df.dtypes)
        g2_count = df[(df['网别'] == '2G') & (df['Cluster'] == 0)].shape[0]
        g3_count = df[(df['网别'] == '3G') & (df['Cluster'] == 0)].shape[0]
        print("总用户:", sum_rows)
        print(df['Cluster'])
        print("2g 中高消费用户群体NUM :", g2_count)
        print("3g 中高消费用户群体NUM :", g3_count)

    # 聚类分析 给出关键特征
    # 消费业务量分出用户群体
    def groupBy_features(self, data: DataFrame):
        # 提取特征列
        features = data[['ARPU值段', '流量使用量', '语音通话时长', '短信条数']]
        # 数据标准化
        scaler = StandardScaler()
        scaled_features = scaler.fit_transform(features)
        # 确定K值（这里假设通过某种方法确定为3）
        # 高消费 中消费 低消费 0 1 2
        k = 3
        # 创建K - Means模型并拟合数据
        kmeans = KMeans(n_clusters=k)
        kmeans.fit(scaled_features)
        # 获取聚类标签
        labels = kmeans.labels_
        # 将聚类标签添加到原始数据中
        data['Cluster'] = labels
        data.to_csv('data.csv', index=False, encoding='utf-8')
        return data

    # 1. ARPU高的 一般 流量使用量高 /语音通话 / 信息条数 多
    def arpu_call_sum_liner(self, df: DataFrame):
        Q1 = df['语音通话时长'].quantile(0.25)
        Q3 = df['语音通话时长'].quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        df_filtered = df[(df['语音通话时长'] >= lower_bound) & (df['语音通话时长'] <= upper_bound)]
        X = df_filtered[['ARPU值段']]  # 特征
        y = df_filtered['语音通话时长']
        # 从训练集中随机取5行数据
        new_data = df_filtered.sample(n=5)
        # 划分数据集
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        # 创建并训练模型
        model = LinearRegression()
        model.fit(X_train, y_train)
        # 预测
        y_pred = model.predict(X_test)
        # 评估模型
        mse = mean_squared_error(y_test, y_pred)
        print(f"均方误差: {mse}")
        print(new_data['ARPU值段'])
        print(new_data['语音通话时长'])
        new_data = new_data[['ARPU值段']]
        predicted_business_volume = model.predict(new_data)
        print("预测的业务量: ", predicted_business_volume)


if __name__ == '__main__':
    mining = DataMining()
    a = DataAnalysis()
    df = pd.read_csv("F:/Dev/Unicom-Moniter/Unicom-Moniter/mysql-export/part-r-00001/part-r-00000", sep=',',
                     encoding='utf-8', names=a.column_names, dtype=a.dtypes,
                     on_bad_lines="skip")
    df = df.dropna(subset=['终端品牌'])
    # mining.arpu_call_sum_liner(df)
    df = mining.groupBy_features(df)
    mining.net_consume_diff(df)
