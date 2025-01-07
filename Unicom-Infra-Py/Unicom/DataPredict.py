import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder, StandardScaler


names = ['当前时间', '网别', '年龄', '消费级别', '换机时间', '是否当前月换机']
data = pd.read_csv("F:/Dev/Unicom-Moniter/Unicom-Moniter/predict_dataset/part-m-00010", names=names)
# 特征工程
# 对分类变量进行编码
le_network = LabelEncoder()
data['网别'] = le_network.fit_transform(data['网别'])

# 计算换机间隔（这里简单计算距离上次换机的月数）
data['换机间隔'] = data['当前时间'].astype(int) - data['换机时间'].astype(int)

# 选择特征列和目标列
X = data[['网别', '年龄', '消费级别', '换机间隔']]
y = data['是否当前月换机']

# 数据划分
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# 特征缩放
scaler = StandardScaler()
X_train = scaler.fit_transform(X_train)
X_test = scaler.transform(X_test)

# 构建分类模型（这里使用随机森林作为示例）
clf = RandomForestClassifier(n_estimators=100)
clf.fit(X_train, y_train)

# 预测
y_pred = clf.predict(X_test)
# 输出预测结果和准确率（仅作为示例
countA = 0
countB = 0
# for i in range(len(y_pred)):
#     if y_pred[i] == 1:
#         if 1 == y_train.values[i]:
#             countA+=1
#     else :
#         if 0 == y_train.values[i]:
#             countB+=1
# # 模型评估
# print("换手机预测成功:" , countA)
# print("不手机预测成功:" , countB)
# print("总预测数据:" , len(y_pred))
accuracy = accuracy_score(y_test, y_pred)
print(f"模型准确率: {accuracy}")
