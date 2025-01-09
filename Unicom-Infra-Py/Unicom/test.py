import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder, StandardScaler

names = ['当前时间', '网别', '年龄', '消费级别', '换机时间', '是否换机']
data = pd.read_csv("F:/Dev/Unicom-Moniter/Unicom-Moniter/predict_dataset/part-m-00010", names=names)
# 先根据换机时间排序数据
data = data.sort_values(by='换机时间')

# 特征工程
# 对分类变量'网别'进行编码
le = LabelEncoder()
data['网别'] = le.fit_transform(data['网别'])

# 计算距离上次换机的时间间隔
data['换机间隔'] = (data['当前时间'] - data['换机时间']).fillna(0).astype(int)

# 选择特征列和目标列
X = data[['网别', '年龄', '消费级别', '换机间隔']]
y = data['是否换机']

# 数据划分
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# 特征缩放
scaler = StandardScaler()
X_train = scaler.fit_transform(X_train)
X_test = scaler.transform(X_test)

# 构建随机森林模型
clf = RandomForestClassifier(n_estimators=100)
clf.fit(X_train, y_train)

# 预测
y_pred = clf.predict(X_test)

# 模型评估
# 模型准确率: 0.9996884579916756
# DataPredict:  模型准确率: 0.9996635346310097
accuracy = accuracy_score(y_test, y_pred)
print(f"模型准确率: {accuracy}")
