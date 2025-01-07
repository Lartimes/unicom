import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sympy.physics.units import years

# 创建DataFrame
# data = {
#     '当前时间': ['201509', '201510', '201511'],
#     '网别': ['2G', '3G', '2G'],
#     '年龄': [6, 7, 8],
#     '消费级别': [20, 25, 30],
#     '换机时间': ['201509', '201508', '201507'],  # 注意：这里的数据可能是有误的，因为换机时间通常在当前时间之后
#     '是否当前月换机': [1, 0, 1]
# }
names = ['当前时间', '网别', '年龄', '消费级别', '换机时间', '是否当前月换机']
df = pd.read_csv("F:/Dev/Unicom-Moniter/Unicom-Moniter/predict_dataset/part-m-00010", names=names)
# 特征工程
# 转换时间特征为可用于计算的格式
df['当前时间'] = df['当前时间'].astype(int)
df['换机时间'] = df['换机时间'].astype(int)
df['换机间隔_月'] = (df['当前时间'] - df['换机时间']) // 100  # 计算月份差

# 将分类特征转换为数值特征
le_net = LabelEncoder()
df['网别_编码'] = le_net.fit_transform(df['网别'])

# 选择特征列和目标列
X = df[['网别_编码', '年龄', '消费级别', '换机间隔_月']]
y = df['是否当前月换机']  # 假设的标签（在实际应用中应使用真实的标签）

# 划分训练集和测试集
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.25, random_state=42)

# 构建和训练分类模型
model = RandomForestClassifier(n_estimators=100, random_state=42)
model.fit(X_train, y_train)

# 预测（注意：这些预测是基于假设的标签进行的）
y_pred = model.predict(X_test)


print("预测结果:", y_pred)
print("真实标签:", y_test.values)
print("准确率:", accuracy_score(y_test, y_pred))
