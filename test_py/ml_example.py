# ==============基于Scikit-learn接口的分类================
from sklearn.datasets import load_iris
from sklearn.datasets import load_boston
import xgboost as xgb
from xgboost import plot_importance
from matplotlib import pyplot as plt
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
import imp

# 加载样本数据集
iris = load_iris()
X,y = iris.data,iris.target
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.4, random_state=100000) # 数据集分割

# 训练模型
model = xgb.XGBClassifier(max_depth=5, learning_rate=0.1, n_estimators=160, silent=True, objective='multi:softmax')
model.fit(X_train, y_train)

# 对测试集进行预测
y_pred = model.predict(X_test)

# 计算准确率
accuracy = accuracy_score(y_test,y_pred)
print('percent: {:.2%}'.format(accuracy))
print("accuarcy: %.2f%%" % (accuracy*100.0))

print(len(X_train))
print(len(X_test))
print(X_test)
# print('############################################')
# print(X_train)

# 显示重要特征
# plot_importance(model)
# plt.show()
