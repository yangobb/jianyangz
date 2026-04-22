import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeClassifier, export_text, export_graphviz
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import accuracy_score, classification_report
import matplotlib.pyplot as plt
import graphviz


df = pd.read_excel('D:\\Users\\jianyangz\\Desktop\\ota_tujia_oversea_cd4.xlsx',sheet_name='shouer')
  

# 2. 数据预处理
# （1）将分类特征（是否近景区）编码为数值（0/1）
# le = LabelEncoder()
# df['是否近景区'] = le.fit_transform(df['是否近景区'])  # 是→1，否→0

# （2）拆分特征（X）和目标变量（y）
X = df[['bedroom_count','good_view','is_active','comment_score','picture_count','empty_rate','facilities_num','vs_jiudian_price','round_time_length']]  # 输入特征


y = df['is_good_ctrip']  # 目标变量

# （3）拆分训练集（80%）和测试集（20%）
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42  # random_state固定拆分方式，保证结果可复现
)

# 3. 训练决策树模型
# 初始化决策树分类器（限制树深度为3，避免过拟合且便于可视化）
clf = DecisionTreeClassifier(
    max_depth=3,  # 树的最大深度
    min_samples_split=2,  # 拆分节点所需的最小样本数
    random_state=42
)

# 训练模型
clf.fit(X_train, y_train)

# 4. 模型评估
# （1）在测试集上预测
y_pred = clf.predict(X_test)

# （2）计算准确率
accuracy = accuracy_score(y_test, y_pred)
print(f"模型准确率：{accuracy:.2f}")  # 输出示例：0.83（83%）

# （3）详细评估报告（包含精确率、召回率等）
print("\n分类报告：")
print(classification_report(y_test, y_pred))

# 5. 提取决策规则（可直接用于业务落地）
print("\n决策树规则：")
rules = export_text(
    clf,
    feature_names=X.columns.tolist(),  # 特征名称
    show_weights=True  # 显示每个节点的样本权重
)
print(rules)

# 6. 可视化决策树（需安装graphviz软件）
dot_data = export_graphviz(
    clf,
    out_file=None,
    feature_names=X.columns.tolist(),  # 特征名称
    class_names=clf.classes_,  # 目标变量类别（高/中/低）
    filled=True,  # 节点填充颜色（颜色越深样本数越多）
    rounded=True,  # 节点边框圆角
    special_characters=True  # 支持特殊字符
)

# 生成可视化图并保存为PDF
graph = graphviz.Source(dot_data)
graph.render("ota_house_heat_tree", format="pdf", cleanup=True)  # 保存为PDF文件
print("\n决策树已保存为 'ota_house_heat_tree.pdf'")
