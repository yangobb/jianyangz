
# file_path = 'D:\\Users\\jianyangz\\Desktop\\转化影响因子评估.xlsx'
file_path = 'D:\\Users\\jianyangz\\Desktop\\转化影响因子评估-直采.xlsx'
sheet_name1='Sheet1'

import pandas as pd
import numpy as np
import statsmodels.api as sm

# 1. 读取数据并取整（确保整数）
df = pd.read_excel(file_path,sheet_name=sheet_name1)
y = df['conversion_1000uv'].round().astype(int)
X_raw = df[['comment_score','house_class', 'price_5', 'house_quality_score','style_score']]

# 2. 特征工程：独热编码 + 数值标准化（关键！）
X = pd.get_dummies(X_raw, drop_first=True)
from sklearn.preprocessing import StandardScaler
scaler = StandardScaler()
X_scaled = pd.DataFrame(
    scaler.fit_transform(X.select_dtypes(include=[np.number])),
    columns=X.select_dtypes(include=[np.number]).columns,
    index=X.index
)
# 合并可能存在的非数值列（此处应无）
X_final = X_scaled

# 3. 第一部分：Logistic 回归（预测是否 >0）
X_logit = sm.add_constant(X_final)
logit_model = sm.Logit((y > 0).astype(int), X_logit)
logit_result = logit_model.fit(disp=0)

# 4. 第二部分：仅对非零样本做 Poisson 回归（稳定！）
mask = y > 0
y_pos = y[mask]
X_poisson = X_logit.loc[mask]
poisson_model = sm.GLM(y_pos, X_poisson, family=sm.families.Poisson())
poisson_result = poisson_model.fit()

# 5. 输出结果
print("✅ Hurdle 模型分析完成！\n")

print("【降低空置风险】（Logistic 部分，系数 < 0 越好）")
logit_coefs = logit_result.params.drop('const')
print(logit_coefs.sort_values().head(5).round(4))

print("\n【提升转化频次】（Poisson 部分，系数 > 0 越好）")
poisson_coefs = poisson_result.params.drop('const')
print(poisson_coefs.sort_values(ascending=False).head(5).round(4))