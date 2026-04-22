
file_path = 'D:\\Users\\jianyangz\\Desktop\\转化影响因子评估.xlsx'
# file_path = 'D:\\Users\\jianyangz\\Desktop\\转化影响因子评估-直采.xlsx'
sheet_name1='Sheet1'


import pandas as pd
import numpy as np
import statsmodels.api as sm

# =============== 1. 一行读取 + 强制取整 ===============
df = pd.read_excel(file_path,sheet_name=sheet_name1)
y = df['conversion_1000uv'].round().astype(int)

# =============== 2. 自动处理所有特征（含字符串） ===============
X = df[['dynamic_business','house_class','price_5','house_quality_score','style_score']].copy()

# 自动把字符串列转成数字（用独热编码），其他列保持原样
X = pd.get_dummies(X, drop_first=True)

# 确保全是数值（解决 object dtype 问题）
X = X.select_dtypes(include=[np.number]).fillna(0)

# =============== 3. 直接跑 ZINB ===============
from statsmodels.discrete.count_model import ZeroInflatedNegativeBinomialP

X = sm.add_constant(X)
model = ZeroInflatedNegativeBinomialP(y, X, exog_infl=X, inflation='logit')
result = model.fit(disp=0)  # disp=0 静默运行

# =============== 4. 清晰输出 ===============
print("✅ 分析完成！\n")

# 提取系数
p = result.params
infl = p[p.index.str.contains('inflate')].rename('空置风险影响')
count = p[~p.index.str.contains('inflate')].rename('转化频次影响')

print("【降低空置风险】（负值越好）")
print(infl.sort_values().head(5).round(4))

print("\n【提升转化频次】（正值越好）")
print(count.sort_values(ascending=False).head(5).round(4))