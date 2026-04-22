import pandas as pd
import jieba
import re
from wordcloud import WordCloud
import matplotlib.pyplot as plt

# 设置中文字体
plt.rcParams["font.family"] = ["SimHei", "WenQuanYi Micro Hei"]

# 读取Excel文件（替换为你的文件路径）
df = pd.read_excel("D:\\Users\\jianyangz\\Downloads\\SA城市评论.xlsx",sheet_name='sheet1')

# 提取目标列（替换为你的列名）
df = df[df['cityname']=='曼谷']
# print(df)

texts = df["commentdetail"].dropna().astype(str)

# 文本处理：合并所有文本、分词、过滤
all_text = " ".join(texts)
all_text = re.sub(r'[^\u4e00-\u9fa5a-zA-Z]', ' ', all_text)  # 保留中英文
words = jieba.cut(all_text)
stopwords = {'民宿','酒店','特别', '可以','房间','还有','还是','就是','但是','我们','的', '了','非常','在', '是', '我', '有', '和', '就', '不', '人', '都', '一', '一个', '上', '也', '很', '到', '说', '要', '去', '你', '会', '着', '没有', '看', '好', '自己', '这'}  # 简化停用词
filtered_words = [w for w in words if w not in stopwords and len(w) > 1]
word_str = " ".join(filtered_words)

# 生成词云
wc = WordCloud(
    font_path="simhei.ttf",  #cls 替换为你的中文字体路径
    background_color="white",
    width=800,
    height=600
).generate(word_str)

# 显示并保存
plt.figure(figsize=(10, 6))
plt.imshow(wc)
plt.axis("off")
plt.savefig("wordcloud.png")
plt.show()
    