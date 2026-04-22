
import pandas as pd
import jieba
import re
from collections import Counter
from wordcloud import WordCloud
import matplotlib.pyplot as plt



# 设置中文字体
plt.rcParams["font.family"] = ["SimHei", "WenQuanYi Micro Hei"]

# 读取Excel文件（替换为你的文件路径）

# 低星评论分析：提取用户最关注的问题点
import pandas as pd
import re
import jieba
from collections import Counter
import matplotlib.pyplot as plt
from wordcloud import WordCloud

# 设置中文显示
plt.rcParams["font.family"] = ["SimHei", "WenQuanYi Micro Hei", "Heiti TC"]
plt.rcParams["axes.unicode_minus"] = False

# 文本预处理
# 定义词语归类规则（核心！可根据实际需求增删）
category_mapping = {
    # 卫生相关
    "卫生问题": ["不干净", "脏", "污渍", "异味", "臭味", "不整洁", "邋遢", "肮脏"],
    # 服务相关
    "服务差": ["态度差", "服务不好", "不理人", "不耐烦", "冷漠", "敷衍", "傲慢"],
    # 速度相关
    "速度慢": ["太慢", "拖延", "耗时", "等太久", "效率低", "慢吞吞"],
    # 质量相关
    "质量差": ["质量不好", "劣质", "易坏", "做工差", "材料差", "不耐用"],
    # 价格相关
    "价格高": ["太贵", "不值", "性价比低", "收费高", "偏贵"]
}

# 反向映射：{词语: 类别}，方便快速查询
word_to_category = {}
for category, words in category_mapping.items():
    for word in words:
        word_to_category[word] = category


#  读取数据
# 读取数据
df = pd.read_excel("D:\\Users\\jianyangz\\Desktop\\\海外追问明细汇总.xlsx",sheet_name='诋毁')

# 提取评论内容
reviews = df['诋毁'].dropna().tolist()
print(f"共加载 {len(reviews)} 条有效评论")

# 定义停用词
stopwords = {'的', '了', '在', '是', '我', '有', '和', '就', '不', '人', '都', '一', '一个', '上', '也', '很', '到', '说', '要', '去', '你', '会', '着', '没有', '看', '好', '自己', '这', '也', '还', '但', '对于', '呢', '啊', '吧'}




# 处理每条评论
processed_words = []
for review in reviews:
    # 去除特殊字符和数字
    review = re.sub(r'[^\w\s]', '', str(review))
    review = re.sub(r'\d+', '', review)
    
    # 分词
    words = jieba.cut(review.strip())
    
    # 过滤停用词和短词
    filtered = [word for word in words if word not in stopwords and len(word) > 1]
    processed_words.extend(filtered)

# 3. 统计高频词（用户最关注的点）
word_counts = Counter(processed_words)
top_words = word_counts.most_common(20)  # 取前20个高频词

# 4. 可视化结果
# 高频词柱状图
plt.figure(figsize=(12, 6))
words, counts = zip(*top_words)
plt.bar(words, counts)
plt.title('用户最关注的问题点（高频词）')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# 词云
wordcloud_text = ' '.join(processed_words)
wordcloud = WordCloud(
    width=800, 
    height=400, 
    background_color='white',
    font_path="simhei.ttf"  # 确保有这个字体或替换为系统中的中文字体
).generate(wordcloud_text)

plt.figure(figsize=(12, 6))
plt.imshow(wordcloud, interpolation='bilinear')
plt.axis('off')
plt.title('评论关键词词云')
plt.tight_layout()
plt.show()

# 5. 输出结果
print("\n用户最关注的问题点（按出现频率排序）：")
for i, (word, count) in enumerate(top_words, 1):
    print(f"{i}. {word} - 出现 {count} 次")
print("\n建议优先改进出现频率最高的前5个问题点")
    