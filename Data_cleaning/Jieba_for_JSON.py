import json
import jieba
from snownlp import SnowNLP
import pandas as pd

# 讀取 JSON 檔案
with open('my_ptt_posts.json', 'r', encoding='utf-8') as f:
    articles = json.load(f)

# 建立一個列表，用來保存每篇文章的分析結果
data = []

# 假設每篇文章包含 'content'，處理每篇文章
for article in articles:
    content = article['content']
    
    # 文字預處理：分詞
    words = jieba.lcut(content)
    
    # 情緒分析
    s = SnowNLP(content)
    sentiment_score = s.sentiments  # 分數範圍：0（負面）到 1（正面）
    
    # 將每篇文章的資訊儲存為字典
    data.append({
        '文章ID': article['id'],
        '文章標題': article['title'],
        '情緒得分': sentiment_score
    })

# 將資料轉換為 DataFrame
df = pd.DataFrame(data)

# 將 DataFrame 保存為 Excel 檔案
df.to_excel('analyzed_articles.xlsx', index=False, engine='openpyxl')

print("情緒分析結果已保存為 analyzed_articles.xlsx")