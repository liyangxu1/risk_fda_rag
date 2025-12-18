import pandas as pd


df = pd.read_csv("../data/processed/处理后产品库标题向量数据.csv")
print(df.shape)
print(df.columns)
counts = df['title_cn'].value_counts()
print(counts)

print("----------------------------------")
print(df[df['title_cn'] == "高级彩绘娃娃"].count())

