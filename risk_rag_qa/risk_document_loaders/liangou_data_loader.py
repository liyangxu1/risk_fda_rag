import pandas as pd


df = pd.read_csv("../data/processed/产品库标题向量数据.csv")
print(df.shape)
print(df.columns)
df1 = df.dropna()
print(df1.shape)
df1.to_csv("../data/processed/处理后产品库标题向量数据.csv")
