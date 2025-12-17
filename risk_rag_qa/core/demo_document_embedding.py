import os
from dotenv import load_dotenv
from risk_rag_qa.risk_document_loaders.risk_csvloader import RiskCSVLoader
from langchain_openai import AzureOpenAIEmbeddings
from langchain_community.vectorstores import Milvus

# 加载环境变量
load_dotenv()

# ai code begin && nums:22
# 1. 加载CSV为Document
# 注意：Milvus字段名不支持中文，需要将中文字段名映射为英文
loader = RiskCSVLoader(
    file_path="../data/processed/亚马逊法规库_20250919.csv",
    content_columns=["受限品", "关键词"],
    metadata_columns=["受限品", "URL"]
)
documents = loader.load()

# 将元数据中的中文字段名映射为英文（Milvus要求字段名以字母或下划线开头）
field_mapping = {
    "受限品": "restricted_product",
    "关键词": "keyword",
    "URL": "url"
}

for doc in documents:
    new_metadata = {}
    for key, value in doc.metadata.items():
        new_key = field_mapping.get(key, key)
        new_metadata[new_key] = value
    doc.metadata = new_metadata
# ai code end

# ai code begin && nums:9
# 2. 创建Azure OpenAI Embedding模型
embeddings = AzureOpenAIEmbeddings(
    azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
    azure_deployment=os.getenv("AZURE_OPENAI_EMBEDDING_DEPLOYMENT"),
    api_key=os.getenv("AZURE_OPENAI_API_KEY"),
    api_version=os.getenv("AZURE_OPENAI_API_VERSION")
)
# ai code end

# 3. 存入Milvus向量数据库
vector_store = Milvus.from_documents(
    documents=documents,
    embedding=embeddings,
    connection_args={
        "host": os.getenv("MILVUS_HOST"),
        "port": os.getenv("MILVUS_PORT"),
        "user": os.getenv("MILVUS_USER"),
        "password": os.getenv("MILVUS_PASSWORD"),
        "db_name": os.getenv("MILVUS_DB_NAME")
    },
    collection_name="amazon_regulations"
)

# 4. 检索测试
results = vector_store.similarity_search("alcohol beer", k=3)
for doc in results:
    print(doc.page_content)
    print(doc.metadata)