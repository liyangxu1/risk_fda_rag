import os
from dotenv import load_dotenv
from langchain_openai import AzureOpenAIEmbeddings
from langchain_community.vectorstores import Milvus

# 加载环境变量
load_dotenv()

# ai code begin && nums:45
# 1. 创建Azure OpenAI Embedding模型（与存储时使用相同的模型）
embeddings = AzureOpenAIEmbeddings(
    azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
    azure_deployment=os.getenv("AZURE_OPENAI_EMBEDDING_DEPLOYMENT"),
    api_key=os.getenv("AZURE_OPENAI_API_KEY"),
    api_version=os.getenv("AZURE_OPENAI_API_VERSION")
)

# 2. 连接到已存在的Milvus向量数据库
vector_store = Milvus(
    embedding_function=embeddings,
    connection_args={
        "host": os.getenv("MILVUS_HOST"),
        "port": os.getenv("MILVUS_PORT"),
        "user": os.getenv("MILVUS_USER"),
        "password": os.getenv("MILVUS_PASSWORD"),
        "db_name": os.getenv("MILVUS_DB_NAME")
    },
    collection_name="liangou_regulations"
)

# 3. 检索测试
def search(query: str, top_k: int = 10):
    """
    检索向量数据库
    
    Args:
        query: 查询文本
        top_k: 返回结果数量
    """
    print(f"\n{'='*60}")
    print(f"查询: {query}")
    print(f"{'='*60}")
    
    results = vector_store.similarity_search(query, k=top_k)
    
    for i, doc in enumerate(results, 1):
        print(f"\n--- 结果 {i} ---")
        print(f"内容: {doc.page_content}")
        print(f"元数据: {doc.metadata}")
    
    return results


if __name__ == "__main__":
    # 测试检索
    search("蛋糕", top_k=10)
    
    # 可以尝试其他查询
    # search("medical device", top_k=10)
    # search("animal products", top_k=10)
# ai code end

