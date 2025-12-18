import os
from dotenv import load_dotenv
from risk_rag_qa.risk_document_loaders.risk_csvloader import RiskCSVLoader
from langchain_openai import AzureOpenAIEmbeddings
from langchain_community.vectorstores import Milvus
import time

start = time.time()
# ============================================================================
# 环境变量加载
# ============================================================================
# 加载环境变量（从.env文件或系统环境变量中读取配置信息）
load_dotenv()

# ============================================================================
# 数据字段处理部分
# ============================================================================
# ai code begin && nums:22
# 1. 加载CSV文件为Document对象
# 注意：Milvus字段名不支持中文，需要将中文字段名映射为英文
loader = RiskCSVLoader(
    file_path="../data/processed/处理后产品库标题向量数据.csv",
    # content_columns: 指定哪些列会被合并成文档的文本内容（page_content）
    # 这些列的内容会被向量化存储，用于相似度检索
    # 向量字段：page_content（由"title_cn生成）
    content_columns=["title_cn"],
    # metadata_columns: 指定哪些列会作为元数据存储
    # 这些列不会被向量化，但会存储在Milvus中，可用于过滤和检索结果展示
    # 元数据字段：metadata（包含"受限品"和"URL"等信息）
    metadata_columns=["lib_main_sku", "title_cn"]
)
documents = loader.load()

# 2. 字段名映射：将中文字段名映射为英文（Milvus要求字段名以字母或下划线开头）
# 映射规则：
#   - "受限品" -> "restricted_product" (元数据字段，存储受限产品名称)
#   - "关键词" -> "keyword" (仅在content_columns中使用，不直接出现在metadata中)
#   - "URL" -> "url" (元数据字段，存储相关URL链接)
"""
field_mapping = {
    "受限品": "restricted_product",
    "关键词": "keyword",
    "URL": "url"
}
"""

# 遍历所有文档，将元数据中的中文字段名替换为英文字段名
for doc in documents:
    new_metadata = {}
    for key, value in doc.metadata.items():
        """
        如果有列名是中文,需要进行替换则加入下面两行
        # new_key = field_mapping.get(key, key)
        # new_metadata[new_key] = value
        """

        new_metadata[key] = value
    doc.metadata = new_metadata
# ai code end

# ============================================================================
# Embedding模型配置部分
# ============================================================================
# ai code begin && nums:9
# 3. 创建Azure OpenAI Embedding模型
# 该模型用于将文本转换为向量（embedding），用于向量相似度检索
embeddings = AzureOpenAIEmbeddings(
    # Azure OpenAI服务的端点地址
    azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
    # Azure OpenAI部署的embedding模型名称
    azure_deployment=os.getenv("AZURE_OPENAI_EMBEDDING_DEPLOYMENT"),
    # API密钥，用于身份验证
    api_key=os.getenv("AZURE_OPENAI_API_KEY"),
    # API版本号
    api_version=os.getenv("AZURE_OPENAI_API_VERSION")
)
# ai code end

# ============================================================================
# 数据库配置部分
# ============================================================================
# 4. 连接Milvus向量数据库（增量插入模式）
# Milvus是一个开源的向量数据库，专门用于存储和检索高维向量数据
vector_store = Milvus(
    embedding_function=embeddings,  # Embedding模型，用于将文本转换为向量
    # Milvus数据库连接参数
    connection_args={
        "host": os.getenv("MILVUS_HOST"),      # Milvus服务器地址
        "port": os.getenv("MILVUS_PORT"),      # Milvus服务器端口
        "user": os.getenv("MILVUS_USER"),      # 数据库用户名
        "password": os.getenv("MILVUS_PASSWORD"),  # 数据库密码
        "db_name": os.getenv("MILVUS_DB_NAME")     # 数据库名称
    },
    # 集合名称（类似关系数据库中的表名）
    # 如果集合不存在会自动创建，如果存在则追加数据
    collection_name="liangou_regulations"
)

# 5. 增量插入：检查已存在数据，只插入新数据
print(f"总共加载了 {len(documents)} 个文档")

# 获取已存在的文档标识（使用lib_main_sku作为唯一标识）
existing_skus = set()
try:
    existing_docs = vector_store.similarity_search("", k=10000)
    for doc in existing_docs:
        if "lib_main_sku" in doc.metadata:
            existing_skus.add(str(doc.metadata["lib_main_sku"]))
    print(f"集合中已存在 {len(existing_skus)} 条记录")
except Exception:
    print("集合可能不存在或为空，将创建新集合")

# 过滤出新文档
new_documents = []
for doc in documents:
    sku = doc.metadata.get("lib_main_sku")
    if sku and str(sku) not in existing_skus:
        new_documents.append(doc)
    elif not sku:
        new_documents.append(doc)

print(f"需要新增 {len(new_documents)} 条记录")

# 6. 分批增量插入：每次只添加10条
BATCH_SIZE = 10
if new_documents:
    total_batches = (len(new_documents) + BATCH_SIZE - 1) // BATCH_SIZE
    print(f"将分 {total_batches} 批处理，每批 {BATCH_SIZE} 条记录\n")
    
    for i in range(0, len(new_documents), BATCH_SIZE):
        batch = new_documents[i:i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        
        texts = [doc.page_content for doc in batch]
        metadatas = [doc.metadata for doc in batch]
        # 生成IDs：使用lib_main_sku作为ID，如果没有则使用索引生成
        ids = []
        for idx, doc in enumerate(batch):
            sku = doc.metadata.get("lib_main_sku")
            if sku:
                ids.append(str(sku))
            else:
                # 如果没有SKU，使用批次索引和文档索引生成唯一ID
                ids.append(f"batch_{batch_num}_doc_{idx}")
        
        print(f"【批次 {batch_num}/{total_batches}】插入 {len(batch)} 条记录:")
        for idx, doc in enumerate(batch, 1):
            sku = doc.metadata.get("lib_main_sku", "N/A")
            title = doc.metadata.get("title_cn", doc.page_content[:30])
            print(f"  [{idx}] ID: {ids[idx-1]} | SKU: {sku} | 标题: {title}")
        
        try:
            vector_store.add_texts(texts=texts, metadatas=metadatas, ids=ids)
            print(f"  ✓ 成功插入 {len(batch)} 条记录\n")
            time.sleep(1)  # 延迟1秒避免API限流
        except Exception as e:
            print(f"  ✗ 插入失败: {str(e)}\n")
            time.sleep(5)
else:
    print("所有文档已存在，无需插入新数据")

end = time.time()
use_time = end-start
print('use_time-->',use_time)
# ============================================================================
# 检索测试部分
# ============================================================================
# 5. 相似度检索测试
# similarity_search方法的工作原理：
#   1. 将查询文本"alcohol beer"通过embedding模型转换为向量
#   2. 在Milvus中搜索与查询向量最相似的k=3个文档向量
#   3. 检索是基于page_content字段的向量进行相似度计算
#   4. 返回最相似的3个文档，包含page_content和metadata
# 
# 检索字段说明：
#   - 检索基于：page_content字段的向量（由"受限品"+"关键词"合并生成）
#   - 返回结果包含：page_content（原始文本内容）和metadata（元数据信息）
results = vector_store.similarity_search("好看的短袖", k=3)
for doc in results:
    print(doc.page_content)  # 打印文档的文本内容（向量化的字段）
    print(doc.metadata)      # 打印文档的元数据（非向量化的字段）