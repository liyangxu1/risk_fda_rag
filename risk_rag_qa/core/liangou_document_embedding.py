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

# ============================================================================
# 断点续传配置部分
# ============================================================================
# ai code begin && nums:15
# 断点续传配置：手动设置起始批次号（从1开始，设置为1表示从头开始）
# 例如：如果上次失败在4447批次，则设置为4447，程序会从该批次继续
START_BATCH_NUM = 4447  # 手动设置起始批次号，从1开始

# 重试配置
MAX_RETRIES = 3  # 每个批次最大重试次数
RETRY_DELAY = 5  # 重试前等待时间（秒）
# ai code end

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

# 6. 分批增量插入：每次只添加10条（支持断点续传）
BATCH_SIZE = 10
if new_documents:
    total_batches = (len(new_documents) + BATCH_SIZE - 1) // BATCH_SIZE
    print(f"将分 {total_batches} 批处理，每批 {BATCH_SIZE} 条记录")
    
    # 断点续传：根据手动设置的起始批次号计算起始索引
    if START_BATCH_NUM > 1:
        start_index = (START_BATCH_NUM - 1) * BATCH_SIZE
        if start_index >= len(new_documents):
            print(f"⚠️  起始批次号 {START_BATCH_NUM} 超出范围，将从第1批次开始\n")
            start_index = 0
        else:
            print(f"🔄 断点续传: 从批次 {START_BATCH_NUM} 开始（索引 {start_index}）\n")
    else:
        start_index = 0
        print("🆕 从头开始插入\n")
    
    # 批量插入循环
    success_count = 0
    failed_count = 0
    
    for i in range(start_index, len(new_documents), BATCH_SIZE):
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
        
        # 重试机制
        insert_success = False
        for retry in range(MAX_RETRIES):
            try:
                vector_store.add_texts(texts=texts, metadatas=metadatas, ids=ids)
                print(f"  ✓ 成功插入 {len(batch)} 条记录")
                insert_success = True
                success_count += len(batch)
                time.sleep(1)  # 延迟1秒避免API限流
                break
            except Exception as e:
                error_msg = str(e)
                if retry < MAX_RETRIES - 1:
                    print(f"  ✗ 插入失败（重试 {retry + 1}/{MAX_RETRIES}）: {error_msg[:200]}...")
                    print(f"  ⏳ 等待 {RETRY_DELAY} 秒后重试...")
                    time.sleep(RETRY_DELAY)
                else:
                    print(f"  ✗ 插入失败（已重试 {MAX_RETRIES} 次）: {error_msg[:200]}...")
                    failed_count += len(batch)
                    print(f"  ⚠️  当前批次失败，继续处理下一批次...")
                    print(f"  💡 提示: 如需断点续传，请将 START_BATCH_NUM 设置为 {batch_num}\n")
                    time.sleep(RETRY_DELAY)
        
        print()  # 空行分隔
        
        # 每100批次输出一次统计信息
        if batch_num % 100 == 0:
            print(f"📊 进度统计: 已处理 {batch_num}/{total_batches} 批次, "
                  f"成功 {success_count} 条, 失败 {failed_count} 条\n")
    
    # 插入完成
    print(f"\n{'='*60}")
    print(f"✅ 批量插入完成!")
    print(f"   总批次数: {total_batches}")
    print(f"   成功插入: {success_count} 条")
    print(f"   插入失败: {failed_count} 条")
    if failed_count > 0:
        print(f"   💡 提示: 如需继续插入失败的记录，请手动设置 START_BATCH_NUM 并重新运行")
    print(f"{'='*60}\n")
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



# todo
"""
Failed to insert batch starting at entity: 0/10
【批次 4447/6839】插入 10 条记录:
  [1] ID: LF10258229 | SKU: LF10258229 | 标题: 2023欧美跨境秋季新品 时尚光滑面料不对称长袖女装
  [2] ID: LK20258230 | SKU: LK20258230 | 标题: 握力器专业练手力量男士电子款训练臂力器材中学生可调节手指锻炼
  [3] ID: LI10258231 | SKU: LI10258231 | 标题: 2022夏季新款时尚帆布手提包女单肩包休闲百搭大容量女士帆布包包
  [4] ID: LF40258232 | SKU: LF40258232 | 标题: 1492-Mini Apple Pendant天然水晶小苹果吊坠 平安夜圣诞跨境货源
  [5] ID: LL20258233 | SKU: LL20258233 | 标题: 户外战术易拉扣伸缩扣钢丝绳多功能钥匙扣露营高回弹防丢绳易拉得
  [6] ID: LX40258234 | SKU: LX40258234 | 标题: 圣诞节装饰品创意搞怪红色裤腿帽儿童成人圣诞帽小丑帽子派对活动
  [7] ID: LA70258235 | SKU: LA70258235 | 标题: 奥克斯折叠水壶旅行出行便携式旅游烧水壶304不锈钢电热水杯批发
  [8] ID: LF20258236 | SKU: LF20258236 | 标题: 秋春新款坚条纹三立扣男式长袖Polo衫商务纯色薄款透气翻领t恤男
  [9] ID: LI10258237 | SKU: LI10258237 | 标题: 女士尼龙布包新款大容量斜挎女包防泼水休闲百搭轻便潮流单肩包
  [10] ID: LM10258238 | SKU: LM10258238 | 标题: 抖音爆款酒桶型男士手表个性潮流石英表夜光防水日历跨境厂家批发
2025-12-18 18:32:52,083 [ERROR][handler]: RPC error: [batch_insert], <MilvusException: (code=<bound method _MultiThreadedRendezvous.code of <_MultiThreadedRendezvous of RPC that terminated with:
	status = StatusCode.UNAVAILABLE
	details = "failed to connect to all addresses; last error: UNAVAILABLE: ipv6:%5B::1%5D:19530: ConnectEx: Connection refused (No connection could be made because the target machine actively refused it.
 -- 10061)"
	debug_error_string = "UNKNOWN:Error received from peer  {grpc_message:"failed to connect to all addresses; last error: UNAVAILABLE: ipv6:%5B::1%5D:19530: ConnectEx: Connection refused (No connection could be made because the target machine actively refused it.\r\n -- 10061)", grpc_status:14}"
>>, message=[batch_insert] Retry run out of 75 retry times, message=failed to connect to all addresses; last error: UNAVAILABLE: ipv6:%5B::1%5D:19530: ConnectEx: Connection refused (No connection could be made because the target machine actively refused it.
 -- 10061))>, <Time:{'RPC start': '2025-12-18 18:29:18.926447', 'RPC error': '2025-12-18 18:32:52.061217'}>
Traceback:
Traceback (most recent call last):
  File "D:\workspace\leite\python\risk_fda_rag\.venv\Lib\site-packages\pymilvus\decorators.py", line 166, in handler
    return func(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^
  File "D:\workspace\leite\python\risk_fda_rag\.venv\Lib\site-packages\pymilvus\client\grpc_handler.py", line 766, in batch_insert
    raise err from err
  File "D:\workspace\leite\python\risk_fda_rag\.venv\Lib\site-packages\pymilvus\client\grpc_handler.py", line 759, in batch_insert
    response = rf.result()
               ^^^^^^^^^^^
  File "D:\workspace\leite\python\risk_fda_rag\.venv\Lib\site-packages\grpc\_channel.py", line 878, in result
    raise self
grpc._channel._MultiThreadedRendezvous: <_MultiThreadedRendezvous of RPC that terminated with:
	status = StatusCode.UNAVAILABLE
	details = "failed to connect to all addresses; last error: UNAVAILABLE: ipv6:%5B::1%5D:19530: ConnectEx: Connection refused (No connection could be made because the target machine actively refused it.
 -- 10061)"
	debug_error_string = "UNKNOWN:Error received from peer  {grpc_message:"failed to connect to all addresses; last error: UNAVAILABLE: ipv6:%5B::1%5D:19530: ConnectEx: Connection refused (No connection could be made because the target machine actively refused it.\r\n -- 10061)", grpc_status:14}"
>

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "D:\workspace\leite\python\risk_fda_rag\.venv\Lib\site-packages\pymilvus\decorators.py", line 263, in handler
    return func(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^
  File "D:\workspace\leite\python\risk_fda_rag\.venv\Lib\site-packages\pymilvus\decorators.py", line 322, in handler
    return func(self, *args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "D:\workspace\leite\python\risk_fda_rag\.venv\Lib\site-packages\pymilvus\decorators.py", line 172, in handler
    raise MilvusException(e.code, f"{to_msg}, message={e.details()}") from e
pymilvus.exceptions.MilvusException: <MilvusException: (code=<bound method _MultiThreadedRendezvous.code of <_MultiThreadedRendezvous of RPC that terminated with:
	status = StatusCode.UNAVAILABLE
	details = "failed to connect to all addresses; last error: UNAVAILABLE: ipv6:%5B::1%5D:19530: ConnectEx: Connection refused (No connection could be made because the target machine actively refused it.
 -- 10061)"
	debug_error_string = "UNKNOWN:Error received from peer  {grpc_message:"failed to connect to all addresses; last error: UNAVAILABLE: ipv6:%5B::1%5D:19530: ConnectEx: Connection refused (No connection could be made because the target machine actively refused it.\r\n -- 10061)", grpc_status:14}"
>>, message=[batch_insert] Retry run out of 75 retry times, message=failed to connect to all addresses; last error: UNAVAILABLE: ipv6:%5B::1%5D:19530: ConnectEx: Connection refused (No connection could be made because the target machine actively refused it.
 -- 10061))>
 (decorators.py:267)
Failed to insert batch starting at entity: 0/10
  ✗ 插入失败: <MilvusException: (code=<bound method _MultiThreadedRendezvous.code of <_MultiThreadedRendezvous of RPC that terminated with:
	status = StatusCode.UNAVAILABLE
	details = "failed to connect to all addresses; last error: UNAVAILABLE: ipv6:%5B::1%5D:19530: ConnectEx: Connection refused (No connection could be made because the target machine actively refused it.
 -- 10061)"
	debug_error_string = "UNKNOWN:Error received from peer  {grpc_message:"failed to connect to all addresses; last error: UNAVAILABLE: ipv6:%5B::1%5D:19530: ConnectEx: Connection refused (No connection could be made because the target machine actively refused it.\r\n -- 10061)", grpc_status:14}"
>>, message=[batch_insert] Retry run out of 75 retry times, message=failed to connect to all addresses; last error: UNAVAILABLE: ipv6:%5B::1%5D:19530: ConnectEx: Connection refused (No connection could be made because the target machine actively refused it.
 -- 10061))>

【批次 4448/6839】插入 10 条记录:
  [1] ID: LJ20258239 | SKU: LJ20258239 | 标题: 跨境热销个性霸气龙手链龙鳞时尚手镯 速卖通 wish 亚马逊 批发
  [2] ID: LF10258240 | SKU: LF10258240 | 标题: 时尚减龄双面羊绒大衣女中长款2023春季新款宽松显瘦毛呢外套
  [3] ID: LE30258241 | SKU: LE30258241 | 标题: 独立站 Eras 巡演日历（2024 年）The Eras Tour Calendar (2024)
  [4] ID: LK20258242 | SKU: LK20258242 | 标题: 欧道美臂器家用健身臂力器开肩美背神器锻炼拜拜肉直角肩器材胳膊
  [5] ID: LP10258243 | SKU: LP10258243 | 标题: 厂家直供儿童手抛降落伞玩具 空中飞伞 带士兵降落伞户外运动玩具
  [6] ID: LG60258244 | SKU: LG60258244 | 标题: 【跨境专供】DIY Clusters Lashes睫毛假睫毛30D/40D睫毛混装套装
  [7] ID: LE30258245 | SKU: LE30258245 | 标题: 成人儿童通用便携充气洗头盆家用老人孕妇免弯腰洗头可折叠平躺式
  [8] ID: LF10258246 | SKU: LF10258246 | 标题: 跨境纯色高领毛衣2023欧美秋冬宽松针织衫Ins亚马逊套头毛衣女
  [9] ID: LJ20258247 | SKU: LJ20258247 | 标题: 珍珠耳饰2021年新款潮丢了一只耳环独特法式高级感轻奢纯银耳钉女
  [10] ID: LE30258248 | SKU: LE30258248 | 标题: 包包收纳袋离尘袋透明整理保护套衣柜防尘防潮悬挂式收纳袋神器
"""