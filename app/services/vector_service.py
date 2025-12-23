import os
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv
from langchain_openai import AzureOpenAIEmbeddings
from langchain_community.vectorstores import Milvus
from langchain_core.documents import Document

# 加载环境变量
load_dotenv()


# ai code begin && nums:95
class VectorService:
    """
    向量检索服务
    
    提供向量数据库的检索功能，支持多个集合（collection）的检索。
    封装了Milvus向量数据库的连接和检索逻辑。
    """
    
    def __init__(self, collection_name: str = "liangou_regulations"):
        """
        初始化向量检索服务
        
        Args:
            collection_name: Milvus集合名称，默认为"liangou_regulations"
        """
        self.collection_name = collection_name
        self._embeddings = None
        self._vector_store = None
        self._initialize()
    
    def _initialize(self):
        """初始化Embedding模型和向量数据库连接"""
        # 创建Azure OpenAI Embedding模型（与存储时使用相同的模型）
        self._embeddings = AzureOpenAIEmbeddings(
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
            azure_deployment=os.getenv("AZURE_OPENAI_EMBEDDING_DEPLOYMENT"),
            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
            api_version=os.getenv("AZURE_OPENAI_API_VERSION")
        )
        
        # 连接到已存在的Milvus向量数据库
        self._vector_store = Milvus(
            embedding_function=self._embeddings,
            connection_args={
                "host": os.getenv("MILVUS_HOST"),
                "port": os.getenv("MILVUS_PORT"),
                "user": os.getenv("MILVUS_USER"),
                "password": os.getenv("MILVUS_PASSWORD"),
                "db_name": os.getenv("MILVUS_DB_NAME")
            },
            collection_name=self.collection_name
        )
    
    def search(self, query: str, top_k: int = 10) -> List[Document]:
        """
        同步检索向量数据库
        
        Args:
            query: 查询文本
            top_k: 返回结果数量，默认10
            
        Returns:
            List[Document]: 检索结果文档列表
        """
        if not query or not query.strip():
            raise ValueError("查询文本不能为空")
        
        results = self._vector_store.similarity_search(query, k=top_k)
        return results
    
    async def search_async(self, query: str, top_k: int = 10) -> List[Document]:
        """
        异步检索向量数据库
        
        Args:
            query: 查询文本
            top_k: 返回结果数量，默认10
            
        Returns:
            List[Document]: 检索结果文档列表
        """
        # 由于Milvus的similarity_search是同步方法，这里使用同步调用
        # 如果需要真正的异步，可以考虑使用线程池
        return self.search(query, top_k)
    
    def search_with_scores(self, query: str, top_k: int = 10) -> List[tuple]:
        """
        检索并返回相似度分数
        
        Args:
            query: 查询文本
            top_k: 返回结果数量，默认10
            
        Returns:
            List[tuple]: (Document, score) 元组列表
        """
        if not query or not query.strip():
            raise ValueError("查询文本不能为空")
        
        results = self._vector_store.similarity_search_with_score(query, k=top_k)
        return results
    
    def format_results(self, results: List[Document]) -> List[Dict[str, Any]]:
        """
        格式化检索结果为字典列表
        
        Args:
            results: Document列表
            
        Returns:
            List[Dict]: 格式化后的结果列表，每个字典包含content和metadata
        """
        formatted_results = []
        for i, doc in enumerate(results, 1):
            formatted_results.append({
                "rank": i,
                "content": doc.page_content,
                "metadata": doc.metadata
            })
        return formatted_results
    
    def format_results_with_scores(self, results: List[tuple]) -> List[Dict[str, Any]]:
        """
        格式化带分数的检索结果为字典列表
        
        Args:
            results: (Document, score) 元组列表
            
        Returns:
            List[Dict]: 格式化后的结果列表，每个字典包含content、metadata和score
        """
        formatted_results = []
        for i, (doc, score) in enumerate(results, 1):
            formatted_results.append({
                "rank": i,
                "content": doc.page_content,
                "metadata": doc.metadata,
                "score": float(score)
            })
        return formatted_results
    
    def get_collection_name(self) -> str:
        """获取当前使用的集合名称"""
        return self.collection_name
    
    def switch_collection(self, collection_name: str):
        """
        切换向量数据库集合
        
        Args:
            collection_name: 新的集合名称
        """
        if collection_name != self.collection_name:
            self.collection_name = collection_name
            self._initialize()
# ai code end

