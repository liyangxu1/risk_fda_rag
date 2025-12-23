import pandas as pd
from typing import List, Dict, Any, Optional, Callable
from langchain_core.documents import Document


# ai code begin && nums:95
class RiskCSVLoader:
    """
    通用风险数据CSV加载器
    
    将CSV文件加载为LangChain Document对象，支持自定义内容列和元数据列。
    
    Args:
        file_path: CSV文件路径
        content_columns: 用于生成文档内容的列名列表，如果为None则使用所有列
        metadata_columns: 用于生成元数据的列名列表，如果为None则使用所有列
        content_formatter: 自定义内容格式化函数，接收row字典，返回字符串
        encoding: 文件编码，默认utf-8
        source_name: 数据源名称，用于元数据中的source字段
    
    Example:
        >>> loader = RiskCSVLoader(
        ...     file_path="data/processed/fda.csv",
        ...     content_columns=["DEVICENAME", "DEVICECLASS"],
        ...     metadata_columns=["PRODUCTCODE", "REGULATIONNUMBER"]
        ... )
        >>> documents = loader.load()
    """
    
    def __init__(
        self,
        file_path: str,
        content_columns: Optional[List[str]] = None,
        metadata_columns: Optional[List[str]] = None,
        content_formatter: Optional[Callable[[Dict[str, Any]], str]] = None,
        encoding: str = "utf-8",
        source_name: Optional[str] = None
    ):
        self.file_path = file_path
        self.content_columns = content_columns
        self.metadata_columns = metadata_columns
        self.content_formatter = content_formatter
        self.encoding = encoding
        self.source_name = source_name or file_path
    
    def _default_content_formatter(self, row: Dict[str, Any], columns: List[str]) -> str:
        """默认的内容格式化器：将指定列格式化为 '列名: 值' 的形式"""
        parts = []
        for col in columns:
            value = row.get(col, "")
            if pd.notna(value) and str(value).strip():
                parts.append(f"{col}: {value}")
        return "\n".join(parts)
    
    def _build_metadata(self, row: Dict[str, Any], columns: List[str], row_index: int) -> Dict[str, Any]:
        """构建文档元数据"""
        metadata = {
            "source": self.source_name,
            "row_index": row_index
        }
        for col in columns:
            value = row.get(col, "")
            # 处理NaN值
            if pd.isna(value):
                metadata[col] = None
            else:
                metadata[col] = value
        return metadata
    
    def load(self) -> List[Document]:
        """
        加载CSV文件并转换为Document列表
        
        Returns:
            Document对象列表
        """
        # 读取CSV文件
        df = pd.read_csv(self.file_path, encoding=self.encoding)
        
        # 确定内容列和元数据列
        all_columns = df.columns.tolist()
        content_cols = self.content_columns if self.content_columns else all_columns
        metadata_cols = self.metadata_columns if self.metadata_columns else all_columns
        
        documents = []
        
        for idx, row in df.iterrows():
            row_dict = row.to_dict()
            
            # 生成文档内容
            if self.content_formatter:
                content = self.content_formatter(row_dict)
            else:
                content = self._default_content_formatter(row_dict, content_cols)
            
            # 跳过空内容
            if not content.strip():
                continue
            
            # 构建元数据
            metadata = self._build_metadata(row_dict, metadata_cols, idx)
            
            # 创建Document
            doc = Document(page_content=content, metadata=metadata)
            documents.append(doc)
        
        return documents
# ai code end


if __name__ == "__main__":
    # ai code begin && nums:42
    import os
    
    # 测试1: 加载亚马逊法规库
    print("=== 测试1: 加载亚马逊法规库 ===")
    amazon_path = r"D:\workspace\leite\python\risk_fda_rag\risk_rag_qa\data\processed\亚马逊法规库_20250919.csv"
    
    if os.path.exists(amazon_path):
        loader = RiskCSVLoader(
            file_path=amazon_path,
            content_columns=["受限品", "关键词"],
            metadata_columns=["受限品", "URL"],
            source_name="amazon_regulations"
        )
        docs = loader.load()
        print(f"加载了 {len(docs)} 个文档")
        print(f"\n示例文档:")
        print(f"内容: {docs[0].page_content}")
        print(f"元数据: {docs[0].metadata}")
    
    # 测试2: 加载FDA医疗器械
    print("\n=== 测试2: 加载FDA医疗器械 ===")
    fda_path = r"D:\workspace\leite\python\risk_fda_rag\risk_rag_qa\data\processed\美国fda医疗器械_总表1.csv"
    
    if os.path.exists(fda_path):
        # 使用自定义格式化器
        def fda_formatter(row: Dict[str, Any]) -> str:
            parts = []
            if row.get("DEVICENAME"):
                parts.append(f"设备名称: {row['DEVICENAME']}")
            if row.get("DEVICECLASS"):
                parts.append(f"设备分类: Class {row['DEVICECLASS']}")
            if row.get("医学专科"):
                parts.append(f"医学专科: {row['医学专科']}")
            if row.get("REGULATIONNUMBER"):
                parts.append(f"法规编号: {row['REGULATIONNUMBER']}")
            return "\n".join(parts)
        
        loader = RiskCSVLoader(
            file_path=fda_path,
            content_formatter=fda_formatter,
            metadata_columns=["PRODUCTCODE", "DEVICECLASS", "医学专科"],
            source_name="fda_devices"
        )
        docs = loader.load()
        print(f"加载了 {len(docs)} 个文档")
        print(f"\n示例文档:")
        print(f"内容: {docs[0].page_content}")
        print(f"元数据: {docs[0].metadata}")
    # ai code end

