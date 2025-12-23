import pandas as pd
from typing import List, Dict, Any

# ai code begin && nums:85
class FDADeviceDocument:
    """FDA医疗器械文档类，适合RAG使用"""
    
    def __init__(self, content: str, metadata: Dict[str, Any]):
        self.page_content = content
        self.metadata = metadata
    
    def __repr__(self):
        return f"FDADeviceDocument(content={self.page_content[:100]}...)"


def load_fda_devices(file_path: str, sheet_name: str = "总表1") -> List[FDADeviceDocument]:
    """
    加载FDA医疗器械数据，转换为RAG文档格式
    
    Args:
        file_path: Excel文件路径
        sheet_name: 工作表名称
    
    Returns:
        FDADeviceDocument列表
    """
    df = pd.read_excel(file_path, sheet_name=sheet_name)
    documents = []
    
    for idx, row in df.iterrows():
        # 构建文档内容 - 将关键信息组合成易于检索的文本
        content_parts = []
        
        # 设备基本信息
        device_name = str(row.get('DEVICENAME', '')).strip()
        device_class = str(row.get('DEVICECLASS', '')).strip()
        product_code = str(row.get('PRODUCTCODE', '')).strip()
        
        content_parts.append(f"设备名称: {device_name}")
        content_parts.append(f"设备分类: Class {device_class}")
        content_parts.append(f"产品代码: {product_code}")
        
        # 医学专科
        specialty_cn = str(row.get('医学专科', '')).strip()
        specialty_en = str(row.get('MEDICALSPECIALTY', '')).strip()
        if specialty_cn:
            content_parts.append(f"医学专科: {specialty_cn} ({specialty_en})")
        
        # 法规信息
        regulation_num = str(row.get('REGULATIONNUMBER', '')).strip()
        regulation_citation = str(row.get('法规大类 Regulation Citation (21CFR)', '')).strip()
        if regulation_num and regulation_num != 'nan':
            content_parts.append(f"法规编号: {regulation_citation} {regulation_num}")
        
        # 产品类型
        product_type = str(row.get('产品类型', '')).strip()
        if product_type and product_type != 'nan':
            content_parts.append(f"产品类型: {product_type}")
        
        # 产品定义
        definition = str(row.get('DEFINITION', '')).strip()
        if definition and definition != 'nan':
            content_parts.append(f"产品定义: {definition}")
        
        # 特殊标识
        implant_flag = str(row.get('Implant_Flag', '')).strip()
        life_sustain_flag = str(row.get('Life_Sustain_support_flag', '')).strip()
        if implant_flag == 'Y':
            content_parts.append("特性: 植入物")
        if life_sustain_flag == 'Y':
            content_parts.append("特性: 生命支持设备")
        
        # 组合内容
        content = "\n".join(content_parts)
        
        # 构建元数据
        metadata = {
            "source": file_path,
            "product_code": product_code,
            "device_name": device_name,
            "device_class": device_class,
            "specialty": specialty_cn,
            "regulation_number": regulation_num,
            "product_type": product_type,
            "is_implant": implant_flag == 'Y',
            "is_life_sustain": life_sustain_flag == 'Y',
            "row_index": idx
        }
        
        doc = FDADeviceDocument(content=content, metadata=metadata)
        documents.append(doc)
    
    return documents
# ai code end


if __name__ == "__main__":
    # ai code begin && nums:17
    # 测试加载FDA数据
    file_path = r"D:\workspace\leite\python\risk_fda_rag\risk_rag_qa\data\raw\美国fda医疗器械.xlsx"
    
    print("正在加载FDA医疗器械数据...")
    documents = load_fda_devices(file_path)
    
    print(f"\n总共加载了 {len(documents)} 个设备文档")
    
    print("\n=== 示例文档 1 ===")
    print(documents[0].page_content)
    print(f"\n元数据: {documents[0].metadata}")
    
    print("\n=== 示例文档 100 ===")
    print(documents[100].page_content)
    print(f"\n元数据: {documents[100].metadata}")
    # ai code end

