import pandas as pd

# ai code begin && nums:15
def read_excel_data(file_path: str, sheet_name: str | int = 0) -> pd.DataFrame:
    """
    读取Excel文件数据

    Args:
        file_path: Excel文件路径
        sheet_name: 工作表名称或索引，默认为第一个工作表

    Returns:
        DataFrame数据
    """
    df = pd.read_excel(file_path, sheet_name=sheet_name)
    return df
# ai code end


if __name__ == "__main__":
    # ai code begin && nums:12
    # 读取亚马逊法规库 - 这是真正的数据表格
    # 列名: ['受限品', '关键词', 'URL']
    excel_path = r"D:\workspace\leite\python\risk_fda_rag\risk_rag_qa\data\raw\亚马逊法规库_20250919.xlsx"
    df = read_excel_data(excel_path)

    print("=== 数据信息 ===")
    print(f"行数: {len(df)}, 列数: {len(df.columns)}")
    print(f"列名: {df.columns.tolist()}")
    print()
    print("=== 前10行数据 ===")
    print(df.head(10))
    # ai code end
