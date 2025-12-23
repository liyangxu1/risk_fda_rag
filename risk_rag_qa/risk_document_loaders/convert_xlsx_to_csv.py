import pandas as pd
import os

# ai code begin && nums:35
def inspect_xlsx(xlsx_path: str):
    """
    检查xlsx文件的所有工作表信息
    """
    xl = pd.ExcelFile(xlsx_path)
    print(f"=== 文件: {xlsx_path} ===")
    print(f"工作表数量: {len(xl.sheet_names)}")
    print(f"工作表名称: {xl.sheet_names}")
    print()
    
    for sheet in xl.sheet_names:
        df = pd.read_excel(xlsx_path, sheet_name=sheet)
        print(f"--- 工作表: {sheet} ---")
        print(f"行数: {len(df)}, 列数: {len(df.columns)}")
        print(f"列名: {df.columns.tolist()}")
        print(f"前3行:")
        print(df.head(3))
        print()


def convert_xlsx_to_csv(xlsx_path: str, csv_path: str = None, sheet_name: str | int = 0):
    """
    将xlsx文件转换为csv文件
    """
    if csv_path is None:
        base_name = xlsx_path.replace('.xlsx', '')
        csv_path = f"{base_name}_{sheet_name}.csv" if isinstance(sheet_name, str) else f"{base_name}_sheet{sheet_name}.csv"
    
    df = pd.read_excel(xlsx_path, sheet_name=sheet_name)
    df.to_csv(csv_path, index=False, encoding='utf-8-sig')
    print(f"转换完成: {csv_path}")
    print(f"行数: {len(df)}, 列数: {len(df.columns)}")
    return csv_path
# ai code end


if __name__ == "__main__":
    # ai code begin && nums:8
    # 转换FDA医疗器械的"总表1"工作表为CSV
    # 原始文件在 raw/ 目录，处理后文件保存到 processed/ 目录
    # xlsx_file = r"D:\workspace\leite\python\risk_fda_rag\risk_rag_qa\data\raw\美国fda医疗器械.xlsx"
    # csv_file = r"D:\workspace\leite\python\risk_fda_rag\risk_rag_qa\data\processed\美国fda医疗器械_总表1.csv"
    xlsx_file = r"D:\workspace\leite\python\risk_fda_rag\risk_rag_qa\data\raw\产品库标题向量数据.xlsx"
    csv_file = r"D:\workspace\leite\python\risk_fda_rag\risk_rag_qa\data\processed\产品库标题向量数据.csv"
    convert_xlsx_to_csv(xlsx_file, csv_file, sheet_name="Sheet1")
    # ai code end

