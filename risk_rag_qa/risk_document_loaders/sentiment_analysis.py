# ai code begin && nums:180
"""
评论情感分析脚本
使用 Coze API 对用户评论进行情感分析，判断评论对销售的影响
返回结果：1-促进销售，2-阻碍销售，3-无影响
"""
import pandas as pd
import httpx
import time
import json
import re
from typing import Dict, Any, Optional


class SentimentAnalyzer:
    """评论情感分析器"""
    
    # Coze API 配置（用于请求对应的工作流）
    API_URL = "https://api.coze.cn/v1/workflows/chat"
    API_KEY = "pat_Ht0Yo5rjW5Fvb2u84EkatBEbXTXCu6UzYlDgwAWw1KrsvUOkksdvtiI2OhtJa0Zs"
    

    WORKFLOW_ID = "7586946762297753642"
    
    def __init__(self):
        """初始化 Coze API 配置"""
        import os
        
        # 支持从环境变量读取配置（可选，主要用于不同环境）
        self.api_url = self.API_URL
        self.api_key = os.getenv("COZE_API_KEY", self.API_KEY)
        self.workflow_id = os.getenv("COZE_WORKFLOW_ID", self.WORKFLOW_ID)
        
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        print(f"✅ Coze工作流配置已加载")
        print(f"   Workflow ID: {self.workflow_id}")
        print(f"   API Key: {self.api_key[:20]}...{self.api_key[-10:]}")
        
    def analyze_sentiment(self, comment: str, retry_count: int = 3) -> Optional[int]:
        """
        调用 Coze API 进行情感分析
        
        Args:
            comment: 评论内容
            retry_count: 重试次数
            
        Returns:
            情感分析结果：1-促进销售，2-阻碍销售，3-无影响，None-分析失败
        """
        if not comment or pd.isna(comment) or str(comment).strip() == "":
            return None
            
        payload = {
            "workflow_id": self.workflow_id,
            "parameters": {
                "CONVERSATION_NAME": "Default",
                "USER_INPUT": str(comment),
                "product_detail": "test"
            },
            "additional_messages": [
                {
                    "content": str(comment),
                    "content_type": "text",
                    "role": "user",
                    "type": "question"
                }
            ]
        }
        
        for attempt in range(retry_count):
            try:
                print(f"📤 正在调用API（尝试 {attempt + 1}/{retry_count}）...")
                with httpx.Client(timeout=60.0) as client:  # 增加超时时间到60秒
                    response = client.post(
                        self.api_url,
                        headers=self.headers,
                        json=payload
                    )
                
                print(f"✅ API调用完成，状态码: {response.status_code}")
                
                if response.status_code == 200:
                    response_text = response.text
                    content_type = response.headers.get('Content-Type', '')
                    
                    print(f"📥 响应类型: {content_type}")
                    print(f"📏 响应长度: {len(response_text)} 字符")
                    
                    # 检查是否是SSE流式响应
                    if 'text/event-stream' in content_type:
                        print("🔄 检测到SSE流式响应，开始解析...")
                        # 解析SSE格式的响应
                        sentiment_result = self._parse_sse_response(response_text, comment[:50] if len(comment) > 50 else comment)
                        if sentiment_result in [1, 2, 3]:
                            print(f"✅ 成功提取情感分析结果: {sentiment_result}")
                            return sentiment_result
                        else:
                            print(f"⚠️  无法从SSE响应中提取结果，响应预览: {response_text[:200]}...")
                            if attempt < retry_count - 1:
                                time.sleep(2 ** attempt)
                                continue
                            return None
                    else:
                        # 普通JSON响应
                        if not response_text or response_text.strip() == "":
                            print(f"⚠️  API返回空响应，尝试 {attempt + 1}/{retry_count}")
                            if attempt < retry_count - 1:
                                time.sleep(2 ** attempt)
                                continue
                            return None
                        
                        # 尝试解析JSON
                        try:
                            result = response.json()
                            # 解析返回结果，提取情感分析结果（1/2/3）
                            sentiment_result = self._parse_response(result, comment[:50] if len(comment) > 50 else comment)
                            if sentiment_result in [1, 2, 3]:
                                return sentiment_result
                            else:
                                print(f"⚠️  警告：API返回了意外的结果: {sentiment_result}")
                                return None
                        except json.JSONDecodeError as e:
                            print(f"⚠️  JSON解析失败: {str(e)}")
                            if attempt < retry_count - 1:
                                time.sleep(2 ** attempt)
                                continue
                            return None
                elif response.status_code == 401:
                    # 401认证错误，不需要重试，返回特殊标记-1
                    error_info = response.text
                    try:
                        error_json = response.json()
                        error_msg = error_json.get("msg", "认证失败")
                    except:
                        error_msg = error_info
                    print(f"❌ API认证失败（401）: {error_msg}")
                    print(f"   请检查 API Key 是否正确或已过期")
                    print(f"   当前使用的 API Key: {self.api_key[:20]}...{self.api_key[-10:]}")
                    print(f"   请在代码中修改 SentimentAnalyzer.API_KEY 或设置环境变量 COZE_API_KEY")
                    return -1  # 返回-1作为401错误的特殊标记
                else:
                    # 其他HTTP错误
                    response_text = response.text[:500] if response.text else "无响应内容"
                    print(f"⚠️  API调用失败，状态码: {response.status_code}")
                    print(f"   响应内容: {response_text}")
                    print(f"   响应头: {dict(response.headers)}")
                    # 对于非401错误，可以重试
                    if attempt < retry_count - 1:
                        time.sleep(2 ** attempt)  # 指数退避
                        continue
                    return None
                    
            except httpx.TimeoutException:
                print(f"⚠️  请求超时，尝试 {attempt + 1}/{retry_count}")
                if attempt < retry_count - 1:
                    time.sleep(2 ** attempt)
                    continue
                return None
            except json.JSONDecodeError as e:
                print(f"⚠️  JSON解析错误: {str(e)}")
                print(f"   尝试 {attempt + 1}/{retry_count}")
                if attempt < retry_count - 1:
                    time.sleep(2 ** attempt)
                    continue
                return None
            except Exception as e:
                print(f"⚠️  请求异常: {str(e)}")
                print(f"   异常类型: {type(e).__name__}")
                print(f"   尝试 {attempt + 1}/{retry_count}")
                if attempt < retry_count - 1:
                    time.sleep(2 ** attempt)
                    continue
                return None
        
        return None
    
    def _parse_sse_response(self, sse_text: str, comment_preview: str = "") -> Optional[int]:
        """
        解析SSE (Server-Sent Events) 格式的响应
        
        Args:
            sse_text: SSE格式的响应文本
            comment_preview: 评论预览（用于调试）
            
        Returns:
            情感分析结果：1/2/3 或 None
        """
        try:
            if not sse_text or len(sse_text.strip()) == 0:
                print("⚠️  SSE响应为空")
                return None
            
            # 解析SSE格式：每行以 event: 或 data: 开头
            lines = sse_text.split('\n')
            current_event = None
            found_completed = False
            
            print(f"📝 开始解析SSE响应，共 {len(lines)} 行")
            
            for i, line in enumerate(lines):
                line = line.strip()
                if not line:
                    continue
                
                if line.startswith('event:'):
                    current_event = line[6:].strip()
                    if current_event == 'conversation.message.completed':
                        found_completed = True
                        print(f"✅ 找到 conversation.message.completed 事件（第 {i+1} 行）")
                elif line.startswith('data:'):
                    if current_event == 'conversation.message.completed':
                        data_content = line[5:].strip()
                        print(f"📦 解析data内容（第 {i+1} 行）...")
                        # 解析data中的JSON
                        try:
                            data_json = json.loads(data_content)
                            # content字段是一个JSON字符串
                            content_str = data_json.get('content', '')
                            if content_str:
                                print(f"📄 content字段长度: {len(content_str)} 字符")
                                # 解析content为JSON
                                try:
                                    content_json = json.loads(content_str)
                                    output_text = content_json.get('output', '')
                                    if output_text:
                                        print(f"💬 output文本: {output_text[:100]}...")
                                        # 从output文本中提取数字 1、2、3
                                        # 优先匹配"输出1"、"输出2"、"输出3"
                                        match = re.search(r'输出\s*([123])', output_text)
                                        if match:
                                            result = int(match.group(1))
                                            print(f"✅ 匹配到'输出{result}'")
                                            return result
                                        # 如果没有"输出"字样，查找文本末尾的数字
                                        match = re.search(r'([123])(?![0-9])', output_text[-100:])
                                        if match:
                                            result = int(match.group(1))
                                            print(f"✅ 在文本末尾找到数字: {result}")
                                            return result
                                        print(f"⚠️  未能在output文本中找到数字1/2/3")
                                except json.JSONDecodeError as e:
                                    print(f"⚠️  content不是有效JSON: {str(e)}")
                                    # content不是JSON，直接搜索文本
                                    match = re.search(r'输出\s*([123])', content_str)
                                    if match:
                                        result = int(match.group(1))
                                        print(f"✅ 在content字符串中找到'输出{result}'")
                                        return result
                                    match = re.search(r'([123])(?![0-9])', content_str[-100:])
                                    if match:
                                        result = int(match.group(1))
                                        print(f"✅ 在content字符串末尾找到数字: {result}")
                                        return result
                            else:
                                print("⚠️  content字段为空")
                        except json.JSONDecodeError as e:
                            print(f"⚠️  解析data JSON失败: {str(e)}")
                            print(f"   data内容预览: {data_content[:200]}")
                            continue
            
            if not found_completed:
                print("⚠️  未找到 conversation.message.completed 事件")
                print(f"   响应预览: {sse_text[:500]}...")
            
            return None
            
        except Exception as e:
            print(f"⚠️  解析SSE响应时出错: {str(e)}")
            import traceback
            traceback.print_exc()
            return None
    
    def _parse_response(self, response_data: Dict[str, Any], comment_preview: str = "") -> Optional[int]:
        """
        解析API返回结果，提取情感分析结果
        
        Args:
            response_data: API返回的JSON数据
            comment_preview: 评论预览（用于调试）
            
        Returns:
            情感分析结果：1/2/3 或 None
        """
        try:
            # 如果响应本身就是数字
            if isinstance(response_data, (int, float)):
                result = int(response_data)
                if result in [1, 2, 3]:
                    return result
            
            # 如果响应是字符串，尝试转换为数字
            if isinstance(response_data, str):
                result = response_data.strip()
                if result in ["1", "2", "3"]:
                    return int(result)
            
            # 如果响应是字典，尝试多种可能的字段
            if isinstance(response_data, dict):
                # 常见的字段名
                possible_keys = [
                    "result", "data", "output", "message", "content", 
                    "sentiment", "sentiment_result", "value", "code",
                    "response", "answer", "text"
                ]
                
                for key in possible_keys:
                    if key in response_data:
                        value = response_data[key]
                        # 如果值是数字
                        if isinstance(value, (int, float)):
                            result = int(value)
                            if result in [1, 2, 3]:
                                return result
                        # 如果值是字符串
                        elif isinstance(value, str):
                            result = value.strip()
                            if result in ["1", "2", "3"]:
                                return int(result)
                        # 如果值是字典，递归查找
                        elif isinstance(value, dict):
                            nested_result = self._parse_response(value, comment_preview)
                            if nested_result is not None:
                                return nested_result
                
                # 如果有关键字段包含列表，遍历列表查找
                if "messages" in response_data:
                    messages = response_data["messages"]
                    if isinstance(messages, list):
                        for msg in messages:
                            if isinstance(msg, dict):
                                nested_result = self._parse_response(msg, comment_preview)
                                if nested_result is not None:
                                    return nested_result
                
                # 最后尝试：从整个响应JSON字符串中查找数字
                response_str = json.dumps(response_data, ensure_ascii=False)
                # 查找独立的数字 1, 2, 3（避免匹配到其他数字如10, 20等）
                # 查找被引号包围的 "1", "2", "3"
                quoted_match = re.search(r'"([123])"', response_str)
                if quoted_match:
                    return int(quoted_match.group(1))
                # 查找独立的数字（前后是冒号、逗号或大括号）
                standalone_match = re.search(r'[:\s,{]([123])[,\s}]', response_str)
                if standalone_match:
                    return int(standalone_match.group(1))
            
            # 如果无法解析，打印调试信息（仅前3次）
            if not hasattr(self, '_parse_error_count'):
                self._parse_error_count = 0
            if self._parse_error_count < 3:
                print(f"⚠️  无法解析API响应（评论: {comment_preview}...）")
                print(f"   响应内容: {json.dumps(response_data, ensure_ascii=False, indent=2)[:500]}")
                self._parse_error_count += 1
            
            return None
            
        except Exception as e:
            print(f"⚠️  解析响应时出错: {str(e)}")
            return None
    
    def analyze_batch(
        self, 
        csv_path: str, 
        output_path: Optional[str] = None,
        start_idx: int = 0,
        end_idx: Optional[int] = None,
        delay: float = 0.5
    ):
        """
        批量分析评论情感
        
        Args:
            csv_path: 输入CSV文件路径
            output_path: 输出CSV文件路径（如果为None，则在原文件名后加_情感分析）
            start_idx: 开始索引（用于分批处理）
            end_idx: 结束索引（如果为None，则处理到最后）
            delay: API调用间隔（秒），避免限流
        """
        # 确定输出文件路径
        if output_path is None:
            output_path = csv_path.replace(".csv", "_情感分析.csv")
        
        # 优先读取输出文件（如果存在），实现断点续传
        import os
        if os.path.exists(output_path):
            print(f"📖 检测到已存在的输出文件: {output_path}")
            print(f"✅ 将从断点处继续处理...")
            try:
                df = pd.read_csv(output_path, encoding='utf-8-sig')
            except UnicodeDecodeError:
                try:
                    df = pd.read_csv(output_path, encoding='utf-8')
                except:
                    df = pd.read_csv(output_path, encoding='gbk')
            print(f"📊 从输出文件读取了 {len(df)} 条数据")
        else:
            # 如果输出文件不存在，读取原始文件
            print(f"📖 读取原始数据文件: {csv_path}")
            try:
                df = pd.read_csv(csv_path, encoding='utf-8')
            except UnicodeDecodeError:
                # 尝试其他编码
                df = pd.read_csv(csv_path, encoding='gbk')
            print(f"📊 从原始文件读取了 {len(df)} 条数据")
        
        if end_idx is None:
            end_idx = len(df)
        
        # 检查是否已有情感分析列
        if "情感分析" in df.columns:
            print("✅ 检测到已存在'情感分析'列")
        else:
            # 在第一列位置插入情感分析列
            df.insert(0, "情感分析", "")
            print("✅ 已在第一列插入'情感分析'列")
        
        # 显示处理范围
        total_to_process = end_idx - start_idx
        print(f"📊 数据总行数: {len(df)}")
        print(f"📍 起始行号: {start_idx} (第 {start_idx + 1} 条)")
        if end_idx < len(df):
            print(f"📍 结束行号: {end_idx - 1} (第 {end_idx} 条)")
        else:
            print(f"📍 结束行号: {len(df) - 1} (最后一条)")
        print(f"📊 将处理: {total_to_process} 条数据")
        
        # 统计当前进度（仅用于显示）
        valid_results = ["促进销售", "阻碍销售", "无影响"]
        processed_count = 0
        empty_data_count = 0
        failed_count = 0
        
        for idx in range(start_idx, min(end_idx, len(df))):
            row = df.iloc[idx]
            sentiment = str(row.get("情感分析", "")).strip()
            
            # 获取评论内容
            comment = str(row.get("评论内容", ""))
            if not comment or comment == "nan" or comment.strip() == "":
                comment = str(row.get("评论内容(中文)", ""))
            
            if not comment or comment == "nan" or comment.strip() == "":
                empty_data_count += 1
            elif sentiment in valid_results:
                processed_count += 1
            elif sentiment == "分析失败":
                failed_count += 1
        
        # 显示当前进度统计
        if start_idx > 0 or processed_count > 0:
            print("\n" + "=" * 60)
            print("📊 当前进度统计（处理范围内）:")
            print("=" * 60)
            print(f"✅ 已成功处理: {processed_count} 条")
            print(f"❌ 分析失败: {failed_count} 条")
            print(f"⏭️  数据为空: {empty_data_count} 条")
            print(f"⏳ 待处理: {total_to_process - processed_count - failed_count - empty_data_count} 条")
            print("=" * 60 + "\n")
        
        # 统计变量
        success_count = 0
        fail_count = 0
        skip_count = 0
        
        # 逐条分析
        print("🚀 开始情感分析...")
        total_rows = min(end_idx, len(df)) - start_idx
        for idx in range(start_idx, min(end_idx, len(df))):
            current_num = idx - start_idx + 1
            if current_num % 10 == 0 or current_num == 1:
                print(f"📊 处理进度: {current_num}/{total_rows} ({current_num/total_rows*100:.1f}%) - 当前行号: {idx}")
            row = df.iloc[idx]
            
            # 获取评论内容（优先使用原文）
            comment = str(row.get("评论内容", ""))
            if not comment or comment == "nan" or comment.strip() == "":
                # 如果原文为空，尝试使用中文翻译
                comment = str(row.get("评论内容(中文)", ""))
            
            # 跳过空数据
            if not comment or comment == "nan" or comment.strip() == "":
                df.at[idx, "情感分析"] = "数据为空"
                skip_count += 1
                continue
            
            # 调用API分析
            result = self.analyze_sentiment(comment)
            
            # 检查是否是401认证错误（通过检查结果是否为特殊标记）
            if result == -1:  # 使用-1作为401错误的特殊标记
                print("\n" + "=" * 50)
                print("❌ 检测到认证错误，停止批量处理")
                print("=" * 50)
                print("请先修复API Key配置后重新运行")
                # 保存已处理的数据
                df.to_csv(output_path, index=False, encoding='utf-8-sig')
                print(f"已保存当前进度到: {output_path}")
                return
            
            if result is not None:
                # 转换为可读的标签
                sentiment_label = {
                    1: "促进销售",
                    2: "阻碍销售",
                    3: "无影响"
                }.get(result, str(result))
                
                df.at[idx, "情感分析"] = sentiment_label
                success_count += 1
            else:
                df.at[idx, "情感分析"] = "分析失败"
                fail_count += 1
            
            # 避免API限流，添加延迟
            if delay > 0:
                time.sleep(delay)
            
            # 每10条保存一次（防止中途中断）
            if (idx + 1) % 10 == 0:
                df.to_csv(output_path, index=False, encoding='utf-8-sig')
                print(f"💾 已保存进度到: {output_path}")
        
        # 保存最终结果
        df.to_csv(output_path, index=False, encoding='utf-8-sig')
        print(f"\n✅ 分析完成！结果已保存到: {output_path}")
        
        # 统计结果
        print("\n" + "=" * 50)
        print("📈 分析结果统计:")
        print("=" * 50)
        sentiment_counts = df["情感分析"].value_counts()
        print(sentiment_counts)
        
        print(f"\n✅ 本次成功分析: {success_count} 条")
        print(f"❌ 本次分析失败: {fail_count} 条")
        print(f"⏭️  本次跳过（数据为空）: {skip_count} 条")
        print(f"📊 本次总计处理: {success_count + fail_count + skip_count} 条")
        
        # 最终统计
        final_processed = len(df[df["情感分析"].isin(valid_results)])
        final_failed = len(df[df["情感分析"] == "分析失败"])
        final_empty = len(df[df["情感分析"] == "数据为空"])
        final_pending = len(df) - final_processed - final_failed - final_empty
        
        print("\n" + "=" * 60)
        print("📊 最终统计:")
        print("=" * 60)
        print(f"✅ 已成功处理: {final_processed} 条")
        print(f"❌ 分析失败: {final_failed} 条")
        print(f"⏭️  数据为空: {final_empty} 条")
        print(f"⏳ 待处理: {final_pending} 条")
        print(f"📈 总完成度: {final_processed}/{len(df)} ({final_processed/len(df)*100:.1f}%)")
        print("=" * 60)


if __name__ == "__main__":
    import sys
    
    # ==================== 配置区域 ====================
    # 数据文件路径
    csv_path = "../data/processed/处理后评论分析.csv"
    
    # 断点续传：从第几行开始处理（从0开始计数，0表示从第一行开始）
    # 例如：如果已经处理了100条，这里填100，就会从第101条开始处理
    RESUME_FROM_LINE = 23  # ⬅️ 在这里手动填入起始行号
    
    # API调用间隔（秒），避免限流
    delay = 0.1
    # ================================================
    
    print("=" * 50)
    print("开始评论情感分析...")
    print("=" * 50)
    
    # 支持命令行参数或直接修改下面的配置
    # 用法1: python sentiment_analysis.py                    # 处理全部数据（从RESUME_FROM_LINE开始）
    # 用法2: python sentiment_analysis.py 100                # 处理前100条（从RESUME_FROM_LINE开始）
    # 用法3: python sentiment_analysis.py 100 200          # 处理第100-200条（从RESUME_FROM_LINE开始）
    
    start_idx = RESUME_FROM_LINE
    end_idx = None
    
    if len(sys.argv) > 1:
        # 如果提供了参数
        try:
            if len(sys.argv) == 2:
                # 只有一个参数：处理前N条（从RESUME_FROM_LINE开始）
                end_idx = start_idx + int(sys.argv[1])
                print(f"📊 模式：从第 {start_idx} 行开始，处理 {int(sys.argv[1])} 条数据")
            elif len(sys.argv) == 3:
                # 两个参数：从M到N（覆盖RESUME_FROM_LINE）
                start_idx = int(sys.argv[1])
                end_idx = int(sys.argv[2])
                print(f"📊 模式：处理第 {start_idx} 到 {end_idx-1} 行（共 {end_idx - start_idx} 条）")
            else:
                print("❌ 参数错误！")
                print("用法：")
                print("  python sentiment_analysis.py                    # 处理全部数据（从RESUME_FROM_LINE开始）")
                print("  python sentiment_analysis.py 100                # 从RESUME_FROM_LINE开始处理100条")
                print("  python sentiment_analysis.py 100 200            # 处理第100-200行（覆盖RESUME_FROM_LINE）")
                sys.exit(1)
        except ValueError:
            print("❌ 参数必须是数字！")
            sys.exit(1)
    else:
        # 没有参数：处理全部数据（从RESUME_FROM_LINE开始）
        if start_idx > 0:
            print(f"📊 模式：从第 {start_idx} 行开始处理全部剩余数据")
        else:
            print("📊 模式：处理全部数据")
    
    print(f"⏱️  API调用间隔: {delay} 秒")
    print("-" * 50)
    
    # 初始化分析器（使用类中配置的token和workflow_id）
    analyzer = SentimentAnalyzer()
    
    # 执行分析
    analyzer.analyze_batch(
        csv_path, 
        start_idx=start_idx, 
        end_idx=end_idx,
        delay=delay
    )
# ai code end

