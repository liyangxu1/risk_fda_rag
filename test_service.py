"""
测试服务是否能正常启动和运行
"""

import sys
import traceback

def test_imports():
    """测试所有模块导入"""
    print("=" * 60)
    print("测试模块导入...")
    print("=" * 60)
    
    try:
        from app.api.main import app
        print("✓ app.api.main 导入成功")
    except Exception as e:
        print(f"✗ app.api.main 导入失败: {e}")
        traceback.print_exc()
        return False
    
    try:
        from app.agents import RiskSearchAgent, DocumentAgent, ClassificationAgent
        print("✓ Agent模块导入成功")
    except Exception as e:
        print(f"✗ Agent模块导入失败: {e}")
        traceback.print_exc()
        return False
    
    try:
        from app.services import VectorService, DocumentService
        print("✓ Service模块导入成功")
    except Exception as e:
        print(f"✗ Service模块导入失败: {e}")
        traceback.print_exc()
        return False
    
    try:
        from app.a2a import AgentCard, A2AProtocol, HTTPTransport
        print("✓ A2A模块导入成功")
    except Exception as e:
        print(f"✗ A2A模块导入失败: {e}")
        traceback.print_exc()
        return False
    
    try:
        from app.api.routes import agent_routes, health_routes
        print("✓ 路由模块导入成功")
    except Exception as e:
        print(f"✗ 路由模块导入失败: {e}")
        traceback.print_exc()
        return False
    
    try:
        from app.api.schemas import AgentTask, AgentResponse
        print("✓ Schema模块导入成功")
    except Exception as e:
        print(f"✗ Schema模块导入失败: {e}")
        traceback.print_exc()
        return False
    
    return True

def test_agent_initialization():
    """测试Agent初始化"""
    print("\n" + "=" * 60)
    print("测试Agent初始化...")
    print("=" * 60)
    
    try:
        from app.agents import RiskSearchAgent, DocumentAgent, ClassificationAgent
        
        risk_agent = RiskSearchAgent()
        print(f"✓ RiskSearchAgent 初始化成功: {risk_agent.get_agent_card()['name']}")
        
        doc_agent = DocumentAgent()
        print(f"✓ DocumentAgent 初始化成功: {doc_agent.get_agent_card()['name']}")
        
        cls_agent = ClassificationAgent()
        print(f"✓ ClassificationAgent 初始化成功: {cls_agent.get_agent_card()['name']}")
        
        return True
    except Exception as e:
        print(f"✗ Agent初始化失败: {e}")
        traceback.print_exc()
        return False

def test_fastapi_app():
    """测试FastAPI应用"""
    print("\n" + "=" * 60)
    print("测试FastAPI应用...")
    print("=" * 60)
    
    try:
        from app.api.main import app
        
        # 检查路由
        routes = [route.path for route in app.routes]
        print(f"✓ FastAPI应用创建成功")
        print(f"  注册的路由数量: {len(routes)}")
        print(f"  主要路由: {routes[:5]}...")
        
        return True
    except Exception as e:
        print(f"✗ FastAPI应用测试失败: {e}")
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("\n开始测试服务...\n")
    
    results = []
    results.append(("模块导入", test_imports()))
    results.append(("Agent初始化", test_agent_initialization()))
    results.append(("FastAPI应用", test_fastapi_app()))
    
    print("\n" + "=" * 60)
    print("测试结果汇总")
    print("=" * 60)
    
    for name, result in results:
        status = "✓ 通过" if result else "✗ 失败"
        print(f"{name}: {status}")
    
    all_passed = all(result for _, result in results)
    
    if all_passed:
        print("\n🎉 所有测试通过！服务可以正常启动。")
        print("\n启动命令: python main.py")
        print("或使用: uvicorn app.api.main:app --reload --host 0.0.0.0 --port 8000")
        sys.exit(0)
    else:
        print("\n⚠️  部分测试失败，请检查错误信息。")
        sys.exit(1)

