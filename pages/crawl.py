import asyncio
import sys
import os
import traceback

# 尝试导入streamlit
import streamlit as st

# 添加项目根目录到Python路径，确保能正确导入模块
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 导入配置类 - 参照base_config.py中的配置结构
from config import base_config

from media_platform.bilibili import BilibiliCrawler
from media_platform.douyin import DouYinCrawler
from media_platform.kuaishou import KuaishouCrawler
from media_platform.tieba import TieBaCrawler
from media_platform.weibo import WeiboCrawler
from media_platform.xhs import XiaoHongShuCrawler
from media_platform.zhihu import ZhihuCrawler
from tools.async_file_writer import AsyncFileWriter

# 初始化变量
search_keywords = ""
detail_urls = ""
creator_urls = ""
max_count = 500
crawler_type = "search"
save_media = True
enable_proxy = False
enable_cdp = True
gap_time = 5
login_type = "qrcode"
cookies = ""
headless = False
enable_comments = True
enable_sub_comments = True
max_comments_count = 10000
platform_key = "bili"
selected_platform = "哔哩哔哩"
config_params = {}

# 浏览器配置
cdp_debug_port = 9222
cdp_headless = False

# 代理配置
proxy_url = ""
proxy_pool_count = 2
proxy_provider = "kuaidaili"

# 数据保存配置
save_data_option = "sqlite"
enable_wordcloud = False

# 界面元素引用
col1 = None
col2 = None
progress_bar = None
log_container = None

# 初始化会话状态变量
if st:
    if "crawler_running" not in st.session_state:
        st.session_state.crawler_running = False
    if "crawler_progress" not in st.session_state:
        st.session_state.crawler_progress = 0
    if "crawler_logs" not in st.session_state:
        st.session_state.crawler_logs = []
    if "selected_platform" not in st.session_state:
        st.session_state.selected_platform = None
    if "crawler_task" not in st.session_state:
        st.session_state.crawler_task = None
    if "event_loop" not in st.session_state:
        st.session_state.event_loop = None

    st.title("数据采集")

    # 平台选择
    platform_options = {
        "小红书": "xhs",
        "抖音": "dy",
        "快手": "ks",
        "哔哩哔哩": "bili",
        "微博": "weibo",
        "贴吧": "tieba",
        "知乎": "zhihu",
    }
    
    # 从base_config获取默认平台，如果存在的话
    default_platform = "哔哩哔哩"  # 默认值参照base_config.py中的PLATFORM
    try:
        base_platform = base_config.PLATFORM
        for name, key in platform_options.items():
            if key == base_platform:
                default_platform = name
                break
    except Exception:
        pass
    
    # 获取默认索引
    default_index = list(platform_options.keys()).index(default_platform)
    
    selected_platform = st.selectbox(
        "选择平台", 
        placeholder="请选择平台", 
        options=list(platform_options.keys()), 
        index=default_index
    )
    platform_key = platform_options[selected_platform]
    st.session_state.selected_platform = selected_platform

# 爬虫配置区域
if st:
    st.subheader(f"{selected_platform}爬虫配置")
    
# 通用配置
if st:
    with st.expander("基本配置", expanded=True):
        # 爬取类型 - 参照base_config.py中的CRAWLER_TYPE配置
        crawler_type = st.radio(
            "爬取类型", 
            ["search(关键词搜索)", "detail(帖子详情)", "creator(创作者主页数据)"], 
            index=0,
            format_func=lambda x: x.split("(")[0]
        )
        crawler_type = crawler_type.split("(")[0]  # 提取类型标识符
        
        # 关键词搜索配置
        if crawler_type == "search":
            search_keywords = st.text_input(
                "搜索关键词", 
                placeholder="请输入搜索关键词，多个关键词用逗号分隔",
                value="老年机器人"
            )
        # 帖子详情配置
        elif crawler_type == "detail":
            detail_urls = st.text_area(
                "帖子详情URL", 
                placeholder="请输入帖子详情URL，多个URL用逗号或换行分隔"
            )
        # 创作者主页配置
        elif crawler_type == "creator":
            creator_urls = st.text_area(
                "创作者主页URL", 
                placeholder="请输入创作者主页URL，多个URL用逗号或换行分隔"
            )
        
        # 爬取数量控制
        max_count = st.number_input(
            "最大爬取数量", 
            min_value=1, 
            max_value=10000, 
            value=500
        )
        
        # 爬取间隔时间
        gap_time = st.number_input(
            "每次请求间隔时间（秒）", 
            min_value=1, 
            max_value=60, 
            value=5
        )
    
    # 媒体与评论配置
    with st.expander("媒体与评论配置", expanded=True):
        # 保存媒体文件选项
        save_media = st.checkbox("保存媒体文件", value=True)
        
        # 是否开启爬评论模式
        enable_comments = st.checkbox("爬取评论", value=True)
        
        # 爬取一级评论的数量控制
        if enable_comments:
            max_comments_count = st.number_input(
                "单帖子最大评论数", 
                min_value=1, 
                max_value=5000, 
                value=1000
            )
            
            # 是否开启爬二级评论模式
            enable_sub_comments = st.checkbox("爬取二级评论", value=True)
    
    # 登录配置
    with st.expander("登录配置", expanded=True):
        # 登录类型
        login_type = st.radio(
            "登录类型", 
            ["qrcode(扫码登录)", "phone(手机号登录)", "cookie(使用Cookie)"], 
            index=0,
            format_func=lambda x: x.split("(")[0]
        )
        login_type = login_type.split("(")[0]  # 提取类型标识符
        
        # Cookie配置
        if login_type == "cookie":
            cookies = st.text_area(
                "Cookie", 
                placeholder="请输入Cookie"
            )
        
        # 是否保存登录状态
        save_login_state = st.checkbox("保存登录状态", value=True)
    
    # 浏览器配置
    with st.expander("浏览器配置", expanded=True):
        # CDP模式配置
        enable_cdp = st.checkbox("启用CDP模式", value=True)
        
        if enable_cdp:
            # CDP调试端口
            cdp_debug_port = st.number_input(
                "CDP调试端口", 
                min_value=1024, 
                max_value=65535, 
                value=9222
            )
            
            # CDP模式下是否启用无头模式
            cdp_headless = st.checkbox("CDP无头模式", value=False)
        else:
            # 无头浏览器配置 - 参照base_config.py中的HEADLESS配置
            headless = st.checkbox("无头浏览器模式", value=False)
    
    # 代理配置
    with st.expander("代理配置", expanded=False):
        # 是否开启IP代理
        enable_proxy = st.checkbox("启用IP代理", value=False)
        
        if enable_proxy:
            # 代理IP池数量
            proxy_pool_count = st.number_input(
                "代理IP池数量", 
                min_value=1, 
                max_value=10, 
                value=2
            )
            
            # 代理IP提供商名称
            proxy_provider = st.selectbox(
                "代理IP提供商", 
                ["kuaidaili(快代理)", "wandouhttp(豌豆HTTP)"], 
                index=0,
                format_func=lambda x: x.split("(")[0]
            )
            proxy_provider = proxy_provider.split("(")[0]
            
            # 代理URL配置
            proxy_url = st.text_input(
                "代理URL", 
                placeholder="http://username:password@host:port"
            )
    
    # 数据保存配置
    with st.expander("数据保存配置", expanded=False):
        # 数据保存类型选项
        save_data_option = st.selectbox(
            "数据保存格式", 
            ["sqlite", "db", "csv", "json"], 
            index=0
        )
        
        # 词云配置
        enable_wordcloud = st.checkbox("生成评论词云图", value=False)

# 进度和日志显示区域
if st:
    st.divider()
    st.subheader("爬虫状态")

    # 进度条
    if st.session_state.crawler_running:
        progress_bar = st.progress(st.session_state.crawler_progress, text="爬虫运行中...")

    # 日志显示区域
    log_container = st.container(height=200, border=True)

    # 启动/停止按钮区域
    col1, col2 = st.columns(2)

# 爬虫运行函数
async def run_crawler():
    try:
        if st:
            st.session_state.crawler_running = True
            st.session_state.crawler_progress = 0
            st.session_state.crawler_logs = [f"开始启动{selected_platform}爬虫..."]
        else:
            print(f"开始启动{selected_platform}爬虫...")
        
        # 根据平台创建爬虫实例
        crawler = None
        config = None
        
        # 尝试导入平台特定的配置类
        platform_configs = {
            "xhs": "xhs_config.XHSConfig",
            "dy": "dy_config.DouYinConfig",
            "ks": "ks_config.KuaiShouConfig",
            "bili": "bilibili_config.BilibiliConfig",
            "weibo": "weibo_config.WeiboConfig",
            "tieba": "tieba_config.TieBaConfig",
            "zhihu": "zhihu_config.ZhihuConfig"
        }
        
        # 动态导入配置类
        if platform_key in platform_configs:
            try:
                module_name, class_name = platform_configs[platform_key].rsplit(".", 1)
                module = __import__(f"config.{module_name}", fromlist=[class_name])
                config_class = getattr(module, class_name)
                config = config_class()
            except Exception as e:
                if st:
                    st.session_state.crawler_logs.append(f"导入{selected_platform}配置类失败: {str(e)}")
                else:
                    print(f"导入{selected_platform}配置类失败: {str(e)}")
        
        # 创建爬虫实例
        try:
            crawler_map = {
                "xhs": XiaoHongShuCrawler,
                "dy": DouYinCrawler,
                "ks": KuaishouCrawler,
                "bili": BilibiliCrawler,
                "weibo": WeiboCrawler,
                "tieba": TieBaCrawler,
                "zhihu": ZhihuCrawler
            }
            
            crawler_class = crawler_map.get(platform_key)
            if crawler_class is not None:
                # 尝试使用不同的初始化方式
                try:
                    if config:
                        crawler = crawler_class(config)
                    else:
                        crawler = crawler_class()
                except TypeError:
                    try:
                        crawler = crawler_class()
                    except Exception:
                        pass
        except Exception as e:
            error_msg = f"创建{selected_platform}爬虫实例失败: {str(e)}"
            if st:
                st.session_state.crawler_logs.append(error_msg)
            else:
                print(error_msg)
            raise ImportError(error_msg)
        
        if not crawler:
            error_msg = f"未找到{selected_platform}爬虫模块或无法创建实例"
            if st:
                st.session_state.crawler_logs.append(error_msg)
            else:
                print(error_msg)
            raise ImportError(error_msg)
        
        # 更新配置 - 参照base_config.py中的配置项
        if config:
            # 基本配置
            if hasattr(config, 'max_count'):
                config.max_count = max_count
            if hasattr(config, 'crawler_type'):
                config.crawler_type = crawler_type
            if hasattr(config, 'login_type'):
                config.login_type = login_type
            if hasattr(config, 'cookies') and cookies:
                config.cookies = cookies
            
            # 媒体与评论配置
            if hasattr(config, 'enable_get_medias'):
                config.enable_get_medias = save_media
            if hasattr(config, 'enable_get_comments'):
                config.enable_get_comments = enable_comments
            if hasattr(config, 'enable_get_sub_comments'):
                config.enable_get_sub_comments = enable_sub_comments if enable_comments else False
            if hasattr(config, 'crawler_max_comments_count_singlenotes'):
                config.crawler_max_comments_count_singlenotes = max_comments_count
            
            # 浏览器配置
            if hasattr(config, 'headless'):
                config.headless = headless
            if hasattr(config, 'enable_cdp_mode'):
                config.enable_cdp_mode = enable_cdp
            if hasattr(config, 'cdp_debug_port'):
                config.cdp_debug_port = cdp_debug_port
            if hasattr(config, 'cdp_headless'):
                config.cdp_headless = cdp_headless
            
            # 代理配置
            if hasattr(config, 'enable_ip_proxy'):
                config.enable_ip_proxy = enable_proxy
            if enable_proxy and hasattr(config, 'proxy_url'):
                config.proxy_url = proxy_url
            
            # 数据保存配置
            if hasattr(config, 'save_data_option'):
                config.save_data_option = save_data_option
            if hasattr(config, 'enable_get_wordcloud'):
                config.enable_get_wordcloud = enable_wordcloud
        
        if st:
            st.session_state.crawler_logs.append("爬虫配置完成，开始执行任务...")
        else:
            print("爬虫配置完成，开始执行任务...")
        
        # 执行爬取任务 - 参照base_config.py中的CRAWLER_TYPE配置
        if crawler_type == "search":
            # 关键词搜索模式
            keywords = [kw.strip() for kw in search_keywords.split(",") if kw.strip()]
            if not keywords:
                raise ValueError("请输入有效的搜索关键词")
            
            if st:
                st.session_state.crawler_logs.append(f"开始搜索关键词: {', '.join(keywords)}")
            else:
                print(f"开始搜索关键词: {', '.join(keywords)}")
            
            # 模拟进度更新
            total_keywords = len(keywords)
            for i, keyword in enumerate(keywords):
                if st and not st.session_state.crawler_running:
                    break
                     
                if st:
                    st.session_state.crawler_logs.append(f"正在爬取关键词: {keyword}")
                else:
                    print(f"正在爬取关键词: {keyword}")
                
                try:
                    # 尝试调用爬虫的搜索方法
                    if hasattr(crawler, 'search'):
                        await crawler.search(keyword)
                    elif hasattr(crawler, 'crawl'):
                        await crawler.crawl(keyword)
                    else:
                        if st:
                            st.session_state.crawler_logs.append(f"警告: 爬虫对象没有搜索方法")
                except Exception as e:
                    if st:
                        st.session_state.crawler_logs.append(f"爬取关键词 '{keyword}' 时出错: {str(e)}")
                    else:
                        print(f"爬取关键词 '{keyword}' 时出错: {str(e)}")
                
                # 模拟进度
                if st:
                    st.session_state.crawler_progress = int(((i + 1) / total_keywords) * 90)
                await asyncio.sleep(gap_time)  # 使用配置的间隔时间
        
        elif crawler_type == "detail":
            # 帖子详情模式
            urls = []
            if 'detail_urls' in locals():
                # 处理逗号和换行分隔的URL
                urls = [url.strip() for url in detail_urls.replace('\n', ',').split(',') if url.strip()]
            
            if not urls:
                raise ValueError("请输入有效的帖子详情URL")
            
            if st:
                st.session_state.crawler_logs.append(f"开始爬取帖子详情: {', '.join(urls[:3])}{'...' if len(urls) > 3 else ''}")
            else:
                print(f"开始爬取帖子详情: {', '.join(urls[:3])}{'...' if len(urls) > 3 else ''}")
            
            # 模拟进度更新
            total_urls = len(urls)
            for i, url in enumerate(urls):
                if st and not st.session_state.crawler_running:
                    break
                     
                if st:
                    st.session_state.crawler_logs.append(f"正在爬取帖子: {url}")
                else:
                    print(f"正在爬取帖子: {url}")
                
                try:
                    # 尝试调用爬虫的详情方法
                    if hasattr(crawler, 'get_detail'):
                        await crawler.get_detail(url)
                    elif hasattr(crawler, 'crawl_detail'):
                        await crawler.crawl_detail(url)
                    else:
                        if st:
                            st.session_state.crawler_logs.append(f"警告: 爬虫对象没有详情方法")
                except Exception as e:
                    if st:
                        st.session_state.crawler_logs.append(f"爬取帖子 '{url}' 时出错: {str(e)}")
                    else:
                        print(f"爬取帖子 '{url}' 时出错: {str(e)}")
                
                # 模拟进度
                if st:
                    st.session_state.crawler_progress = int(((i + 1) / total_urls) * 90)
                await asyncio.sleep(gap_time)  # 使用配置的间隔时间
        
        elif crawler_type == "creator":
            # 创作者主页模式
            urls = []
            if 'creator_urls' in locals():
                # 处理逗号和换行分隔的URL
                urls = [url.strip() for url in creator_urls.replace('\n', ',').split(',') if url.strip()]
            
            if not urls:
                raise ValueError("请输入有效的创作者主页URL")
            
            if st:
                st.session_state.crawler_logs.append(f"开始爬取创作者主页: {', '.join(urls[:3])}{'...' if len(urls) > 3 else ''}")
            else:
                print(f"开始爬取创作者主页: {', '.join(urls[:3])}{'...' if len(urls) > 3 else ''}")
            
            # 模拟进度更新
            total_urls = len(urls)
            for i, url in enumerate(urls):
                if st and not st.session_state.crawler_running:
                    break
                     
                if st:
                    st.session_state.crawler_logs.append(f"正在爬取创作者: {url}")
                else:
                    print(f"正在爬取创作者: {url}")
                
                try:
                    # 尝试调用爬虫的创作者方法
                    if hasattr(crawler, 'crawl_user'):
                        await crawler.crawl_user(url)
                    elif hasattr(crawler, 'crawl_creator'):
                        await crawler.crawl_creator(url)
                    else:
                        if st:
                            st.session_state.crawler_logs.append(f"警告: 爬虫对象没有创作者方法")
                except Exception as e:
                    if st:
                        st.session_state.crawler_logs.append(f"爬取创作者 '{url}' 时出错: {str(e)}")
                    else:
                        print(f"爬取创作者 '{url}' 时出错: {str(e)}")
                
                # 模拟进度
                if st:
                    st.session_state.crawler_progress = int(((i + 1) / total_urls) * 90)
                await asyncio.sleep(gap_time)  # 使用配置的间隔时间
        
        if st and st.session_state.crawler_running:
            st.session_state.crawler_progress = 100
            st.session_state.crawler_logs.append("爬虫任务执行完成！")
        else:
            print("爬虫任务执行完成！")
        
    except Exception as e:
        error_msg = f"错误: {str(e)}"
        traceback_msg = f"详细错误栈: {traceback.format_exc()}"
        if st:
            st.session_state.crawler_logs.append(error_msg)
            st.session_state.crawler_logs.append(traceback_msg)
        else:
            print(error_msg)
            print(traceback_msg)
    finally:
        if st:
            st.session_state.crawler_running = False
            st.session_state.crawler_task = None
            
            # 使用回调方式更新页面，避免直接调用st.rerun()
            # 这可以减少事件循环冲突
            async def update_ui():
                # 短暂延迟确保状态更新
                await asyncio.sleep(0.1)
                
            # 安排UI更新任务，但不等待完成
            if asyncio.get_event_loop().is_running():
                asyncio.create_task(update_ui())

# 启动爬虫按钮
if st and col1 is not None:
    with col1:
        if not st.session_state.crawler_running:
            start_button = st.button("启动爬虫", type="primary", use_container_width=True)
            if start_button:
                # 验证必要参数 - 参照base_config.py中的CRAWLER_TYPE验证
                valid_input = True
                error_msg = ""
                
                if crawler_type == "search" and (not 'search_keywords' in locals() or not search_keywords.strip()):
                    valid_input = False
                    error_msg = "请输入搜索关键词"
                elif crawler_type == "detail" and (not 'detail_urls' in locals() or not detail_urls.strip()):
                    valid_input = False
                    error_msg = "请输入帖子详情URL"
                elif crawler_type == "creator" and (not 'creator_urls' in locals() or not creator_urls.strip()):
                    valid_input = False
                    error_msg = "请输入创作者主页URL"
                elif login_type == "cookie" and (not 'cookies' in locals() or not cookies.strip()):
                    valid_input = False
                    error_msg = "Cookie登录模式下，请输入有效的Cookie"
                
                if not valid_input:
                    st.error(error_msg)
                else:
                    # 安全地管理事件循环和任务
                    if st.session_state.crawler_task and not st.session_state.crawler_task.done():
                        try:
                            st.session_state.crawler_task.cancel()
                            # 移除await，因为这里不是async函数
                            # 在非异步环境中，我们只取消任务而不等待它完成
                            # await st.session_state.crawler_task  # 这行代码导致了错误
                        except Exception:
                            pass
                    
                    # 使用当前事件循环或创建新的
                    try:
                        loop = asyncio.get_event_loop()
                        # 检查循环是否在运行
                        if loop.is_running():
                            # 在运行的事件循环中创建任务
                            st.session_state.crawler_task = loop.create_task(run_crawler())
                        else:
                            # 如果循环未运行，启动一个新的
                            st.session_state.event_loop = asyncio.new_event_loop()
                            asyncio.set_event_loop(st.session_state.event_loop)
                            st.session_state.crawler_task = st.session_state.event_loop.create_task(run_crawler())
                            
                            # 在单独的线程中运行事件循环
                            import threading
                            def run_loop():
                                try:
                                    st.session_state.event_loop.run_until_complete(st.session_state.crawler_task)
                                except Exception as e:
                                    print(f"事件循环错误: {e}")
                                finally:
                                    st.session_state.event_loop.close()
                                    st.session_state.event_loop = None
                                    
                            thread = threading.Thread(target=run_loop, daemon=True)
                            thread.start()
                    except Exception as e:
                        st.session_state.crawler_logs.append(f"启动爬虫时出错: {str(e)}")
                    
                    # 短暂延迟让异步任务开始执行
                    import time
                    time.sleep(0.1)
        else:
            st.button("爬虫运行中...", disabled=True, use_container_width=True)

# 停止爬虫按钮
if st and col2 is not None:
    with col2:
        stop_button = st.button("停止爬虫", disabled=not st.session_state.crawler_running, use_container_width=True)
        if stop_button:
            # 安全地取消任务
            if st.session_state.crawler_task and not st.session_state.crawler_task.done():
                try:
                    st.session_state.crawler_task.cancel()
                except Exception as e:
                    st.session_state.crawler_logs.append(f"取消爬虫任务时出错: {str(e)}")
            
            # 清理事件循环
            if st.session_state.event_loop:
                try:
                    st.session_state.event_loop.stop()
                except Exception as e:
                    pass
                st.session_state.event_loop = None
            
            st.session_state.crawler_running = False
            st.session_state.crawler_task = None
            st.session_state.crawler_logs.append("爬虫已停止")
            
            # 使用time.sleep替代st.rerun()，减少事件循环冲突
            import time
            time.sleep(0.1)

# 显示日志
if st and log_container is not None:
    with log_container:
        for log in st.session_state.crawler_logs:
            # 根据日志内容添加不同的样式
            if log.startswith("错误:") or log.startswith("详细错误栈:"):
                st.error(log)
            elif log.startswith("开始") or log.startswith("爬虫任务执行完成"):
                st.success(log)
            else:
                st.info(log)

# 采集历史统计
if st:
    st.divider()
    st.subheader("采集历史统计")
    st.info("此功能待实现：将显示各平台和关键词的历史采集数据量统计")