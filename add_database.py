import sqlite3
import time

def add_crawled_at_column(db_path: str):
    """
    为SQLite数据库中所有用户表新增crawled_at字段（int时间戳，默认当前时间）
    
    参数:
        db_path: SQLite数据库文件路径（如"./data.db"）
    """
    # 获取当前时间戳（整数类型，例如1762095759）
    current_timestamp = int(time.time())
    
    conn = None
    try:
        # 连接数据库
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # 1. 获取所有非系统表（排除sqlite_开头的系统表）
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name NOT LIKE 'sqlite_%'
        """)
        tables = [row[0] for row in cursor.fetchall()]
        
        if not tables:
            print("数据库中没有用户表，无需操作")
            return
        
        # 2. 遍历每个表，检查并添加crawled_at字段
        for table in tables:
            # 检查表中是否已存在crawled_at字段
            cursor.execute(f"PRAGMA table_info({table})")
            columns = [row[1] for row in cursor.fetchall()]  # 获取所有列名
            
            if "crawled_at" in columns:
                print(f"表 {table} 已存在crawled_at字段，跳过")
                continue
            
            # 新增crawled_at字段：INTEGER类型，默认值为当前时间戳
            try:
                cursor.execute(f"""
                    ALTER TABLE {table} 
                    ADD COLUMN crawled_at INTEGER DEFAULT {current_timestamp}
                """)
                print(f"表 {table} 已成功添加crawled_at字段")
            except sqlite3.Error as e:
                print(f"表 {table} 添加字段失败：{str(e)}")
        
        # 提交事务
        conn.commit()
        print("所有表处理完成")
        
    except sqlite3.Error as e:
        print(f"数据库操作失败：{str(e)}")
        if conn:
            conn.rollback()  # 出错时回滚
    finally:
        # 确保连接关闭
        if conn:
            conn.close()


# 示例使用：替换为你的数据库路径
if __name__ == "__main__":
    db_file_path = "./sqlite_tables.db"  # 替换为实际的数据库文件路径
    add_crawled_at_column(db_file_path)