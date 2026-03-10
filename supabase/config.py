"""
Supabase 连接配置
从 .env 文件读取配置信息
"""
import os
from dotenv import load_dotenv

# 加载 .env 文件
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env'))

# ============================================
# Supabase 连接信息 (从 .env 读取)
# ============================================

# 支持大写和小写的环境变量名
SUPABASE_HOST = os.getenv('SUPABASE_HOST') or os.getenv('host', 'db.YOUR_PROJECT_ID.supabase.co')
SUPABASE_PORT = int(os.getenv('SUPABASE_PORT') or os.getenv('port', 5432))
SUPABASE_DATABASE = os.getenv('SUPABASE_DATABASE') or os.getenv('database', 'postgres')
SUPABASE_USER = os.getenv('SUPABASE_USER') or os.getenv('user', 'postgres')
SUPABASE_PASSWORD = os.getenv('SUPABASE_PASSWORD') or os.getenv('password', '')

# Supabase API (可选)
SUPABASE_URL = os.getenv('SUPABASE_URL', '')
SUPABASE_KEY = os.getenv('SUPABASE_KEY', '')
