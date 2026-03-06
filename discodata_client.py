"""
DISCODATA 通用查询客户端
支持查询欧洲环境署 (EEA) 所有公开数据集

功能：
- 查询任意已知的表
- 选择指定列下载数据
- 导出为 CSV/JSON

API 格式：表名需要完整路径 [Database].[Version].[TableName]
"""

import json
import csv
import urllib.parse
from typing import Dict, List, Optional, Any
import requests


class DiscoDataClient:
    """DISCODATA API 客户端"""

    BASE_URL = "https://discodata.eea.europa.eu/sql"

    def __init__(self, hits_per_page: int = 500):
        """
        初始化客户端

        Args:
            hits_per_page: 每页返回的记录数，默认 500
        """
        self.hits_per_page = hits_per_page
        self.session = requests.Session()

    def _build_url(self, query: str, page: int = 1) -> str:
        """构建 API URL"""
        encoded_query = urllib.parse.quote(query)
        return f"{self.BASE_URL}?query={encoded_query}&p={page}&nrOfHits={self.hits_per_page}"

    def execute_query(self, query: str, page: int = 1) -> Dict[str, Any]:
        """
        执行 SQL 查询

        Args:
            query: SQL 查询语句
            page: 页码

        Returns:
            API 响应 JSON
        """
        url = self._build_url(query, page)
        response = self.session.get(url, timeout=60)
        response.raise_for_status()
        data = response.json()

        # 检查错误
        if 'errors' in data and data['errors']:
            error_msg = data['errors'][0].get('error', 'Unknown error')
            raise Exception(f"API Error: {error_msg}")

        return data

    def fetch_all(self, query: str, max_records: Optional[int] = None) -> List[Dict]:
        """
        获取所有结果（自动分页）

        Args:
            query: SQL 查询语句
            max_records: 最大记录数限制，None 表示不限制

        Returns:
            所有结果记录列表
        """
        all_results = []
        page = 1

        while True:
            result = self.execute_query(query, page)
            records = result.get('results', [])

            if not records:
                break

            all_results.extend(records)
            print(f"  Page {page}: {len(records)} records (total: {len(all_results)})")

            if max_records and len(all_results) >= max_records:
                all_results = all_results[:max_records]
                break

            if len(records) < self.hits_per_page:
                break

            page += 1

        return all_results

    # ========== 数据查询 ==========

    def preview(self, table_full_name: str, rows: int = 10) -> List[Dict]:
        """
        预览表数据

        Args:
            table_full_name: 表的完整名称，如 [WISE_UWWTD].[v1r1].[T_UWWTPS]
            rows: 预览行数

        Returns:
            数据记录列表
        """
        query = f"SELECT TOP {rows} * FROM {table_full_name}"
        print(f"\nQuery: {query}")
        result = self.execute_query(query)
        return result.get('results', [])

    def get_columns(self, table_full_name: str) -> List[str]:
        """
        获取表的列名（通过查询一行数据获取）

        Args:
            table_full_name: 表的完整名称

        Returns:
            列名列表
        """
        data = self.preview(table_full_name, rows=1)
        if data:
            return list(data[0].keys())
        return []

    def select(self, table_full_name: str, 
               columns: Optional[List[str]] = None,
               where: Optional[str] = None,
               order_by: Optional[str] = None,
               limit: Optional[int] = None) -> List[Dict]:
        """
        查询数据

        Args:
            table_full_name: 表的完整名称
            columns: 列名列表，None 表示所有列
            where: WHERE 条件（不含 WHERE 关键字）
            order_by: ORDER BY 条件（不含 ORDER BY 关键字）
            limit: 返回记录数限制

        Returns:
            数据记录列表
        """
        # 构建列选择
        cols = ", ".join(columns) if columns else "*"

        # 构建查询
        query = f"SELECT"
        if limit:
            query += f" TOP {limit}"
        query += f" {cols} FROM {table_full_name}"

        if where:
            query += f" WHERE {where}"
        if order_by:
            query += f" ORDER BY {order_by}"

        print(f"\nQuery: {query[:150]}...")
        return self.fetch_all(query, max_records=limit)

    def count(self, table_full_name: str, where: Optional[str] = None) -> int:
        """统计记录数"""
        query = f"SELECT COUNT(*) as total FROM {table_full_name}"
        if where:
            query += f" WHERE {where}"
        result = self.execute_query(query)
        if result.get('results'):
            return result['results'][0].get('total', 0)
        return 0

    def query(self, sql: str, max_records: Optional[int] = None) -> List[Dict]:
        """
        执行自定义 SQL 查询

        Args:
            sql: 完整的 SQL 查询语句
            max_records: 最大记录数限制

        Returns:
            数据记录列表
        """
        print(f"\nQuery: {sql[:150]}...")
        return self.fetch_all(sql, max_records=max_records)

    # ========== 数据导出 ==========

    @staticmethod
    def to_csv(data: List[Dict], filename: str):
        """导出数据为 CSV 文件"""
        if not data:
            print("No data to export!")
            return

        fieldnames = list(data[0].keys())

        with open(filename, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)

        print(f"\n✓ Exported to {filename}")
        print(f"  Columns: {len(fieldnames)}")
        print(f"  Rows: {len(data)}")

    @staticmethod
    def to_json(data: List[Dict], filename: str):
        """导出数据为 JSON 文件"""
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        print(f"\n✓ Exported to {filename}")
        print(f"  Records: {len(data)}")


def create_client(hits_per_page: int = 500) -> DiscoDataClient:
    """创建客户端实例"""
    return DiscoDataClient(hits_per_page=hits_per_page)
