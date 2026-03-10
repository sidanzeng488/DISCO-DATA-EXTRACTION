# DISCO ETL Pipeline

从 DISCODATA 提取数据，转换格式，部署到 Supabase 数据库。

## 数据流

```
DISCO API / Website
        │
        ▼
   [extract.py]  ──────►  DATA/current/*.csv (原始数据)
        │
        ▼
  [transform.py] ──────►  DATA/current/transformed/*.csv (转换后)
        │
        ▼
   [deploy.py]   ──────►  Supabase Database
```

## 使用方法

### 1. 提取数据

```bash
python etl/extract.py
```

**自动下载（通过 API）：**
- plants, agglomerations, discharge_points
- plant_agglo_links, art17_investments, art17_contacts
- report_periods, gwb_groundwater

**手动下载（从网站导出 CSV）：**

| 文件 | 链接 |
|-----|------|
| swb_surface_water.csv | [导出](https://discomap.eea.europa.eu/App/DiscodataViewer/?fqn=[WISE_WFD].[v2r1].[SWB_SurfaceWaterBody]) |
| swb_protected_areas.csv | [导出](https://discomap.eea.europa.eu/App/DiscodataViewer/?fqn=[WISE_WFD].[v2r1].[SWB_SurfaceWaterBody_SWAssociatedProtectedArea]) |
| gwb_protected_areas.csv | [导出](https://discomap.eea.europa.eu/App/DiscodataViewer/?fqn=[WISE_WFD].[v2r1].[GWB_GroundWaterBody_GWAssociatedProtectedArea]) |

下载后保存到 `DATA/current/` 文件夹。

### 2. 转换数据

```bash
python etl/transform.py
```

执行以下转换：
- 字段重命名（DISCO → Supabase Schema）
- 合并 SWB + GWB → `water_bodies.csv`
- 合并 Protected Areas → `water_body_protected_areas.csv`
- 提取国家列表 → `countries.csv`
- 创建 sensitivity 查找表

### 3. 部署到 Supabase

```bash
python etl/deploy.py
```

按外键依赖顺序导入：
1. countries
2. sensitivity
3. report_periods
4. water_bodies
5. agglomerations
6. plants
7. discharge_points
8. water_body_protected_areas

### 4. 预览数据（可选）

```bash
python etl/preview.py
```

## 文件结构

```
etl/
├── config.py        # 配置：表定义、字段映射
├── extract.py       # 从 DISCO 提取数据
├── transform.py     # 数据转换
├── deploy.py        # 部署到 Supabase
├── preview.py       # 数据预览
└── README.md        # 本文档

DATA/
├── current/                 # 当前数据
│   ├── plants.csv          # 原始数据
│   ├── ...
│   └── transformed/        # 转换后数据
│       ├── plants.csv
│       └── ...
├── backup/                  # 历史备份
│   └── 2026-03-09/
└── manifest.json           # 提取记录
```

## 字段映射示例

### Plants (T_UWWTPS)
| DISCO | Supabase |
|-------|----------|
| uwwCode | uwwtp_code |
| uwwName | plant_name |
| uwwLatitude | lat |
| uwwLongitude | longitude |
| uwwCapacity | plant_capacity |
| uwwPrimaryTreatment | provides_primary_treatment |
| uwwNRemoval | provides_nitrogen_removal |
| uwwPRemoval | provides_phosphorus_removal |

### Water Bodies (SWB + GWB)
| DISCO SWB | DISCO GWB | Supabase |
|-----------|-----------|----------|
| euSurfaceWaterBodyCode | euGroundWaterBodyCode | eu_water_body_code |
| surfaceWaterBodyName | - | water_body_name |
| countryCode | countryCode | country_code |
| - | - | water_type ('SWB'/'GWB') |

### Discharge Points
| DISCO | Supabase |
|-------|----------|
| dcpCode | dcp_code |
| dcpSurfaceWaters | is_surface_water |
| dcpWaterbodyID | water_body_code (when SWB) |
| dcpGroundWater | water_body_code (when GWB) |
| dcpTypeOfReceivingArea | sensitivity_code |

## 配置

### Supabase 连接

编辑 `.env` 文件：

```env
SUPABASE_HOST=db.xxxxx.supabase.co
SUPABASE_PORT=5432
SUPABASE_DATABASE=postgres
SUPABASE_USER=postgres
SUPABASE_PASSWORD=your_password
```

### 添加新表

1. 在 `config.py` 的 `DISCO_TABLES_AUTO` 或 `DISCO_TABLES_MANUAL` 中添加表定义
2. 在 `FIELD_MAPPINGS` 中添加字段映射
3. 如需要，在 `transform.py` 中添加转换逻辑
4. 在 `deploy.py` 中添加导入函数

## 备份策略

- 每次 extract 前自动备份当前数据到 `DATA/backup/`
- 保留最新版本 + 上一个版本
- 备份文件夹以日期命名（如 `2026-03-09`）

## 数据库完整性检查

运行检查脚本：

```bash
python etl/check_complete.py
```

### 最新状态 (2026-03-09)

| 表 | 数据库行数 | CSV行数 | 状态 |
|---|-----------|--------|------|
| country | 30 | - | ✅ OK |
| sensitivity | 7 | - | ✅ OK |
| report_code | 30 | 30 | ✅ OK |
| water_bodies | 159,955 | 159,982 | ✅ OK |
| water_body_protected_areas | 304,605 | 304,605 | ✅ OK |
| agglomeration | 25,208 | 26,898 | ✅ OK |
| plants | 26,064 | 26,224 | ✅ OK |
| discharge_points | 24,537 | 25,713 | ✅ OK |
| **总计** | **540,436** | | |

### 差异说明

部分表的数据库行数少于 CSV 行数，原因如下：

1. **重复主键**：CSV 中存在重复的主键值，使用 `ON CONFLICT DO NOTHING` 跳过
2. **外键约束**：`discharge_points` 中部分记录的 `water_body_code` 在 `water_bodies` 表中不存在，被跳过

### 外键分析 (discharge_points)

| 类型 | 数量 |
|-----|------|
| 有 water_body_code | 19,525 |
| 无 water_body_code (NULL) | 5,012 |

## 快速导入

使用批量导入脚本（比逐行插入快 100 倍）：

```bash
python etl/fast_import.py
```

此脚本使用 `execute_values` 批量插入，自动跳过已存在的记录和无效的外键。
