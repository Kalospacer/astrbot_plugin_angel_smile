# 天使之笑 2.0 测试文档

## 改造完成清单

### ✅ 已完成
1. **storage.py** - SQLite 数据库存储，支持多 tag
2. **meme_manager.py** - 自动偷图 + LLM 打标
3. **render.py** - 组合匹配 :tag1:tag2:
4. **main.py** - 异步消息监听
5. **metadata.yaml** - 版本升级到 2.0.0

### 📝 功能说明

#### 自动偷图
- 收到 subtype=1 的表情包自动偷走
- 完全异步，不阻塞 pipeline
- LLM 自动打 2-4 个 tag
- SQLite 存储，支持事务

#### 组合匹配
- 雪雪用 `:amused:cat:` 组合
- 后端查找包含这些 tag 的表情包
- 随机选择一个发送
- 没匹配则不发送

#### 提示词注入
- 系统提示词自动注入可用 tag 列表
- 雪雪知道有哪些 tag 可用
- 按使用频率排序，常用 tag 在前

## 测试步骤

1. 重启 AstrBot 加载插件
2. 发送表情包（subtype=1）
3. 检查日志："自动偷图成功"
4. 雪雪尝试用 `:tag1:tag2:` 发送表情包

## 数据库结构

```sql
-- memes 表
CREATE TABLE memes (
    meme_id TEXT PRIMARY KEY,
    file_path TEXT NOT NULL UNIQUE,
    tags TEXT NOT NULL DEFAULT '[]',
    source TEXT,
    usage_count INTEGER DEFAULT 0,
    added_time REAL DEFAULT 0
);

-- tag_index 表
CREATE TABLE tag_index (
    tag TEXT PRIMARY KEY,
    meme_ids TEXT NOT NULL
);
```

## 注意事项

- 首次启动会自动创建数据库
- 旧版 JSON 数据会自动迁移
- 临时文件会自动清理
- LLM 打标失败有默认 tag 保底
