import json
import random
import shutil
import sqlite3
import time
from pathlib import Path
from typing import Any

from astrbot.api import logger

from ..constants import SUPPORTED_IMAGE_SUFFIXES
from ..models import MemeSaveResult, PluginPaths
from ..utils import safe_filename


class MemeStorage:
    def __init__(self, paths: PluginPaths):
        self.paths = paths
        self.stickers_data: dict[str, str] = {}
        self.db_path: Path = paths.data_dir / "memes.db"
        self._conn: sqlite3.Connection | None = None

    def _to_relative_storage_path(self, file_path: str | Path) -> str:
        """将文件路径标准化为相对于 stickers_dir 的相对路径。"""
        path = Path(file_path)
        if not path.is_absolute():
            return str(path).replace("\\", "/")

        try:
            relative_path = path.resolve().relative_to(
                self.paths.stickers_dir.resolve()
            )
        except ValueError as exc:
            raise ValueError(f"文件路径不在表情包目录内: {file_path}") from exc
        return str(relative_path).replace("\\", "/")

    def _to_absolute_storage_path(self, file_path: str | Path) -> Path:
        """将数据库中的相对路径还原为绝对路径。"""
        path = Path(file_path)
        if path.is_absolute():
            return path
        return (self.paths.stickers_dir / path).resolve()

    def _get_connection(self) -> sqlite3.Connection:

        if self._conn is None:
            self._conn = sqlite3.connect(str(self.db_path))
            self._conn.row_factory = sqlite3.Row
        return self._conn

    def _row_to_meme(self, row: sqlite3.Row) -> dict[str, Any]:
        """将数据库行记录转换为表情包字典。"""
        return {
            "meme_id": row["meme_id"],
            "file_path": str(self._to_absolute_storage_path(row["file_path"])),
            "tags": json.loads(row["tags"]),
            "source": row["source"],
            "usage_count": row["usage_count"],
            "added_time": row["added_time"],
        }

    def _close_connection(self) -> None:
        """关闭数据库连接"""
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def _init_database(self) -> None:
        """初始化数据库表结构"""
        conn = self._get_connection()
        cursor = conn.cursor()

        # memes 表：存储表情包信息
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS memes (
                meme_id TEXT PRIMARY KEY,
                file_path TEXT NOT NULL UNIQUE,
                tags TEXT NOT NULL DEFAULT '[]',
                source TEXT,
                usage_count INTEGER DEFAULT 0,
                added_time REAL DEFAULT 0
            )
        """)

        # tag_index 表：tag 倒排索引
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tag_index (
                tag TEXT PRIMARY KEY,
                meme_ids TEXT NOT NULL DEFAULT '[]'
            )
        """)

        # 创建索引
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_memes_file_path ON memes(file_path)"
        )
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_tag_index_tag ON tag_index(tag)")

        conn.commit()

    def initialize(self) -> None:
        self.paths.data_dir.mkdir(parents=True, exist_ok=True)

        # 初始化数据库
        self._init_database()

        # 兼容旧版：如果存在 memes_data.json，迁移数据
        if self.paths.stickers_data_file.exists():
            self._migrate_from_json()

        # 初始化目录结构
        if not self.paths.stickers_dir.exists():
            self.paths.stickers_dir.mkdir(parents=True, exist_ok=True)
            source_memes = self.paths.default_dir / "memes"
            if source_memes.exists():
                for child in source_memes.iterdir():
                    if child.is_dir():
                        shutil.copytree(
                            child,
                            self.paths.stickers_dir / child.name,
                            dirs_exist_ok=True,
                        )

        # 启动时先清理空目录，再扫描现有文件并同步到数据库
        removed_empty_dirs = self._cleanup_empty_sticker_dirs()
        if removed_empty_dirs > 0:
            logger.info(f"AngelSmile: 启动时清理了 {removed_empty_dirs} 个空表情包目录")
        self._sync_files_to_database()

        self.load_stickers_data()

    def _migrate_from_json(self) -> None:
        """从旧版 JSON 文件迁移数据到数据库"""
        try:
            raw_data = json.loads(
                self.paths.stickers_data_file.read_text(encoding="utf-8")
            )
            if isinstance(raw_data, dict):
                conn = self._get_connection()
                cursor = conn.cursor()

                for category, description in raw_data.items():
                    if isinstance(category, str):
                        # 将分类作为 tag 添加到该分类下的所有表情包
                        category_dir = self.paths.stickers_dir / category
                        if category_dir.exists():
                            for file_path in category_dir.rglob("*"):
                                if (
                                    file_path.is_file()
                                    and file_path.suffix.lower()
                                    in SUPPORTED_IMAGE_SUFFIXES
                                ):
                                    meme_id = f"{category}_{file_path.stem}_{int(time.time() * 1000)}"
                                    self._save_meme_internal(
                                        cursor,
                                        meme_id,
                                        str(
                                            file_path.relative_to(
                                                self.paths.stickers_dir
                                            )
                                        ),
                                        [category],
                                        "migrated",
                                        0,
                                        time.time(),
                                    )

                conn.commit()
                logger.info("AngelSmile: 已从 JSON 迁移数据到 SQLite 数据库")

                # 备份旧文件
                backup_path = self.paths.stickers_data_file.with_suffix(".json.bak")
                self.paths.stickers_data_file.rename(backup_path)
        except Exception as exc:
            logger.error(f"AngelSmile: 迁移 JSON 数据失败: {exc}")

    def _sync_files_to_database(self) -> None:
        """扫描文件系统并同步到数据库"""
        if not self.paths.stickers_dir.exists():
            return

        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT file_path FROM memes")
        existing_paths: set[str] = {row[0] for row in cursor.fetchall()}

        current_paths: set[str] = set()
        for file_path in self.paths.stickers_dir.rglob("*"):
            if (
                file_path.is_file()
                and file_path.suffix.lower() in SUPPORTED_IMAGE_SUFFIXES
            ):
                current_paths.add(self._to_relative_storage_path(file_path))

        for file_path in current_paths - existing_paths:
            rel_path = Path(file_path)
            category = rel_path.parts[0] if len(rel_path.parts) > 1 else "unsorted"
            meme_id = f"{category}_{rel_path.stem}_{int(time.time() * 1000)}"
            self._save_meme_internal(
                cursor, meme_id, file_path, [category], "synced", 0, time.time()
            )

        for file_path in existing_paths - current_paths:
            self._delete_meme_internal(cursor, file_path)

        conn.commit()

    def _save_meme_internal(
        self,
        cursor: sqlite3.Cursor,
        meme_id: str,
        file_path: str,
        tags: list[str],
        source: str | None,
        usage_count: int,
        added_time: float,
    ) -> None:
        """内部方法：保存表情包到数据库"""
        tags_json = json.dumps(tags, ensure_ascii=False)

        cursor.execute(
            """INSERT OR REPLACE INTO memes
               (meme_id, file_path, tags, source, usage_count, added_time)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (meme_id, file_path, tags_json, source or "", usage_count, added_time),
        )

        # 更新 tag_index
        for tag in tags:
            cursor.execute("SELECT meme_ids FROM tag_index WHERE tag = ?", (tag,))
            row = cursor.fetchone()
            if row:
                meme_ids = json.loads(row[0])
                if meme_id not in meme_ids:
                    meme_ids.append(meme_id)
                    cursor.execute(
                        "UPDATE tag_index SET meme_ids = ? WHERE tag = ?",
                        (json.dumps(meme_ids, ensure_ascii=False), tag),
                    )
            else:
                cursor.execute(
                    "INSERT INTO tag_index (tag, meme_ids) VALUES (?, ?)",
                    (tag, json.dumps([meme_id], ensure_ascii=False)),
                )

    def _delete_meme_internal(self, cursor: sqlite3.Cursor, file_path: str) -> None:
        """内部方法：从数据库删除表情包"""
        cursor.execute(
            "SELECT meme_id, tags FROM memes WHERE file_path = ?", (file_path,)
        )
        row = cursor.fetchone()
        if row:
            meme_id = row[0]
            tags = json.loads(row[1])

            # 从 memes 表删除
            cursor.execute("DELETE FROM memes WHERE file_path = ?", (file_path,))

            # 更新 tag_index
            for tag in tags:
                cursor.execute("SELECT meme_ids FROM tag_index WHERE tag = ?", (tag,))
                tag_row = cursor.fetchone()
                if tag_row:
                    meme_ids = json.loads(tag_row[0])
                    if meme_id in meme_ids:
                        meme_ids.remove(meme_id)
                        if meme_ids:
                            cursor.execute(
                                "UPDATE tag_index SET meme_ids = ? WHERE tag = ?",
                                (json.dumps(meme_ids, ensure_ascii=False), tag),
                            )
                        else:
                            cursor.execute(
                                "DELETE FROM tag_index WHERE tag = ?", (tag,)
                            )

    def _remove_empty_parent_dirs(self, file_path: Path) -> None:
        """删除 stickers_dir 下已空的父目录。"""
        current = file_path.parent
        stickers_root = self.paths.stickers_dir.resolve()

        while current != stickers_root and stickers_root in current.parents:
            try:
                next(current.iterdir())
            except StopIteration:
                current.rmdir()
                current = current.parent
                continue
            except OSError as exc:
                logger.warning(f"AngelSmile: 删除空目录失败: {current}, {exc}")
            break

    def _cleanup_empty_sticker_dirs(self) -> int:
        """启动时删除 stickers_dir 下的空目录。"""
        if not self.paths.stickers_dir.exists():
            return 0

        removed_count = 0
        for directory in sorted(
            (path for path in self.paths.stickers_dir.rglob("*") if path.is_dir()),
            key=lambda path: len(path.parts),
            reverse=True,
        ):
            try:
                next(directory.iterdir())
            except StopIteration:
                directory.rmdir()
                removed_count += 1
            except OSError as exc:
                logger.warning(f"AngelSmile: 删除空目录失败: {directory}, {exc}")
        return removed_count

    def _delete_file_from_storage(self, file_path: str | Path) -> None:
        full_path = self._to_absolute_storage_path(file_path)
        try:
            full_path.unlink(missing_ok=True)
            self._remove_empty_parent_dirs(full_path)
            logger.info(f"AngelSmile: 已删除表情包文件: {full_path}")
        except Exception as exc:
            logger.warning(f"AngelSmile: 删除表情包文件失败: {exc}")

    def load_stickers_data(self) -> dict[str, str]:
        """加载分类数据（从数据库的 tags 中提取）"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            # 从 tag_index 获取所有 tag 作为分类
            cursor.execute("SELECT tag FROM tag_index")
            tags = [row[0] for row in cursor.fetchall()]

            # 构建 stickers_data（兼容旧版）
            self.stickers_data = {}
            for tag in tags:
                # 使用 tag 作为分类名，描述默认为空
                self.stickers_data[tag] = f"{tag} 分类的表情包"

            logger.info(f"AngelSmile: 已加载 {len(self.stickers_data)} 个表情分类")
        except Exception as exc:
            logger.error(f"AngelSmile: 加载表情数据失败: {exc}", exc_info=True)
            self.stickers_data = {}
        return self.stickers_data

    def _normalize_stickers_data(self, raw_data: Any) -> dict[str, str]:
        """标准化分类数据（兼容旧版）"""
        if raw_data is None:
            return {}
        if not isinstance(raw_data, dict):
            raise TypeError("分类数据顶层必须是对象")

        normalized: dict[str, str] = {}
        for raw_key, raw_value in raw_data.items():
            if not isinstance(raw_key, str):
                raise TypeError("分类名必须是字符串")
            normalized[raw_key] = str(raw_value or "").strip()
        return normalized

    def persist(self) -> None:
        """持久化数据（数据库已实时保存，此方法保留兼容）"""
        pass

    def has_sticker_assets(self, category: str) -> bool:
        """检查分类是否有表情包资源"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT COUNT(*) FROM memes WHERE tags LIKE ?", (f'%"{category}"%',)
        )
        return cursor.fetchone()[0] > 0

    def get_available_stickers_data(self) -> dict[str, str]:
        """获取有资源的分类数据"""
        return {
            category: description
            for category, description in self.stickers_data.items()
            if self.has_sticker_assets(category)
        }

    def get_catalog_stickers_data(self) -> dict[str, str]:
        """获取所有分类数据"""
        return dict(self.stickers_data)

    def get_catalog_description(self, category: str) -> str | None:
        """获取分类描述"""
        description = self.stickers_data.get(category)
        if description is None:
            return None
        return str(description).strip()

    def get_random_sticker_path(self, category: str) -> str | None:
        """获取随机表情包路径（兼容旧版）"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT file_path FROM memes WHERE tags LIKE ?", (f'%"{category}"%',)
        )
        rows = cursor.fetchall()
        if not rows:
            return None

        file_path = random.choice(rows)[0]

        cursor.execute(
            "UPDATE memes SET usage_count = usage_count + 1 WHERE file_path = ?",
            (file_path,),
        )
        conn.commit()

        return str(self._to_absolute_storage_path(file_path))

    def iter_all_sticker_files(self) -> list[Path]:
        """获取所有表情包文件路径"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT file_path FROM memes")
        return [self._to_absolute_storage_path(row[0]) for row in cursor.fetchall()]

    def get_sticker_count(self) -> int:
        """获取当前表情包总数"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM memes")
            return cursor.fetchone()[0]
        except Exception as exc:
            logger.error(f"AngelSmile: 获取表情包数量失败: {exc}")
            return 0

    def save_meme(
        self,
        source_file: Path,
        category: str,
        description: str,
        reason: str,
        save_name: str | None = None,
        overwrite_description: bool = False,
    ) -> MemeSaveResult:
        """保存表情包（兼容旧版）"""
        target_dir = self.paths.stickers_dir / category
        target_dir.mkdir(parents=True, exist_ok=True)

        target_file = target_dir / safe_filename(save_name, source_file.suffix.lower())
        if target_file.exists():
            target_file = (
                target_dir
                / f"{target_file.stem}_{int(time.time())}{target_file.suffix}"
            )

        shutil.copy2(source_file, target_file)

        # 保存到数据库
        meme_id = f"{category}_{target_file.stem}_{int(time.time() * 1000)}"
        self.save_meme_with_tags(
            meme_id=meme_id,
            file_path=str(target_file),
            tags=[category],
            source="manual_save",
        )

        # 更新分类数据（兼容旧版）
        if category not in self.stickers_data:
            self.stickers_data[category] = description
        elif overwrite_description:
            self.stickers_data[category] = description

        return MemeSaveResult(
            category=category,
            description=self.get_catalog_description(category) or description,
            saved_file=target_file,
            reason=reason,
        )

    # ==================== 新增方法 ====================

    def save_meme_with_tags(
        self,
        meme_id: str,
        file_path: str,
        tags: list[str],
        source: str | None = None,
    ) -> bool:
        """
        保存表情包到数据库（带标签）

        Args:
            meme_id: 表情包唯一标识
            file_path: 文件路径
            tags: 标签列表
            source: 来源

        Returns:
            是否保存成功
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            relative_path = self._to_relative_storage_path(file_path)
            self._save_meme_internal(
                cursor, meme_id, relative_path, tags, source, 0, time.time()
            )
            conn.commit()
            return True
        except Exception as exc:
            logger.error(f"AngelSmile: 保存表情包失败: {exc}")
            return False

    def get_memes_by_tags(self, tags: list[str]) -> list[dict[str, Any]]:
        """
        根据标签获取表情包（多标签匹配，取交集）

        Args:
            tags: 标签列表

        Returns:
            匹配的表情包列表
        """
        normalized_tags = [str(tag).strip() for tag in tags if str(tag).strip()]
        if not normalized_tags:
            return []

        conn = self._get_connection()
        cursor = conn.cursor()

        matched_meme_ids: set[str] | None = None
        for tag in normalized_tags:
            cursor.execute("SELECT meme_ids FROM tag_index WHERE tag = ?", (tag,))
            row = cursor.fetchone()
            if row is None:
                return []

            meme_ids = set(json.loads(row["meme_ids"]))
            if matched_meme_ids is None:
                matched_meme_ids = meme_ids
            else:
                matched_meme_ids &= meme_ids

            if not matched_meme_ids:
                return []

        assert matched_meme_ids is not None

        results = []
        for meme_id in matched_meme_ids:
            cursor.execute("SELECT * FROM memes WHERE meme_id = ?", (meme_id,))
            row = cursor.fetchone()
            if row is None:
                continue

            results.append(self._row_to_meme(row))

        return results

    def get_all_tags(self) -> list[str]:
        """
        获取所有标签

        Returns:
            标签列表
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT tag FROM tag_index ORDER BY tag")
        return [row[0] for row in cursor.fetchall()]

    def get_tag_index(self) -> dict[str, list[str]]:
        """
        获取标签倒排索引

        Returns:
            {tag: [meme_id, ...]}
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT tag, meme_ids FROM tag_index")

        result = {}
        for row in cursor.fetchall():
            result[row["tag"]] = json.loads(row["meme_ids"])

        return result

    def get_meme_by_id(self, meme_id: str) -> dict[str, Any] | None:
        """
        根据 ID 获取表情包

        Args:
            meme_id: 表情包 ID

        Returns:
            表情包信息或 None
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM memes WHERE meme_id = ?", (meme_id,))
        row = cursor.fetchone()

        if row is None:
            return None

        return self._row_to_meme(row)

    def get_meme_by_file_path(self, file_path: str | Path) -> dict[str, Any] | None:
        """根据文件路径获取表情包。支持绝对路径或相对路径。"""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            relative_path = self._to_relative_storage_path(file_path)
        except ValueError:
            return None
        cursor.execute("SELECT * FROM memes WHERE file_path = ?", (relative_path,))
        row = cursor.fetchone()
        if row is None:
            return None

        return self._row_to_meme(row)

    def increment_usage_count(self, meme_id: str) -> bool:
        """增加表情包使用次数。"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE memes SET usage_count = usage_count + 1 WHERE meme_id = ?",
                (meme_id,),
            )
            conn.commit()
            return cursor.rowcount > 0
        except Exception as exc:
            logger.error(f"AngelSmile: 更新表情包使用次数失败: {exc}")
            return False

    def update_meme_tags(self, meme_id: str, tags: list[str]) -> bool:
        """
        更新表情包标签

        Args:
            meme_id: 表情包 ID
            tags: 新标签列表

        Returns:
            是否更新成功
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            # 获取旧标签
            cursor.execute(
                "SELECT file_path, tags FROM memes WHERE meme_id = ?", (meme_id,)
            )
            row = cursor.fetchone()
            if not row:
                return False

            # file_path = row["file_path"]
            old_tags = json.loads(row["tags"])

            # 删除旧标签索引
            for tag in old_tags:
                cursor.execute("SELECT meme_ids FROM tag_index WHERE tag = ?", (tag,))
                tag_row = cursor.fetchone()
                if tag_row:
                    meme_ids = json.loads(tag_row[0])
                    if meme_id in meme_ids:
                        meme_ids.remove(meme_id)
                        if meme_ids:
                            cursor.execute(
                                "UPDATE tag_index SET meme_ids = ? WHERE tag = ?",
                                (json.dumps(meme_ids, ensure_ascii=False), tag),
                            )
                        else:
                            cursor.execute(
                                "DELETE FROM tag_index WHERE tag = ?", (tag,)
                            )

            # 更新标签
            tags_json = json.dumps(tags, ensure_ascii=False)
            cursor.execute(
                "UPDATE memes SET tags = ? WHERE meme_id = ?", (tags_json, meme_id)
            )

            # 添加新标签索引
            for tag in tags:
                cursor.execute("SELECT meme_ids FROM tag_index WHERE tag = ?", (tag,))
                tag_row = cursor.fetchone()
                if tag_row:
                    meme_ids = json.loads(tag_row[0])
                    if meme_id not in meme_ids:
                        meme_ids.append(meme_id)
                        cursor.execute(
                            "UPDATE tag_index SET meme_ids = ? WHERE tag = ?",
                            (json.dumps(meme_ids, ensure_ascii=False), tag),
                        )
                else:
                    cursor.execute(
                        "INSERT INTO tag_index (tag, meme_ids) VALUES (?, ?)",
                        (tag, json.dumps([meme_id], ensure_ascii=False)),
                    )

            conn.commit()
            return True
        except Exception as exc:
            logger.error(f"AngelSmile: 更新表情包标签失败: {exc}")
            return False

    def delete_meme(self, meme_id: str) -> bool:
        """
        删除表情包（包括数据库记录和文件）

        Args:
            meme_id: 表情包 ID

        Returns:
            是否删除成功
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            cursor.execute(
                "SELECT file_path, tags FROM memes WHERE meme_id = ?", (meme_id,)
            )
            row = cursor.fetchone()
            if not row:
                return False

            file_path = row["file_path"]
            tags = json.loads(row["tags"])

            cursor.execute("DELETE FROM memes WHERE meme_id = ?", (meme_id,))

            for tag in tags:
                cursor.execute("SELECT meme_ids FROM tag_index WHERE tag = ?", (tag,))
                tag_row = cursor.fetchone()
                if tag_row:
                    meme_ids = json.loads(tag_row[0])
                    if meme_id in meme_ids:
                        meme_ids.remove(meme_id)
                        if meme_ids:
                            cursor.execute(
                                "UPDATE tag_index SET meme_ids = ? WHERE tag = ?",
                                (json.dumps(meme_ids, ensure_ascii=False), tag),
                            )
                        else:
                            cursor.execute(
                                "DELETE FROM tag_index WHERE tag = ?", (tag,)
                            )

            conn.commit()

            self._delete_file_from_storage(file_path)

            return True
        except Exception as exc:
            logger.error(f"AngelSmile: 删除表情包失败: {exc}")
            return False

    def delete_all_memes_and_get_paths(self) -> list[str]:
        """批量删除全部表情包并返回已删除记录的绝对路径。"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT file_path FROM memes ORDER BY added_time ASC")
        file_paths = [self._to_absolute_storage_path(row["file_path"]) for row in cursor]

        cursor.execute("DELETE FROM memes")
        cursor.execute("DELETE FROM tag_index")
        conn.commit()

        for file_path in file_paths:
            self._delete_file_from_storage(file_path)

        self._cleanup_empty_sticker_dirs()
        self.load_stickers_data()
        return [str(path) for path in file_paths]

    def get_all_memes(self) -> list[dict[str, Any]]:
        """获取全部表情包记录。"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM memes ORDER BY added_time ASC")

        return [self._row_to_meme(row) for row in cursor.fetchall()]

    def get_least_used_memes(self, count: int) -> list[dict[str, Any]]:
        """
        获取调用次数最少的表情包

        Args:
            count: 获取数量

        Returns:
            表情包列表，按 usage_count 升序排列
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM memes ORDER BY usage_count ASC, added_time ASC LIMIT ?",
                (count,),
            )

            return [self._row_to_meme(row) for row in cursor.fetchall()]
        except Exception as exc:
            logger.error(f"AngelSmile: 获取最少使用表情包失败: {exc}")
            return []

    def get_usage_stats(self) -> dict[str, Any]:
        """
        获取使用统计

        Returns:
            统计信息字典
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            # 总数量
            cursor.execute("SELECT COUNT(*) FROM memes")
            total_count = cursor.fetchone()[0]

            # 总使用次数
            cursor.execute("SELECT SUM(usage_count) FROM memes")
            total_usage = cursor.fetchone()[0] or 0

            # 平均使用次数
            avg_usage = total_usage / total_count if total_count > 0 else 0

            # 最少使用的表情包
            cursor.execute(
                "SELECT meme_id, usage_count FROM memes ORDER BY usage_count ASC LIMIT 1"
            )
            row = cursor.fetchone()
            least_used = {"meme_id": row[0], "usage_count": row[1]} if row else None

            # 最多使用的表情包
            cursor.execute(
                "SELECT meme_id, usage_count FROM memes ORDER BY usage_count DESC LIMIT 1"
            )
            row = cursor.fetchone()
            most_used = {"meme_id": row[0], "usage_count": row[1]} if row else None

            return {
                "total_count": total_count,
                "total_usage": total_usage,
                "avg_usage": avg_usage,
                "least_used": least_used,
                "most_used": most_used,
            }
        except Exception as exc:
            logger.error(f"AngelSmile: 获取使用统计失败: {exc}")
            return {
                "total_count": 0,
                "total_usage": 0,
                "avg_usage": 0,
                "least_used": None,
                "most_used": None,
            }

    def __del__(self):
        """析构时关闭数据库连接"""
        self._close_connection()
