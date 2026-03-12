import json
import os
import re
import shutil
import tempfile
import time
from asyncio import Lock
from pathlib import Path
from urllib.parse import urlparse

import aiohttp

from astrbot.api import logger

from ..constants import SUPPORTED_IMAGE_SUFFIXES
from ..models import MemeToolResult
from ..utils import (
    get_allowed_image_roots,
    is_path_within_roots,
    normalize_category_name,
    resolve_user_path,
)
from .dedup import DHashDedupService

# 默认的图片审查提示词
DEFAULT_REVIEW_SYSTEM_PROMPT = """请审查这张图片是否是表情包/梗图/二次元表情。

审查标准：
1. 这是否是表情包、梗图、二次元表情或可爱的萌图？
2. 是否适合作为聊天表情包使用？
3. 不是隐私照片（如个人生活照、自拍）
4. 不是普通照片（如风景、物品实拍）
5. 不是截图（如聊天记录截图、屏幕截图）

请以 JSON 格式返回结果：
{
    "should_steal": true/false,  // 是否应该偷取这个表情包
    "reason": "说明理由",       // 简要说明判断理由
    "tags": ["标签1", "标签2"]   // 如果是表情包，给出2-4个标签（简短精准，2-4字，描述情绪/动作/场景）
}

只输出 JSON，不要输出其他内容。"""

FALLBACK_REVIEW_NEGATIVE_MARKERS = (
    "不适合",
    "不应该偷",
    "不建议偷",
    "不是表情包",
    "不是梗图",
    "不是二次元",
    'should_steal": false',
    "should_steal:false",
    '"should_steal": false',
)

FALLBACK_REVIEW_POSITIVE_MARKERS = (
    "是表情包",
    "是梗图",
    "是二次元",
    "适合作为聊天表情包",
    "适合做表情包",
    "适合作为表情包",
    "建议偷",
    "应该偷",
    'should_steal": true',
    "should_steal:true",
    '"should_steal": true',
)


class MemeManager:
    def __init__(self, storage, context=None, plugin_config=None):
        self.storage = storage
        self.context = context
        self.plugin_config = plugin_config or {}
        self.write_lock = Lock()
        self.dedup = DHashDedupService(storage=self.storage)
        self.allowed_image_roots = get_allowed_image_roots(
            extra_roots=(self.storage.paths.plugin_dir, self.storage.paths.data_dir)
        )

    def initialize(self) -> None:
        self.dedup.initialize()

    def set_context(self, context) -> None:
        """设置 AstrBot 上下文，用于调用 LLM"""
        self.context = context

    def set_plugin_config(self, plugin_config) -> None:
        """设置插件配置。"""
        self.plugin_config = plugin_config or {}

    def _get_llm_provider(self):
        """获取配置的 LLM provider"""
        if self.context is None:
            logger.warning("AngelSmile: 未设置 context，无法调用 LLM")
            return None

        try:
            provider_id = self.plugin_config.get("tag_provider_id", "")

            if provider_id:
                provider = self.context.get_provider_by_id(provider_id)
            else:
                provider = self.context.get_using_provider() or None

            return provider
        except Exception as exc:
            logger.error(f"AngelSmile: 获取 LLM provider 失败: {exc}")
            return None

    def _get_review_prompt(self) -> str:
        """获取配置的审查提示词，如果没有配置则使用默认值"""
        if self.context is None:
            return DEFAULT_REVIEW_SYSTEM_PROMPT

        try:
            review_prompt = self.plugin_config.get("review_system_prompt", "")
            if (
                review_prompt
                and isinstance(review_prompt, str)
                and review_prompt.strip()
            ):
                return review_prompt.strip()
        except Exception as exc:
            logger.warning(f"AngelSmile: 获取 review_system_prompt 配置失败: {exc}")

        return DEFAULT_REVIEW_SYSTEM_PROMPT

    @staticmethod
    def _fallback_should_steal(result_text: str) -> bool:
        text_lower = result_text.lower()
        if any(marker in text_lower for marker in FALLBACK_REVIEW_NEGATIVE_MARKERS):
            return False
        return any(marker in text_lower for marker in FALLBACK_REVIEW_POSITIVE_MARKERS)

    @staticmethod
    def _parse_should_steal(raw_value) -> bool:
        if isinstance(raw_value, bool):
            return raw_value
        if isinstance(raw_value, str):
            normalized = raw_value.strip().lower()
            if normalized in {"true", "1", "yes"}:
                return True
            if normalized in {"false", "0", "no"}:
                return False
        return False

    async def review_image(self, image_url: str) -> dict:
        """
        使用 LLM 审查图片是否是表情包

        Args:
            image_url: 图片 URL

        Returns:
            {"should_steal": bool, "reason": str, "tags": list}
        """
        provider = self._get_llm_provider()
        if provider is None:
            return {
                "should_steal": False,
                "reason": "未找到可用的 LLM 提供商，无法审查图片",
                "tags": [],
            }

        try:
            prompt = self._get_review_prompt()
            response = await provider.text_chat(
                prompt=prompt,
                image_urls=[image_url],
            )

            if response is None or not hasattr(response, "completion_text"):
                return {"should_steal": False, "reason": "LLM 返回结果为空", "tags": []}

            result_text = response.completion_text.strip()

            json_match = re.search(r"\{[^}]*\}", result_text, re.DOTALL)
            if json_match:
                try:
                    result = json.loads(json_match.group())
                    return {
                        "should_steal": self._parse_should_steal(
                            result.get("should_steal", False)
                        ),
                        "reason": str(result.get("reason", "未知")),
                        "tags": list(result.get("tags", []))
                        if isinstance(result.get("tags"), list)
                        else [],
                    }
                except json.JSONDecodeError:
                    pass

            should_steal = self._fallback_should_steal(result_text)

            tags = []
            if should_steal:
                tag_matches = re.findall(r'["\']([^"\']{2,10})["\']', result_text)
                tags = tag_matches[:4] if tag_matches else ["未分类"]

            return {
                "should_steal": should_steal,
                "reason": result_text[:200] if not should_steal else "判断为表情包",
                "tags": tags,
            }

        except Exception as exc:
            logger.error(f"AngelSmile: LLM 审查图片失败: {exc}", exc_info=True)
            return {
                "should_steal": False,
                "reason": f"审查过程出错: {str(exc)}",
                "tags": [],
            }

    async def save_with_tags(
        self,
        image_url: str,
        tags: list,
        source_group: str,
        source_user: str,
        max_stickers: int | None = None,
    ) -> dict:
        """
        保存图片并打上指定标签

        Args:
            image_url: 图片 URL
            tags: 标签列表
            source_group: 来源群组
            source_user: 来源用户

        Returns:
            {"success": bool, "meme_id": str, "message": str}
        """
        temp_file_path: Path | None = None

        try:
            temp_file_path = await self._download_image(image_url)
            if temp_file_path is None:
                return {"success": False, "meme_id": "", "message": "下载图片失败"}

            if not tags or not isinstance(tags, list):
                tags = ["未分类", "自动导入"]

            tags = [str(tag).strip() for tag in tags if tag and str(tag).strip()]
            tags = tags[:4]
            tags = [tag for tag in tags if len(tag) <= 10]
            if not tags:
                tags = ["未分类"]

            file_suffix = Path(urlparse(image_url).path).suffix.lower()
            if file_suffix not in SUPPORTED_IMAGE_SUFFIXES:
                file_suffix = ".jpg"

            primary_category = tags[0]
            normalized_category = normalize_category_name(primary_category)

            timestamp = int(time.time() * 1000)
            random_suffix = os.urandom(4).hex()
            file_name = f"auto_{timestamp}_{random_suffix}{file_suffix}"

            target_dir = self.storage.paths.stickers_dir / normalized_category
            target_dir.mkdir(parents=True, exist_ok=True)
            target_file = target_dir / file_name

            source_info = "auto_steal"
            if source_group:
                source_info += f"_group:{source_group}"
            if source_user:
                source_info += f"_user:{source_user}"

            async with self.write_lock:
                if max_stickers is not None:
                    current_count = self.storage.get_sticker_count()
                    if current_count >= max_stickers:
                        return {
                            "success": False,
                            "meme_id": "",
                            "message": f"当前表情包数量已达到上限 ({max_stickers})",
                        }

                duplicate = self.dedup.find_similar_duplicate(temp_file_path)
                if duplicate is not None:
                    logger.info(
                        f"AngelSmile: 图片已存在，跳过保存: {duplicate.matched_file}"
                    )
                    self._cleanup_temp_file(temp_file_path)
                    temp_file_path = None
                    return {
                        "success": False,
                        "meme_id": "",
                        "message": f"图片已存在: {duplicate.matched_file}",
                    }

                shutil.move(str(temp_file_path), str(target_file))
                temp_file_path = None

                meme_id = f"{normalized_category}_{timestamp}_{random_suffix}"
                success = self.storage.save_meme_with_tags(
                    meme_id=meme_id,
                    file_path=str(target_file),
                    tags=tags,
                    source=source_info,
                )
                if not success:
                    target_file.unlink(missing_ok=True)
                    return {
                        "success": False,
                        "meme_id": "",
                        "message": "保存表情包到数据库失败",
                    }

                self.dedup.register_file(target_file)

            logger.info(f"AngelSmile: 保存表情包成功，meme_id={meme_id}, tags={tags}")
            return {
                "success": True,
                "meme_id": meme_id,
                "message": f"保存成功，标签: {', '.join(tags)}",
            }

        except Exception as exc:
            logger.error(f"AngelSmile: 保存表情包失败: {exc}", exc_info=True)
            return {"success": False, "meme_id": "", "message": f"保存失败: {str(exc)}"}
        finally:
            if temp_file_path is not None:
                self._cleanup_temp_file(temp_file_path)

    async def steal_meme(
        self,
        image_path: str,
        category: str,
        description: str | None = None,
        save_name: str | None = None,
    ) -> str:
        raw_path = resolve_user_path(image_path)
        if not raw_path.exists() or not raw_path.is_file():
            return f"图片不存在或不是文件: {raw_path}"

        if not is_path_within_roots(raw_path, self.allowed_image_roots):
            return "图片路径不在允许的目录范围内。"

        suffix = raw_path.suffix.lower()
        if suffix not in SUPPORTED_IMAGE_SUFFIXES:
            return f"暂不支持的图片格式: {suffix or '无扩展名'}"

        if not category.strip():
            return (
                "缺少 category。请先根据分类目录选择一个分类，再调用图片入库工具保存。"
            )

        final_category = normalize_category_name(category)
        final_description = str(
            description
            or self.storage.get_catalog_description(final_category)
            or "手动指定分类导入的表情包"
        ).strip()
        reason = "手动指定分类"
        overwrite_description = bool(description)

        async with self.write_lock:
            duplicate = self.dedup.find_similar_duplicate(raw_path)
            if duplicate is not None:
                return MemeToolResult(
                    ok=True,
                    saved=False,
                    category=final_category,
                    description=final_description,
                    message="这个表情包已经偷过了",
                    reason="这个表情包已经偷过了",
                    duplicate=True,
                    duplicate_type="similar",
                    matched_file=str(duplicate.matched_file),
                    distance=duplicate.distance,
                ).to_message()

            try:
                result = self.storage.save_meme(
                    source_file=Path(raw_path),
                    category=final_category,
                    description=final_description,
                    reason=reason,
                    save_name=save_name,
                    overwrite_description=overwrite_description,
                )
            except Exception as exc:
                logger.error(f"AngelSmile: 手动偷图保存失败: {exc}", exc_info=True)
                return f"保存失败: {exc}"
            self.dedup.register_file(result.saved_file)

        return result.to_tool_result().to_message()

    async def _generate_tags_with_llm(self, image_url: str) -> list[str]:
        """
        使用 LLM 分析表情包并生成标签

        Args:
            image_url: 图片 URL

        Returns:
            标签列表
        """
        provider = self._get_llm_provider()
        if provider is None:
            return ["未分类", "自动导入"]

        try:
            prompt = f"""分析这个表情包，给出 2-4 个最准确的标签。
标签要求：简短精准（2-4字），描述情绪/动作/场景
常见标签：开心、无语、可爱、沙雕、摸鱼、猫猫、狗狗
表情包URL: {image_url}
只输出标签，用逗号分隔，如：开心,可爱,猫猫"""

            response = await provider.text_chat(
                prompt=prompt,
                image_urls=[image_url],
            )

            if response is None or not hasattr(response, "completion_text"):
                logger.warning("AngelSmile: LLM 返回结果为空")
                return ["未分类", "自动导入"]

            result_text = response.completion_text.strip()
            if not result_text:
                return ["未分类", "自动导入"]

            tags = [tag.strip() for tag in result_text.split(",") if tag.strip()]
            tags = tags[:4]
            tags = [tag for tag in tags if len(tag) <= 10]

            if len(tags) < 2:
                default_tags = ["未分类", "自动导入"]
                tags.extend(default_tags[: 2 - len(tags)])

            return tags

        except Exception as exc:
            logger.error(f"AngelSmile: LLM 生成标签失败: {exc}", exc_info=True)
            return ["未分类", "自动导入"]

    async def _download_image(self, image_url: str) -> Path | None:
        """
        异步下载图片到临时目录

        Args:
            image_url: 图片 URL

        Returns:
            临时文件路径，失败返回 None
        """
        try:
            parsed_url = urlparse(image_url)
            file_suffix = Path(parsed_url.path).suffix.lower()
            if file_suffix not in SUPPORTED_IMAGE_SUFFIXES:
                file_suffix = ".jpg"

            temp_dir = Path(tempfile.gettempdir()) / "angel_smile"
            temp_dir.mkdir(parents=True, exist_ok=True)

            temp_file = temp_dir / f"download_{os.urandom(8).hex()}{file_suffix}"

            timeout = aiohttp.ClientTimeout(total=30)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(image_url) as response:
                    if response.status != 200:
                        logger.warning(
                            f"AngelSmile: 下载图片失败，状态码: {response.status}"
                        )
                        return None

                    content = await response.read()
                    if not content or len(content) < 100:
                        logger.warning("AngelSmile: 下载的图片内容过小或为空")
                        return None

                    temp_file.write_bytes(content)

            logger.debug(f"AngelSmile: 图片下载成功: {temp_file}")
            return temp_file

        except aiohttp.ClientError as exc:
            logger.error(f"AngelSmile: 下载图片网络错误: {exc}")
            return None
        except Exception as exc:
            logger.error(f"AngelSmile: 下载图片失败: {exc}", exc_info=True)
            return None

    def _cleanup_temp_file(self, temp_file_path: Path | None) -> None:
        """清理临时文件"""
        if temp_file_path is None or not temp_file_path.exists():
            return
        try:
            temp_file_path.unlink(missing_ok=True)
            logger.debug(f"AngelSmile: 已清理临时文件: {temp_file_path}")
        except Exception as exc:
            logger.warning(f"AngelSmile: 清理临时文件失败: {exc}")
