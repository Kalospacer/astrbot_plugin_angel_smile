import asyncio
from pathlib import Path

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.message_components import Image, Plain
from astrbot.api.provider import ProviderRequest
from astrbot.api.star import Context, Star, StarTools, register

from .constants import STEAL_TOOL_NAME
from .models import PluginPaths
from .services.meme_manager import MemeManager
from .services.render import StickerRenderer
from .services.storage import MemeStorage
from .tools.steal_meme import StealMemeTool


@register(
    "astrbot_plugin_angel_smile",
    "Kalo & Muice",
    "天使之笑 2.0：多 Tag 智能表情包系统，支持自动异步偷图和组合匹配。",
    "2.0.0",
)
class AngelSmilePlugin(Star):
    def __init__(self, context: Context):
        super().__init__(context)
        self.context = context
        self.config = context.get_config()

        plugin_dir = Path(__file__).resolve().parent
        data_dir = StarTools.get_data_dir()

        paths = PluginPaths(
            plugin_dir=plugin_dir,
            data_dir=data_dir,
            stickers_dir=data_dir / "memes",
            stickers_data_file=data_dir / "memes_data.json",
            default_dir=plugin_dir / "default",
        )

        self.storage = MemeStorage(paths)
        self.manager = MemeManager(self.storage, context)
        self.renderer = StickerRenderer(self.storage)
        self.steal_meme_tool = StealMemeTool(manager=self.manager)

        StarTools.unregister_llm_tool(STEAL_TOOL_NAME)
        self.context.add_llm_tools(self.steal_meme_tool)

        # 记录正在处理的 URL，防止重复
        self._processing_urls = set()

        # 清理任务引用
        self._cleanup_task = None

    async def initialize(self):
        self.storage.initialize()
        self.manager.initialize()

        # 启动自动清理任务
        self._start_cleanup_task()

        logger.info("AngelSmile 2.0: 插件已通过 SQLite 数据库初始化")

    def _start_cleanup_task(self):
        """启动自动清理后台任务"""
        cleanup_interval = self.config.get("cleanup_interval_hours", 1)
        cleanup_count = self.config.get("cleanup_count", 5)

        if cleanup_interval <= 0 or cleanup_count <= 0:
            logger.info(
                "AngelSmile: 自动清理已禁用（cleanup_interval_hours 或 cleanup_count 为 0）"
            )
            return

        self._cleanup_task = asyncio.create_task(
            self._cleanup_loop(cleanup_interval, cleanup_count)
        )
        logger.info(
            f"AngelSmile: 自动清理任务已启动（间隔: {cleanup_interval}小时, 每次清理: {cleanup_count}个）"
        )

    async def _cleanup_loop(self, interval_hours: int, cleanup_count: int):
        """
        自动清理循环

        Args:
            interval_hours: 清理间隔（小时）
            cleanup_count: 每次清理数量
        """
        failure_count = 0
        retry_delay_seconds = 60
        max_retry_delay_seconds = 3600
        max_failures_before_warning = 5

        while True:
            try:
                await asyncio.sleep(interval_hours * 3600)
                await self._perform_cleanup(cleanup_count)
                failure_count = 0
                retry_delay_seconds = 60

            except asyncio.CancelledError:
                logger.info("AngelSmile: 清理任务已取消")
                break
            except Exception as exc:
                failure_count += 1
                log_message = (
                    f"AngelSmile: 清理任务执行失败（第 {failure_count} 次）: {exc}"
                )
                if failure_count >= max_failures_before_warning:
                    logger.error(log_message, exc_info=True)
                else:
                    logger.warning(log_message)

                await asyncio.sleep(retry_delay_seconds)
                retry_delay_seconds = min(
                    retry_delay_seconds * 2, max_retry_delay_seconds
                )

    async def _perform_cleanup(self, cleanup_count: int):
        """
        执行清理操作

        Args:
            cleanup_count: 要清理的表情包数量
        """
        try:
            # 获取使用统计
            stats = self.storage.get_usage_stats()
            total_count = stats.get("total_count", 0)

            if total_count == 0:
                logger.info("AngelSmile: 没有表情包需要清理")
                return

            # 获取调用次数最少的表情包
            least_used = self.storage.get_least_used_memes(cleanup_count)

            if not least_used:
                logger.info("AngelSmile: 没有找到可以清理的表情包")
                return

            deleted_count = 0
            for meme in least_used:
                meme_id = meme.get("meme_id")
                usage_count = meme.get("usage_count", 0)
                file_path = meme.get("file_path", "unknown")

                if self.storage.delete_meme(meme_id):
                    deleted_count += 1
                    logger.info(
                        f"AngelSmile: 已清理表情包 [{meme_id}] (使用次数: {usage_count}, 路径: {file_path})"
                    )
                else:
                    logger.warning(f"AngelSmile: 清理表情包失败 [{meme_id}]")

            # 重新加载分类数据（因为可能删除了某些分类的最后一个表情包）
            self.storage.load_stickers_data()

            logger.info(
                f"AngelSmile: 清理完成，删除了 {deleted_count}/{len(least_used)} 个表情包"
            )

        except Exception as exc:
            logger.error(f"AngelSmile: 执行清理时出错: {exc}", exc_info=True)

    @filter.event_message_type(filter.EventMessageType.ALL)
    async def on_message(self, event: AstrMessageEvent):
        """监听所有消息，自动异步偷图"""
        # 从 message_obj.message 获取消息链
        message_chain = getattr(event.message_obj, "message", None)
        if not message_chain:
            return

        for item in message_chain:
            if isinstance(item, Image):
                image_url = item.url or item.path
                if not image_url:
                    continue

                # 内存去重
                if image_url in self._processing_urls:
                    continue

                self._processing_urls.add(image_url)
                logger.info(
                    f"[AngelSmile] 发现图片，提交 LLM 审查: {image_url[:50]}..."
                )

                # 异步审查+偷图，不阻塞 pipeline
                asyncio.create_task(
                    self._llm_review_and_steal(
                        image_url=image_url,
                        source_group=str(event.get_group_id()),
                        source_user=str(event.get_sender_id()),
                    )
                )

    async def _llm_review_and_steal(
        self, image_url: str, source_group: str, source_user: str
    ):
        """LLM 审查图片并决定是否偷图"""
        try:
            # 检查当前表情包数量是否超过限制
            max_stickers = self.config.get("max_stickers", 100)
            current_count = self.storage.get_sticker_count()
            if current_count >= max_stickers:
                logger.info(
                    f"[AngelSmile] 当前表情包数量 ({current_count}) 已达到上限 ({max_stickers})，跳过偷图"
                )
                return

            # 调用 LLM 审查
            review_result = await self.manager.review_image(image_url)

            if not review_result["should_steal"]:
                logger.info(f"[AngelSmile] LLM 决定不偷: {review_result['reason']}")
                return

            # LLM 决定偷，顺便打标
            logger.info(f"[AngelSmile] LLM 决定偷图: {review_result['reason']}")

            result = await self.manager.save_with_tags(
                image_url=image_url,
                tags=review_result["tags"],
                source_group=source_group,
                source_user=source_user,
            )
            logger.info(f"[AngelSmile] 自动偷图成功: {result}")

        except Exception as e:
            logger.error(f"[AngelSmile] 自动偷图失败: {e}", exc_info=True)
        finally:
            self._processing_urls.discard(image_url)

    @filter.on_llm_request()
    async def on_llm_req(self, event: AstrMessageEvent, req: ProviderRequest):
        """注入 Tag 提示词"""
        prompt_catalog = self.renderer.build_prompt_catalog()
        if prompt_catalog:
            req.system_prompt = f"{req.system_prompt or ''}\n\n{prompt_catalog}"

    @filter.on_decorating_result()
    async def on_decorating_result(self, event: AstrMessageEvent):
        """组合匹配渲染"""
        result = event.get_result()
        if result is None or not getattr(result, "chain", None):
            return

        new_chain = []
        for item in result.chain:
            if isinstance(item, Plain):
                new_chain.extend(await self.renderer.render_text(item.text))
            else:
                new_chain.append(item)
        result.chain = new_chain

    async def terminate(self):
        """插件停止时清理资源"""
        StarTools.unregister_llm_tool(self.steal_meme_tool.name)

        if self._cleanup_task and not self._cleanup_task.done():
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass

        logger.info("AngelSmile 2.0: 插件已停止")
