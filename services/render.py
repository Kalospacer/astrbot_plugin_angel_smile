import random
import re

from astrbot.api import logger
from astrbot.api.message_components import Image, Plain


class StickerRenderer:
    def __init__(self, storage):
        self.storage = storage

    def build_sticker_list(self) -> str:
        """构建表情列表（兼容旧版）"""
        all_tags = self.storage.get_all_tags()
        if not all_tags:
            return ""
        return "\n".join(f"- :{tag}:" for tag in all_tags[:20])

    def build_prompt_catalog(self) -> str:
        """构建提示词目录（新版：注入所有可用 tag）"""
        all_tags = self.storage.get_all_tags()

        if not all_tags:
            return ""

        # 按使用频率排序（有索引的表情包数量）
        tag_index = self.storage.get_tag_index()
        tag_counts = []
        for tag in all_tags:
            meme_ids = tag_index.get(tag, [])
            tag_counts.append((tag, len(meme_ids)))

        # 按数量降序，取前 30 个
        tag_counts.sort(key=lambda x: x[1], reverse=True)
        top_tags = [tag for tag, count in tag_counts[:30]]

        tag_list = ", ".join(f":{tag}:" for tag in top_tags)

        return f"""
<表情包标签库>
可用标签：{tag_list}
使用方式：组合多个标签，如 :amused:cat: 表示又开心又是猫的表情
提示：标签越多，匹配越精准；没有匹配则不发送表情包
</表情包标签库>
"""

    def _parse_tags(self, text: str) -> list[str]:
        """解析 :tag1:tag2: 格式，提取所有 tag"""
        pattern = re.compile(r":([a-zA-Z0-9_\-\u4e00-\u9fff]+)")
        return [match.group(1) for match in pattern.finditer(text)]

    async def render_text(self, text: str) -> list:
        """渲染文本，支持 :tag1:tag2: 组合匹配"""
        components = []
        try:
            # 匹配 :tag1:tag2: 格式（连续多个标签）
            # 例如："你好:amused:cat:哈哈" -> 匹配 ":amused:cat:"
            pattern = re.compile(r"(?::[a-zA-Z0-9_\-\u4e00-\u9fff]+)+:")
            last_end = 0

            for match in pattern.finditer(text):
                matched_tag = match.group(0)
                logger.debug("[AngelSmile] render_text 匹配到标签串")

                if match.start() > last_end:
                    components.append(Plain(text[last_end : match.start()]))

                tags = self._parse_tags(matched_tag)

                if tags:
                    matched_memes = self.storage.get_memes_by_tags(tags)
                    logger.debug(
                        f"[AngelSmile] render_text 查询结果: {len(matched_memes)} 个表情包"
                    )

                    if matched_memes:
                        meme_data = random.choice(matched_memes)
                        if meme_data and meme_data.get("file_path"):
                            file_path = meme_data["file_path"]
                            meme_id = meme_data.get("meme_id")
                            logger.debug("[AngelSmile] render_text 命中表情包并替换")
                            components.append(Image.fromFileSystem(file_path))
                            if meme_id:
                                self.storage.increment_usage_count(meme_id)
                        else:
                            components.append(Plain(matched_tag))
                    else:
                        components.append(Plain(matched_tag))
                else:
                    components.append(Plain(matched_tag))

                last_end = match.end()

            if last_end < len(text):
                components.append(Plain(text[last_end:]))

        except Exception as exc:
            logger.error(f"AngelSmile: 处理表情标签时出错: {exc}", exc_info=True)
            return [Plain(text)]

        return components
