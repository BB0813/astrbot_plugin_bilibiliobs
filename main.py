import asyncio
import aiohttp
import json
import os
from typing import Dict, List, Optional
from astrbot.api.event import filter, AstrMessageEvent, MessageEventResult, MessageChain
from astrbot.api.star import Context, Star, register
from astrbot.api import logger, AstrBotConfig
from .bili_login import BilibiliLoginManager

@register("bili_live_notice", "Binbim", "B站UP主开播监测插件", "1.2.0", "https://github.com/BB0813/astrbot_plugin_bilibiliobs")
class BiliLiveNoticePlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig = None):
        super().__init__(context)
        self.config = config or {}
        self.check_interval = int(self.config.get("check_interval", 60)) if isinstance(self.config, dict) else 60
        self.max_monitors = int(self.config.get("max_monitors", 50)) if isinstance(self.config, dict) else 50
        self.enable_notifications = bool(self.config.get("enable_notifications", True)) if isinstance(self.config, dict) else True
        self.enable_end_notifications = bool(self.config.get("enable_end_notifications", True)) if isinstance(self.config, dict) else True
        # 按会话隔离的订阅数据: {unified_msg_origin: {uid: {uname, room_id, added_by, added_time, at_all}}}
        self.monitored_uids: Dict[str, Dict[str, Dict]] = {}
        # 按会话隔离的直播状态缓存: {unified_msg_origin: {uid: live_status}}
        # 修复：每个会话独立的缓存，避免同一UP主在多群订阅时只有第一个群能收到通知
        self.live_status_cache: Dict[str, Dict[str, int]] = {}
        self.uid_error_counts: Dict[str, int] = {}
        self.uid_skip_until: Dict[str, float] = {}
        self.current_interval = self.check_interval
        self._last_rate_limited = False
        self._init_lock = asyncio.Lock()
        self._initialized = False
        self.monitor_task = None
        self.session = None
        # 配置文件路径
        self.config_file = os.path.join(self._get_data_dir(), "monitor_config.json")
        # 登录管理器
        self.login_manager = BilibiliLoginManager(context, self._save_cookie_to_config)
        # 启动初始化任务
        asyncio.create_task(self.initialize())

    def _get_data_dir(self) -> str:
        """获取数据存储目录，优先使用 AstrBot 数据目录"""
        try:
            from astrbot.core.utils.astrbot_path import get_astrbot_data_path
            base = os.path.join(get_astrbot_data_path(), "plugins", "bili_live_notice")
        except Exception:
            base = os.path.join(os.path.expanduser("~"), ".astrbot", "bili_live_notice")
        os.makedirs(base, exist_ok=True)
        return base

    async def ensure_session(self):
        if not self.session or self.session.closed:
            self.session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=10),
                connector=aiohttp.TCPConnector(limit=10, limit_per_host=5)
            )
            logger.info("HTTP会话已创建")

    async def initialize(self):
        """插件初始化方法"""
        async with self._init_lock:
            if self._initialized:
                logger.info("插件已初始化，跳过")
                return
            try:
                logger.info("正在初始化B站开播监测插件...")

                # 初始化HTTP会话
                await self.ensure_session()

                # 加载配置文件
                await self.load_config()

                # 统计总监控数
                total = sum(len(uids) for uids in self.monitored_uids.values())
                logger.info(f"已加载 {total} 个监控配置")

                # 启动监控任务
                if not self.monitor_task or self.monitor_task.done():
                    self.monitor_task = asyncio.create_task(self.monitor_live_status())
                    logger.info("监控任务已启动")

                self._initialized = True
                logger.info("B站开播监测插件初始化完成")

            except Exception as e:
                logger.error(f"插件初始化失败: {e}")
                await self._cleanup_resources()
                raise

    async def load_config(self):
        """加载监控配置文件，支持旧格式自动迁移"""
        try:
            if os.path.exists(self.config_file):
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    raw = data.get('monitored_uids', {})
                    # 检测旧格式并自动迁移
                    if raw and _is_old_format(raw):
                        logger.info("检测到旧版订阅格式，正在自动迁移...")
                        self.monitored_uids = _migrate_old_format(raw)
                        logger.info(f"迁移完成，共 {sum(len(v) for v in self.monitored_uids.values())} 条订阅")
                    else:
                        self.monitored_uids = raw
                    # 修复：live_status_cache 现在是按会话隔离的 {origin: {uid: status}}
                    old_cache = data.get('live_status_cache', {})
                    if old_cache and isinstance(old_cache, dict):
                        # 检测是否是旧格式的全局缓存 {uid: status}
                        first_val = next(iter(old_cache.values()), None)
                        if first_val is not None and not isinstance(first_val, dict):
                            # 旧格式迁移：把全局缓存的值继承到每个会话的缓存中
                            # 这样正在直播的UP主不会被误判为"刚开播"，避免误发通知
                            self.live_status_cache = {}
                            for origin in self.monitored_uids:
                                self.live_status_cache[origin] = dict(old_cache)
                            logger.info("旧版全局缓存已迁移为按会话隔离，并继承旧状态避免误报开播")
                            # 迁移后立即落盘新格式
                            await self.save_config()
                        else:
                            self.live_status_cache = old_cache
                    else:
                        self.live_status_cache = {}
                    self.enable_notifications = data.get('enable_notifications', self.enable_notifications)
                    self.enable_end_notifications = data.get('enable_end_notifications', self.enable_end_notifications)
                    total = sum(len(uids) for uids in self.monitored_uids.values())
                    logger.info(f"已加载 {total} 个监控配置")
            else:
                # 兼容旧路径迁移
                legacy_file = os.path.join(os.path.dirname(__file__), "monitor_config.json")
                if os.path.exists(legacy_file):
                    with open(legacy_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        raw = data.get('monitored_uids', {})
                        if raw and _is_old_format(raw):
                            self.monitored_uids = _migrate_old_format(raw)
                        else:
                            self.monitored_uids = raw
                        # 修复：旧路径的 live_status_cache 也需按会话隔离处理
                        old_cache = data.get('live_status_cache', {})
                        if old_cache and isinstance(old_cache, dict):
                            first_val = next(iter(old_cache.values()), None)
                            if first_val is not None and not isinstance(first_val, dict):
                                # 旧格式：继承到每个会话，避免误报开播
                                self.live_status_cache = {}
                                for origin in self.monitored_uids:
                                    self.live_status_cache[origin] = dict(old_cache)
                                logger.info("旧路径缓存已迁移为按会话隔离")
                            else:
                                self.live_status_cache = old_cache
                        else:
                            self.live_status_cache = {}
                        self.enable_notifications = data.get('enable_notifications', self.enable_notifications)
                        self.enable_end_notifications = data.get('enable_end_notifications', self.enable_end_notifications)
                    await self.save_config()
                    logger.info("已从旧路径迁移配置")
                else:
                    logger.info("配置文件不存在，使用默认配置")
        except Exception as e:
            logger.error(f"加载配置文件失败: {e}")

    async def save_config(self):
        """保存监控配置到文件"""
        try:
            data = {
                'monitored_uids': self.monitored_uids,
                'live_status_cache': self.live_status_cache,
                'enable_notifications': self.enable_notifications,
                'enable_end_notifications': self.enable_end_notifications
            }
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            logger.debug("配置文件已保存")
        except Exception as e:
            logger.error(f"保存配置文件失败: {e}")

    def _get_bilibili_headers(self) -> Dict[str, str]:
        """获取B站API请求头，支持Cookie"""
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        cookie = self.config.get("bilibili_cookie", "") if isinstance(self.config, dict) else ""
        if cookie:
            headers["Cookie"] = cookie
        return headers

    async def _save_cookie_to_config(self, cookie: str):
        """保存Cookie到配置（登录管理器回调）"""
        try:
            if isinstance(self.config, dict):
                self.config["bilibili_cookie"] = cookie
                # 尝试保存到插件配置文件
                config_path = os.path.join(self._get_data_dir(), "..", "..", "config", "plugins", "bili_live_notice.json")
                os.makedirs(os.path.dirname(config_path), exist_ok=True)
                with open(config_path, 'w', encoding='utf-8') as f:
                    json.dump(self.config, f, ensure_ascii=False, indent=2)
                logger.info("Cookie已保存到插件配置")
        except Exception as e:
            logger.error(f"保存Cookie到配置失败: {e}")


    async def get_live_status(self, uid: str) -> Dict:
        """获取指定UID的直播状态"""
        try:
            batch = await self.get_live_status_batch([uid])
            if uid in batch:
                return batch[uid]
        except asyncio.TimeoutError:
            logger.error(f"获取UID {uid} 直播状态超时")
        except aiohttp.ClientError as e:
            logger.error(f"网络请求错误 (UID: {uid}): {e}")
        except json.JSONDecodeError as e:
            logger.error(f"JSON解析错误 (UID: {uid}): {e}")
        except ValueError as e:
            logger.error(f"UID格式错误: {uid}, {e}")
        except Exception as e:
            logger.error(f"获取UID {uid} 直播状态失败: {e}")

        return {"live_status": 0, "room_id": 0, "title": "", "uname": "", "cover": ""}

    async def get_live_status_batch(self, uids: list[str]) -> Dict[str, Dict]:
        """批量获取多个UID的直播状态，返回以字符串UID为键的字典"""
        result_map: Dict[str, Dict] = {}
        try:
            await self.ensure_session()
            url = "https://api.live.bilibili.com/room/v1/Room/get_status_info_by_uids"
            data = {"uids": [int(u) for u in uids]}
            headers = self._get_bilibili_headers()
            timeout = aiohttp.ClientTimeout(total=10)
            async with self.session.post(url, json=data, headers=headers, timeout=timeout) as response:
                if response.status == 200:
                    body = await response.json()
                    if body.get("code") == 0:
                        self._last_rate_limited = False
                        data_obj = body.get("data", {})
                        if isinstance(data_obj, dict):
                            for u in uids:
                                key = str(u)
                                user_data = data_obj.get(key)
                                if user_data:
                                    result_map[str(u)] = {
                                        "live_status": user_data.get("live_status", 0),
                                        "room_id": user_data.get("room_id", 0),
                                        "title": user_data.get("title", ""),
                                        "uname": user_data.get("uname", ""),
                                        "cover": user_data.get("cover_from_user", "")
                                    }
                        elif isinstance(data_obj, list):
                            by_uid = {}
                            for entry in data_obj:
                                uid_val = str(entry.get("uid") or entry.get("mid") or "")
                                if uid_val:
                                    by_uid[uid_val] = entry
                            for u in uids:
                                entry = by_uid.get(str(u))
                                if entry:
                                    result_map[str(u)] = {
                                        "live_status": entry.get("live_status", 0),
                                        "room_id": entry.get("room_id", 0),
                                        "title": entry.get("title", ""),
                                        "uname": entry.get("uname", ""),
                                        "cover": entry.get("cover_from_user", "")
                                    }
                    else:
                        logger.warning(f"B站API返回错误码: {body.get('code')}, 消息: {body.get('message', '未知错误')}")
                        # 检测Cookie失效（-101错误）
                        if body.get('code') == -101:
                            cookie = self.config.get("bilibili_cookie", "") if isinstance(self.config, dict) else ""
                            if cookie:
                                asyncio.create_task(self.login_manager.check_and_notify_cookie_invalid(cookie, "B站API返回-101错误"))
                elif response.status == 429:
                    self._last_rate_limited = True
                    logger.warning(f"B站API请求频率限制，状态码: {response.status}")
                else:
                    logger.warning(f"B站API请求失败，状态码: {response.status}")
        except Exception as e:
            logger.error(f"批量获取直播状态失败: {e}")
        finally:
            for u in uids:
                if str(u) not in result_map:
                    result_map[str(u)] = {"live_status": 0, "room_id": 0, "title": "", "uname": "", "cover": ""}
        return result_map

    async def monitor_live_status(self):
        """监控直播状态的后台任务"""
        consecutive_errors = 0
        max_consecutive_errors = 5

        while True:
            try:
                if not self.monitored_uids:
                    await asyncio.sleep(self.check_interval)
                    continue

                # 收集所有需要检查的UID（去重）
                all_uids = set()
                for origin_uids in self.monitored_uids.values():
                    all_uids.update(origin_uids.keys())

                if not all_uids:
                    await asyncio.sleep(self.check_interval)
                    continue

                # 批量查询状态
                now = asyncio.get_running_loop().time()
                uids_to_check = [uid for uid in all_uids if self.uid_skip_until.get(uid, 0) <= now]
                if not uids_to_check:
                    # 修复：所有 uid 都在退避中，直接跳过本轮，避免向 B站 API 传空列表
                    # 空列表会导致 API 返回 "invalid params" 并进一步加剧退避，形成死循环
                    await asyncio.sleep(self.current_interval)
                    continue
                status_map = await self.get_live_status_batch(uids_to_check)

                # 按会话逐个检测并发送通知
                for origin, origin_uids in dict(self.monitored_uids).items():
                    # 确保每个会话都有独立的缓存
                    if origin not in self.live_status_cache:
                        self.live_status_cache[origin] = {}
                    
                    for uid, monitor_info in dict(origin_uids).items():
                        current_status = status_map.get(uid, {"live_status": 0})
                        # 修复：每个会话使用独立的缓存来判断状态变化
                        previous_status = self.live_status_cache[origin].get(uid, 0)

                        # 检测到开播
                        if current_status.get("live_status") == 1 and previous_status != 1:
                            await self.send_live_notification(uid, current_status, origin, monitor_info)

                        # 检测到关播
                        if previous_status == 1 and current_status.get("live_status") != 1:
                            await self.send_end_notification(uid, current_status, origin, monitor_info)

                        # 更新当前会话的缓存
                        self.live_status_cache[origin][uid] = current_status.get("live_status", 0)

                        # 错误统计与退避
                        is_empty = (not current_status.get("uname")) and current_status.get("room_id", 0) == 0
                        if is_empty:
                            cnt = self.uid_error_counts.get(uid, 0) + 1
                            self.uid_error_counts[uid] = cnt
                            self.uid_skip_until[uid] = now + min(300, 30 * cnt)
                        else:
                            self.uid_error_counts.pop(uid, None)
                            self.uid_skip_until.pop(uid, None)

                # 重置错误计数器
                consecutive_errors = 0

                # 基于限流动态调整间隔
                await asyncio.sleep(self.current_interval)
                if self._last_rate_limited:
                    self.current_interval = min(300, max(self.check_interval, int(self.current_interval * 2)))
                else:
                    self.current_interval = max(self.check_interval, int(self.current_interval * 0.75))

            except asyncio.CancelledError:
                logger.info("监控任务被取消")
                break
            except Exception as e:
                consecutive_errors += 1
                logger.error(f"监控任务出错 (第{consecutive_errors}次): {e}")

                if consecutive_errors >= max_consecutive_errors:
                    wait_time = min(300, 60 * consecutive_errors)
                    logger.warning(f"连续错误{consecutive_errors}次，等待{wait_time}秒后重试")
                    await asyncio.sleep(wait_time)
                else:
                    await asyncio.sleep(self.current_interval)

    def _build_message_chain(self, template: str, uname: str, title: str, room_id: int, cover: str, at_all: bool = False) -> MessageChain:
        """根据模板构建消息链"""
        # 替换占位符
        text = template.format(uname=uname, title=title, room_id=room_id, cover="")
        chain = MessageChain()
        if at_all:
            chain.at_all()
        chain.message(text)
        # 如果有封面图，单独添加（不放进模板替换）
        if cover and "{cover}" in template:
            chain.url_image(cover)
        elif cover:
            # 即使模板没写 {cover}，也附带封面
            chain.url_image(cover)
        return chain

    def _get_notify_template(self, is_live: bool) -> str:
        """获取通知模板"""
        if is_live:
            default = "🔴 {uname} 开播啦！\n📺 直播标题: {title}\n🔗 直播间: https://live.bilibili.com/{room_id}"
        else:
            default = "⚫ {uname} 已结束直播"
        key = "live_notify_template" if is_live else "end_notify_template"
        if isinstance(self.config, dict):
            return self.config.get(key, default)
        return default

    async def send_live_notification(self, uid: str, status_info: Dict, origin: str, monitor_info: Dict):
        """发送开播通知"""
        try:
            if not self.enable_notifications:
                logger.info("已禁用开播通知，跳过发送")
                return
            uname = status_info.get("uname", "未知UP主")
            title = status_info.get("title", "无标题")
            room_id = status_info.get("room_id", 0)
            cover = status_info.get("cover", "")
            at_all = monitor_info.get("at_all", False)

            template = self._get_notify_template(is_live=True)
            message_chain = self._build_message_chain(template, uname, title, room_id, cover, at_all)
            await self.context.send_message(origin, message_chain)
            logger.info(f"开播通知已发送: {uname}")
        except Exception as e:
            logger.error(f"发送开播通知失败: {e}")

    async def send_end_notification(self, uid: str, status_info: Dict, origin: str, monitor_info: Dict):
        """发送关播通知"""
        try:
            if not self.enable_notifications or not self.enable_end_notifications:
                return
            uname = status_info.get("uname", "未知UP主")
            room_id = status_info.get("room_id", 0)
            cover = status_info.get("cover", "")
            at_all = monitor_info.get("at_all", False)

            template = self._get_notify_template(is_live=False)
            message_chain = self._build_message_chain(template, uname, "", room_id, cover, at_all)
            await self.context.send_message(origin, message_chain)
            logger.info(f"关播通知已发送: {uname}")
        except Exception as e:
            logger.error(f"发送关播通知失败: {e}")

    def _count_total_monitors(self) -> int:
        """统计所有会话的总监控数"""
        return sum(len(uids) for uids in self.monitored_uids.values())

    def _get_current_origin_monitors(self, origin: str) -> Dict[str, Dict]:
        """获取当前会话的订阅列表"""
        return self.monitored_uids.get(origin, {})

    @filter.command("")
    async def handle_all_messages(self, event: AstrMessageEvent):
        """拦截所有消息，优先处理登录命令"""
        # 尝试处理登录命令
        if await self.login_manager.handle_admin_command(event):
            return  # 命令已处理，不再继续
        # 不是登录命令，继续传递给其他过滤器

    @filter.command("添加监控")
    async def add_monitor(self, event: AstrMessageEvent):
        """添加UP主监控"""
        try:
            args = event.message_str.strip().split()
            if len(args) < 2:
                yield event.plain_result("❌ 使用方法: /添加监控 <UID> [at_all]\n例如: /添加监控 123456\n可选参数 at_all 表示开播时@全体成员")
                return

            uid = args[1]
            if not uid.isdigit():
                yield event.plain_result("❌ UID必须是数字")
                return

            origin = event.unified_msg_origin

            # 检查当前会话是否已订阅此UID
            origin_uids = self.monitored_uids.get(origin, {})
            if uid in origin_uids:
                yield event.plain_result(f"❌ 当前会话已订阅UID {uid}，请勿重复添加")
                return

            # 数量限制（按当前会话）
            if len(origin_uids) >= self.max_monitors:
                yield event.plain_result(f"❌ 当前会话监控数量已达上限({self.max_monitors})")
                return

            # 检查UP主是否存在
            status_info = await self.get_live_status(uid)
            if not status_info.get("uname"):
                yield event.plain_result(f"❌ 未找到UID为 {uid} 的UP主")
                return

            # 解析 at_all 参数
            at_all = "at_all" in args

            # 添加到当前会话的监控列表
            if origin not in self.monitored_uids:
                self.monitored_uids[origin] = {}
            self.monitored_uids[origin][uid] = {
                "uname": status_info.get("uname", ""),
                "room_id": status_info.get("room_id", 0),
                "added_by": event.get_sender_name(),
                "added_time": asyncio.get_running_loop().time(),
                "at_all": at_all
            }
            # 修复：按会话隔离的缓存
            if origin not in self.live_status_cache:
                self.live_status_cache[origin] = {}
            self.live_status_cache[origin][uid] = status_info["live_status"]

            await self.save_config()

            uname = status_info.get("uname", "未知UP主")
            at_all_tip = "（开播时@全体成员）" if at_all else ""
            yield event.plain_result(f"✅ 已添加 {uname}(UID:{uid}) 到当前会话监控列表{at_all_tip}")

        except Exception as e:
            logger.error(f"添加监控失败: {e}")
            yield event.plain_result("❌ 添加监控失败，请稍后重试")

    @filter.command("批量添加监控")
    async def batch_add_monitor(self, event: AstrMessageEvent):
        """批量添加UP主监控"""
        try:
            args = event.message_str.strip().split()
            if len(args) < 2:
                yield event.plain_result("❌ 使用方法: /批量添加监控 <UID1> <UID2> [at_all]\n例如: /批量添加监控 123456 789012")
                return

            at_all = "at_all" in args
            uids = [a for a in args[1:] if a.isdigit()]
            if not uids:
                yield event.plain_result("❌ 未找到有效的UID")
                return

            origin = event.unified_msg_origin
            origin_uids = self.monitored_uids.get(origin, {})
            added = []
            skipped = []
            failed = []

            for uid in uids:
                if uid in origin_uids:
                    skipped.append(uid)
                    continue
                if len(origin_uids) + len(added) >= self.max_monitors:
                    failed.append(f"{uid}(达上限)")
                    continue

                status_info = await self.get_live_status(uid)
                if not status_info.get("uname"):
                    failed.append(f"{uid}(未找到)")
                    continue

                if origin not in self.monitored_uids:
                    self.monitored_uids[origin] = {}
                self.monitored_uids[origin][uid] = {
                    "uname": status_info.get("uname", ""),
                    "room_id": status_info.get("room_id", 0),
                    "added_by": event.get_sender_name(),
                    "added_time": asyncio.get_running_loop().time(),
                    "at_all": at_all
                }
                # 修复：按会话隔离的缓存
                if origin not in self.live_status_cache:
                    self.live_status_cache[origin] = {}
                self.live_status_cache[origin][uid] = status_info["live_status"]
                added.append(f"{status_info.get('uname', '')}(UID:{uid})")

            await self.save_config()

            parts = []
            if added:
                parts.append(f"✅ 已添加: {', '.join(added)}")
            if skipped:
                parts.append(f"⏭ 已跳过(重复): {', '.join(skipped)}")
            if failed:
                parts.append(f"❌ 失败: {', '.join(failed)}")
            yield event.plain_result("\n".join(parts) if parts else "没有可处理的UID")

        except Exception as e:
            logger.error(f"批量添加监控失败: {e}")
            yield event.plain_result("❌ 批量添加监控失败，请稍后重试")

    @filter.command("移除监控")
    async def remove_monitor(self, event: AstrMessageEvent):
        """移除UP主监控"""
        try:
            args = event.message_str.strip().split()
            if len(args) < 2:
                yield event.plain_result("❌ 使用方法: /移除监控 <UID>\n例如: /移除监控 123456")
                return

            uid = args[1]
            if not uid.isdigit():
                yield event.plain_result("❌ UID必须是数字")
                return

            origin = event.unified_msg_origin
            origin_uids = self.monitored_uids.get(origin, {})

            if uid in origin_uids:
                del self.monitored_uids[origin][uid]
                # 如果该会话没有订阅了，清理空字典
                if not self.monitored_uids[origin]:
                    del self.monitored_uids[origin]
                # 注意：不清理live_status_cache，因为其他会话可能还在用
                await self.save_config()
                yield event.plain_result(f"✅ 已移除UID {uid} 的监控")
            else:
                yield event.plain_result(f"❌ 当前会话中UID {uid} 不在监控列表中")

        except Exception as e:
            logger.error(f"移除监控失败: {e}")
            yield event.plain_result("❌ 移除监控失败，请稍后重试")

    @filter.command("批量移除监控")
    async def batch_remove_monitor(self, event: AstrMessageEvent):
        """批量移除UP主监控"""
        try:
            args = event.message_str.strip().split()
            if len(args) < 2:
                yield event.plain_result("❌ 使用方法: /批量移除监控 <UID1> <UID2>\n例如: /批量移除监控 123456 789012")
                return

            uids = [a for a in args[1:] if a.isdigit()]
            if not uids:
                yield event.plain_result("❌ 未找到有效的UID")
                return

            origin = event.unified_msg_origin
            origin_uids = self.monitored_uids.get(origin, {})
            removed = []
            not_found = []

            for uid in uids:
                if uid in origin_uids:
                    del self.monitored_uids[origin][uid]
                    removed.append(uid)
                else:
                    not_found.append(uid)

            # 清理空字典
            if origin in self.monitored_uids and not self.monitored_uids[origin]:
                del self.monitored_uids[origin]

            await self.save_config()

            parts = []
            if removed:
                parts.append(f"✅ 已移除: {', '.join(removed)}")
            if not_found:
                parts.append(f"❌ 未找到: {', '.join(not_found)}")
            yield event.plain_result("\n".join(parts) if parts else "没有可处理的UID")

        except Exception as e:
            logger.error(f"批量移除监控失败: {e}")
            yield event.plain_result("❌ 批量移除监控失败，请稍后重试")

    @filter.command("监控列表")
    async def list_monitors(self, event: AstrMessageEvent):
        """查看当前会话的监控列表"""
        try:
            origin = event.unified_msg_origin
            origin_uids = self._get_current_origin_monitors(origin)

            if not origin_uids:
                yield event.plain_result("📝 当前会话没有监控任何UP主")
                return

            message = "📝 当前会话监控列表:\n"
            for uid, info in origin_uids.items():
                status_info = await self.get_live_status(uid)
                uname = info.get("uname", status_info.get("uname", "未知UP主"))
                live_status = "🔴 直播中" if status_info.get("live_status") == 1 else "⚫ 未开播"
                at_all_tip = " 📢@all" if info.get("at_all") else ""
                message += f"• {uname}(UID:{uid}) - {live_status}{at_all_tip}\n"

            yield event.plain_result(message.strip())

        except Exception as e:
            logger.error(f"获取监控列表失败: {e}")
            yield event.plain_result("❌ 获取监控列表失败，请稍后重试")

    @filter.command("检查直播")
    async def check_live(self, event: AstrMessageEvent):
        """手动检查指定UP主的直播状态"""
        try:
            args = event.message_str.strip().split()
            if len(args) < 2:
                yield event.plain_result("❌ 使用方法: /检查直播 <UID>\n例如: /检查直播 123456")
                return

            uid = args[1]
            if not uid.isdigit():
                yield event.plain_result("❌ UID必须是数字")
                return

            status_info = await self.get_live_status(uid)
            if not status_info.get("uname"):
                yield event.plain_result(f"❌ 未找到UID为 {uid} 的UP主")
                return

            uname = status_info.get("uname", "未知UP主")
            live_status = status_info.get("live_status", 0)

            if live_status == 1:
                title = status_info.get("title", "无标题")
                room_id = status_info.get("room_id", 0)
                cover = status_info.get("cover", "")
                message = f"🔴 {uname} 正在直播\n"
                message += f"📺 直播标题: {title}\n"
                message += f"🔗 直播间: https://live.bilibili.com/{room_id}"
                if cover:
                    yield event.make_result().message(message).url_image(cover)
                    return
            else:
                message = f"⚫ {uname} 当前未开播"

            yield event.plain_result(message)

        except Exception as e:
            logger.error(f"检查直播状态失败: {e}")
            yield event.plain_result("❌ 检查直播状态失败，请稍后重试")

    async def _cleanup_resources(self):
        """清理插件资源"""
        try:
            if self.monitor_task and not self.monitor_task.done():
                self.monitor_task.cancel()
                try:
                    await self.monitor_task
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    logger.error(f"取消监控任务时出错: {e}")
                finally:
                    self.monitor_task = None

            if self.session and not self.session.closed:
                await self.session.close()
                self.session = None

        except Exception as e:
             logger.error(f"清理资源时出错: {e}")

    def get_plugin_status(self) -> Dict:
        """获取插件运行状态"""
        return {
            "session_active": self.session and not self.session.closed,
            "monitor_task_running": self.monitor_task and not self.monitor_task.done(),
            "monitored_count": self._count_total_monitors(),
            "session_count": len(self.monitored_uids),
            "config_file_exists": os.path.exists(self.config_file)
        }

    @filter.command("插件状态")
    async def plugin_status(self, event: AstrMessageEvent):
        """查看插件运行状态"""
        try:
            status = self.get_plugin_status()

            message = "🔧 插件运行状态:\n"
            message += f"• HTTP会话: {'✅ 正常' if status['session_active'] else '❌ 异常'}\n"
            message += f"• 监控任务: {'✅ 运行中' if status['monitor_task_running'] else '❌ 已停止'}\n"
            message += f"• 总监控数量: {status['monitored_count']} 个UP主\n"
            message += f"• 订阅会话数: {status['session_count']} 个\n"
            message += f"• 配置文件: {'✅ 存在' if status['config_file_exists'] else '❌ 缺失'}"

            yield event.plain_result(message)

        except Exception as e:
            logger.error(f"获取插件状态失败: {e}")
            yield event.plain_result("❌ 获取插件状态失败")

    @filter.command("开启通知")
    async def enable_notify_cmd(self, event: AstrMessageEvent):
        try:
            self.enable_notifications = True
            await self.save_config()
            yield event.plain_result("✅ 已开启开播与关播通知")
        except Exception as e:
            logger.error(f"开启通知失败: {e}")
            yield event.plain_result("❌ 开启通知失败")

    @filter.command("关闭通知")
    async def disable_notify_cmd(self, event: AstrMessageEvent):
        try:
            self.enable_notifications = False
            await self.save_config()
            yield event.plain_result("✅ 已关闭所有通知")
        except Exception as e:
            logger.error(f"关闭通知失败: {e}")
            yield event.plain_result("❌ 关闭通知失败")

    @filter.command("开启关播通知")
    async def enable_end_notify_cmd(self, event: AstrMessageEvent):
        try:
            self.enable_end_notifications = True
            await self.save_config()
            yield event.plain_result("✅ 已开启关播通知")
        except Exception as e:
            logger.error(f"开启关播通知失败: {e}")
            yield event.plain_result("❌ 开启关播通知失败")

    @filter.command("关闭关播通知")
    async def disable_end_notify_cmd(self, event: AstrMessageEvent):
        try:
            self.enable_end_notifications = False
            await self.save_config()
            yield event.plain_result("✅ 已关闭关播通知")
        except Exception as e:
            logger.error(f"关闭关播通知失败: {e}")
            yield event.plain_result("❌ 关闭关播通知失败")

    async def terminate(self):
        """插件销毁方法"""
        try:
            logger.info("正在停止B站开播监测插件...")

            if hasattr(self, 'monitored_uids') and self.monitored_uids:
                await self.save_config()
                logger.info("监控配置已保存")

            await self._cleanup_resources()

            logger.info("B站开播监测插件已完全停止")

        except Exception as e:
            logger.error(f"插件销毁时出错: {e}")
            try:
                await self._cleanup_resources()
            except Exception as cleanup_error:
                logger.error(f"强制清理资源时出错: {cleanup_error}")


def _is_old_format(data: Dict) -> bool:
    """检测是否为旧版数据格式（顶层key是UID数字）"""
    if not data:
        return False
    sample_key = next(iter(data))
    # 旧格式: key是UID数字，value包含 unified_msg_origin 字段
    if sample_key.isdigit() and isinstance(data[sample_key], dict):
        return "unified_msg_origin" in data[sample_key]
    return False


def _migrate_old_format(old_data: Dict) -> Dict[str, Dict[str, Dict]]:
    """将旧格式迁移到新格式: {uid: {.., unified_msg_origin}} -> {origin: {uid: {..}}}"""
    new_data: Dict[str, Dict[str, Dict]] = {}
    for uid, info in old_data.items():
        origin = info.pop("unified_msg_origin", "unknown")
        if origin not in new_data:
            new_data[origin] = {}
        # 确保不含旧字段
        info.pop("unified_msg_origin", None)
        info.setdefault("at_all", False)
        new_data[origin][uid] = info
    return new_data
