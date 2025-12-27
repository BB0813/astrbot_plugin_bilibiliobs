import asyncio
import aiohttp
import json
import os
from typing import Dict
from astrbot.api.event import filter, AstrMessageEvent, MessageEventResult, MessageChain
from astrbot.api.star import Context, Star, register
from astrbot.api import logger, AstrBotConfig

@register("bili_live_notice", "Binbim", "B站UP主开播监测插件", "1.0.0", "https://github.com/Binbim/astrbot_plugin_BiliBiliOBS")
class BiliLiveNoticePlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig = None):
        super().__init__(context)
        self.config = config or {}
        self.check_interval = int(self.config.get("check_interval", 60)) if isinstance(self.config, dict) else 60
        self.max_monitors = int(self.config.get("max_monitors", 50)) if isinstance(self.config, dict) else 50
        self.enable_notifications = bool(self.config.get("enable_notifications", True)) if isinstance(self.config, dict) else True
        self.enable_end_notifications = bool(self.config.get("enable_end_notifications", True)) if isinstance(self.config, dict) else True
        self.monitored_uids: Dict[str, Dict] = {}  # 存储监控的UP主信息
        self.live_status_cache: Dict[str, int] = {}  # 缓存直播状态
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
        # 启动初始化任务
        asyncio.create_task(self.initialize())
        
    def _get_data_dir(self) -> str:
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
                logger.info(f"已加载 {len(self.monitored_uids)} 个监控配置")
                
                # 启动监控任务
                if not self.monitor_task or self.monitor_task.done():
                    self.monitor_task = asyncio.create_task(self.monitor_live_status())
                    logger.info("监控任务已启动")
                
                self._initialized = True
                logger.info("B站开播监测插件初始化完成")
                
            except Exception as e:
                logger.error(f"插件初始化失败: {e}")
                # 清理已创建的资源
                await self._cleanup_resources()
                raise
    
    async def load_config(self):
        """加载监控配置文件"""
        try:
            # 优先从新路径读取
            if os.path.exists(self.config_file):
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.monitored_uids = data.get('monitored_uids', {})
                    self.live_status_cache = data.get('live_status_cache', {})
                    self.enable_notifications = data.get('enable_notifications', self.enable_notifications)
                    self.enable_end_notifications = data.get('enable_end_notifications', self.enable_end_notifications)
                    logger.info(f"已加载 {len(self.monitored_uids)} 个监控配置")
            else:
                # 兼容旧路径迁移
                legacy_file = os.path.join(os.path.dirname(__file__), "monitor_config.json")
                if os.path.exists(legacy_file):
                    with open(legacy_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        self.monitored_uids = data.get('monitored_uids', {})
                        self.live_status_cache = data.get('live_status_cache', {})
                        self.enable_notifications = data.get('enable_notifications', self.enable_notifications)
                        self.enable_end_notifications = data.get('enable_end_notifications', self.enable_end_notifications)
                        logger.info(f"已从旧路径迁移 {len(self.monitored_uids)} 个监控配置")
                    # 保存到新路径
                    await self.save_config()
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
        
        return {"live_status": 0, "room_id": 0, "title": "", "uname": ""}
    
    async def get_live_status_batch(self, uids: list[str]) -> Dict[str, Dict]:
        """批量获取多个UID的直播状态，返回以字符串UID为键的字典"""
        result_map: Dict[str, Dict] = {}
        try:
            await self.ensure_session()
            url = "https://api.live.bilibili.com/room/v1/Room/get_status_info_by_uids"
            data = {"uids": [int(u) for u in uids]}
            headers = {
                "Content-Type": "application/json",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }
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
                                        "uname": user_data.get("uname", "")
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
                                        "uname": entry.get("uname", "")
                                    }
                    else:
                        logger.warning(f"B站API返回错误码: {body.get('code')}, 消息: {body.get('message', '未知错误')}")
                elif response.status == 429:
                    self._last_rate_limited = True
                    logger.warning(f"B站API请求频率限制，状态码: {response.status}")
                else:
                    logger.warning(f"B站API请求失败，状态码: {response.status}")
        except Exception as e:
            logger.error(f"批量获取直播状态失败: {e}")
        finally:
            # 为未返回的数据填充默认项
            for u in uids:
                if str(u) not in result_map:
                    result_map[str(u)] = {"live_status": 0, "room_id": 0, "title": "", "uname": ""}
        return result_map
    
    async def monitor_live_status(self):
        """监控直播状态的后台任务"""
        consecutive_errors = 0
        max_consecutive_errors = 5
        
        while True:
            try:
                # 如果没有监控对象，等待后继续
                if not self.monitored_uids:
                    await asyncio.sleep(self.check_interval)
                    continue
                
                # 复制字典以避免在迭代过程中修改
                monitored_copy = dict(self.monitored_uids)
                
                # 批量查询状态
                now = asyncio.get_running_loop().time()
                uids = [uid for uid in monitored_copy.keys() if self.uid_skip_until.get(uid, 0) <= now]
                status_map = await self.get_live_status_batch(uids)
                
                for uid, info in monitored_copy.items():
                    current_status = status_map.get(uid, {"live_status": 0})
                    previous_status = self.live_status_cache.get(uid, 0)
                    
                    # 检测到开播
                    if current_status.get("live_status") == 1 and previous_status != 1:
                        await self.send_live_notification(uid, current_status, info)
                    
                    # 检测到关播
                    if previous_status == 1 and current_status.get("live_status") != 1:
                        await self.send_end_notification(uid, current_status, info)
                    
                    # 更新缓存
                    self.live_status_cache[uid] = current_status.get("live_status", 0)
                    
                    # 错误统计与退避：当返回为空信息时提高退避
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
                
                # 每60秒检查一次
                # 基于限流动态调整间隔
                await asyncio.sleep(self.current_interval)
                if self._last_rate_limited:
                    self.current_interval = min(300, max(self.check_interval, int(self.current_interval * 2)))
                else:
                    # 逐步回落到配置的基础间隔
                    self.current_interval = max(self.check_interval, int(self.current_interval * 0.75))
                
            except asyncio.CancelledError:
                logger.info("监控任务被取消")
                break
            except Exception as e:
                consecutive_errors += 1
                logger.error(f"监控任务出错 (第{consecutive_errors}次): {e}")
                
                # 如果连续错误次数过多，增加等待时间
                if consecutive_errors >= max_consecutive_errors:
                    wait_time = min(300, 60 * consecutive_errors)  # 最多等待5分钟
                    logger.warning(f"连续错误{consecutive_errors}次，等待{wait_time}秒后重试")
                    await asyncio.sleep(wait_time)
                else:
                    await asyncio.sleep(self.current_interval)  # 正常等待
    
    async def send_live_notification(self, uid: str, status_info: Dict, monitor_info: Dict):
        """发送开播通知"""
        try:
            if not self.enable_notifications:
                logger.info("已禁用开播通知，跳过发送")
                return
            uname = status_info.get("uname", "未知UP主")
            title = status_info.get("title", "无标题")
            room_id = status_info.get("room_id", 0)
            
            message = f"🔴 {uname} 开播啦！\n"
            message += f"📺 直播标题: {title}\n"
            message += f"🔗 直播间: https://live.bilibili.com/{room_id}"
            
            # 使用AstrBot的消息发送机制
            unified_msg_origin = monitor_info.get("unified_msg_origin")
            if unified_msg_origin:
                message_chain = MessageChain().message(message)
                await self.context.send_message(unified_msg_origin, message_chain)
                logger.info(f"开播通知已发送: {uname}")
            else:
                logger.warning(f"无法发送开播通知，缺少unified_msg_origin: {uid}")
            
        except Exception as e:
            logger.error(f"发送开播通知失败: {e}")
    
    async def send_end_notification(self, uid: str, status_info: Dict, monitor_info: Dict):
        try:
            if not self.enable_notifications or not self.enable_end_notifications:
                return
            uname = status_info.get("uname", "未知UP主")
            message = f"⚫ {uname} 已结束直播"
            unified_msg_origin = monitor_info.get("unified_msg_origin")
            if unified_msg_origin:
                message_chain = MessageChain().message(message)
                await self.context.send_message(unified_msg_origin, message_chain)
                logger.info(f"关播通知已发送: {uname}")
        except Exception as e:
            logger.error(f"发送关播通知失败: {e}")
    
    @filter.command("添加监控")
    async def add_monitor(self, event: AstrMessageEvent):
        """添加UP主监控"""
        try:
            # 解析命令参数
            args = event.message_str.strip().split()
            if len(args) < 2:
                yield event.plain_result("❌ 使用方法: /添加监控 <UID>\n例如: /添加监控 123456")
                return
            
            uid = args[1]
            if not uid.isdigit():
                yield event.plain_result("❌ UID必须是数字")
                return
            
            # 数量限制
            if len(self.monitored_uids) >= self.max_monitors:
                yield event.plain_result(f"❌ 监控数量已达上限({self.max_monitors})")
                return
            
            # 检查UP主是否存在
            status_info = await self.get_live_status(uid)
            if not status_info.get("uname"):
                yield event.plain_result(f"❌ 未找到UID为 {uid} 的UP主")
                return
            
            # 添加到监控列表
            self.monitored_uids[uid] = {
                "uname": status_info.get("uname", ""),
                "room_id": status_info.get("room_id", 0),
                "added_by": event.get_sender_name(),
                "added_time": asyncio.get_running_loop().time(),
                "unified_msg_origin": event.unified_msg_origin
            }
            self.live_status_cache[uid] = status_info["live_status"]
            
            # 保存配置
            await self.save_config()
            
            uname = status_info.get("uname", "未知UP主")
            yield event.plain_result(f"✅ 已添加 {uname}(UID:{uid}) 到监控列表")
            
        except Exception as e:
            logger.error(f"添加监控失败: {e}")
            yield event.plain_result("❌ 添加监控失败，请稍后重试")
    
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
                
            if uid in self.monitored_uids:
                del self.monitored_uids[uid]
                if uid in self.live_status_cache:
                    del self.live_status_cache[uid]
                # 保存配置
                await self.save_config()
                yield event.plain_result(f"✅ 已移除UID {uid} 的监控")
            else:
                yield event.plain_result(f"❌ UID {uid} 不在监控列表中")
                
        except Exception as e:
            logger.error(f"移除监控失败: {e}")
            yield event.plain_result("❌ 移除监控失败，请稍后重试")
    
    @filter.command("监控列表")
    async def list_monitors(self, event: AstrMessageEvent):
        """查看监控列表"""
        try:
            if not self.monitored_uids:
                yield event.plain_result("📝 当前没有监控任何UP主")
                return
            
            message = "📝 当前监控列表:\n"
            for uid, info in self.monitored_uids.items():
                status_info = await self.get_live_status(uid)
                uname = status_info.get("uname", "未知UP主")
                live_status = "🔴 直播中" if status_info.get("live_status") == 1 else "⚫ 未开播"
                message += f"• {uname}(UID:{uid}) - {live_status}\n"
            
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
                message = f"🔴 {uname} 正在直播\n"
                message += f"📺 直播标题: {title}\n"
                message += f"🔗 直播间: https://live.bilibili.com/{room_id}"
            else:
                message = f"⚫ {uname} 当前未开播"
            
            yield event.plain_result(message)
            
        except Exception as e:
            logger.error(f"检查直播状态失败: {e}")
            yield event.plain_result("❌ 检查直播状态失败，请稍后重试")

    async def _cleanup_resources(self):
        """清理插件资源"""
        try:
            # 取消监控任务
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
            
            # 关闭HTTP会话
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
            "monitored_count": len(self.monitored_uids),
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
            message += f"• 监控数量: {status['monitored_count']} 个UP主\n"
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
            
            # 保存当前配置
            if hasattr(self, 'monitored_uids') and self.monitored_uids:
                await self.save_config()
                logger.info("监控配置已保存")
            
            # 清理所有资源
            await self._cleanup_resources()
            
            logger.info("B站开播监测插件已完全停止")
            
        except Exception as e:
            logger.error(f"插件销毁时出错: {e}")
            # 即使出错也要尝试清理资源
            try:
                await self._cleanup_resources()
            except Exception as cleanup_error:
                logger.error(f"强制清理资源时出错: {cleanup_error}")
