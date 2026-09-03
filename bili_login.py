"""B站Cookie扫码登录模块"""
import asyncio
import aiohttp
import hashlib
import json
import os
import tempfile
import time
import qrcode
from typing import Dict, Optional, Any
from astrbot.api.event import AstrMessageEvent
from astrbot.api.message_components import Image, Plain
from astrbot.api import logger

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"


class BilibiliLoginManager:
    """B站登录管理器"""

    QRCODE_GENERATE_URL = "https://passport.bilibili.com/x/passport-login/web/qrcode/generate"
    QRCODE_POLL_URL = "https://passport.bilibili.com/x/passport-login/web/qrcode/poll"
    NAV_URL = "https://api.bilibili.com/x/web-interface/nav"
    QR_CODE_TTL_SECONDS = 180

    def __init__(self, context, config_save_callback):
        """初始化登录管理器

        Args:
            context: AstrBot Context 对象
            config_save_callback: 保存配置的回调函数，签名为 async def(cookie: str)
        """
        self.context = context
        self.config_save_callback = config_save_callback
        self._login_in_progress = False
        self._lock = asyncio.Lock()
        self._admin_id = None  # 管理员ID，首次私聊时自动记录

    async def validate_cookie(self, cookie: str) -> Optional[bool]:
        """验证Cookie是否有效

        Returns:
            True: Cookie有效
            False: Cookie无效（-101错误）
            None: 网络错误或其他异常
        """
        if not cookie or not cookie.strip():
            return False

        headers = {
            "User-Agent": UA,
            "Referer": "https://www.bilibili.com",
            "Origin": "https://www.bilibili.com",
            "Cookie": cookie.strip(),
        }

        try:
            timeout = aiohttp.ClientTimeout(total=10)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(self.NAV_URL, headers=headers) as resp:
                    if resp.content_type != "application/json":
                        return None
                    data = await resp.json()

                    if data.get("code") != 0:
                        error_code = int(data.get("code", 0))
                        if error_code == -101:
                            return False
                        return None

                    nav_data = data.get("data") or {}
                    if "isLogin" not in nav_data:
                        return None
                    return bool(nav_data.get("isLogin"))
        except Exception as e:
            logger.error(f"验证Cookie失败: {e}")
            return None

    async def handle_admin_command(self, event: AstrMessageEvent) -> bool:
        """处理管理员更新Cookie命令

        Returns:
            True: 命令已处理
            False: 不是此命令
        """
        # 只处理私聊消息
        if not self._is_private_message(event):
            return False

        message_text = (event.message_str or "").strip()
        if message_text.lower() not in ["更新cookie", "更新b站cookie", "b站登录", "bilibili登录"]:
            return False

        # 记录管理员ID
        if not self._admin_id:
            self._admin_id = event.unified_msg_origin
            logger.info(f"已记录管理员ID: {self._admin_id}")

        await self._start_login_flow(event)
        return True

    async def check_and_notify_cookie_invalid(self, cookie: str, reason: str = ""):
        """检查Cookie是否失效，如果失效则通知管理员

        Args:
            cookie: 当前配置的Cookie
            reason: 失效原因
        """
        if not self._admin_id:
            logger.warning("未记录管理员ID，无法发送Cookie失效通知")
            return

        is_valid = await self.validate_cookie(cookie)
        if is_valid is False:
            reason_text = reason or "Cookie已失效"
            message = (
                f"⚠️ 检测到B站Cookie不可用\n"
                f"原因: {reason_text}\n\n"
                f"请回复以下任一命令进行扫码登录续期：\n"
                f"• 更新cookie\n"
                f"• b站登录\n"
                f"或手动在插件配置中更新 bilibili_cookie"
            )
            try:
                await self.context.send_message(self._admin_id, message)
                logger.info("已向管理员发送Cookie失效通知")
            except Exception as e:
                logger.error(f"发送Cookie失效通知失败: {e}")

    def _is_private_message(self, event: AstrMessageEvent) -> bool:
        """判断是否为私聊消息"""
        origin = event.unified_msg_origin or ""
        # 简单判断：私聊通常不包含群标识
        return "group" not in origin.lower() and "qq_" in origin.lower()

    async def _start_login_flow(self, event: AstrMessageEvent):
        """启动扫码登录流程"""
        async with self._lock:
            if self._login_in_progress:
                await event.send(event.plain_result("⚠️ 已有一轮扫码登录正在进行，请等待完成"))
                return
            self._login_in_progress = True

        try:
            # 生成二维码
            timeout = aiohttp.ClientTimeout(total=10)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                payload = await self._generate_login_payload(session)

            # 发送二维码
            await self._send_qrcode(event, payload["login_url"])

            # 启动轮询任务
            asyncio.create_task(
                self._poll_login_and_notify(
                    qrcode_key=payload["qrcode_key"],
                    unified_msg_origin=event.unified_msg_origin,
                )
            )

        except Exception as e:
            self._login_in_progress = False
            logger.error(f"启动扫码登录失败: {e}")
            await event.send(event.plain_result(f"❌ 启动扫码登录失败: {e}"))

    async def _generate_login_payload(self, session: aiohttp.ClientSession) -> Dict[str, str]:
        """生成登录二维码数据"""
        headers = {
            "User-Agent": UA,
            "Referer": "https://www.bilibili.com",
            "Origin": "https://www.bilibili.com",
        }

        async with session.get(self.QRCODE_GENERATE_URL, headers=headers) as resp:
            data = await resp.json()

        if data.get("code") != 0:
            raise RuntimeError(f"生成二维码失败: {data.get('message', '未知错误')}")

        payload = data.get("data") or {}
        login_url = str(payload.get("url", "")).strip()
        qrcode_key = str(payload.get("qrcode_key", "")).strip()

        if not login_url or not qrcode_key:
            raise RuntimeError("生成二维码失败: 响应数据不完整")

        return {"login_url": login_url, "qrcode_key": qrcode_key}

    async def _send_qrcode(self, event: AstrMessageEvent, login_url: str):
        """发送二维码图片"""
        qr_path = None
        try:
            # 生成二维码图片
            qr_path = await asyncio.to_thread(self._create_qrcode_image, login_url)

            # 构建消息链
            chain = [
                Plain(
                    "📱 请使用哔哩哔哩客户端扫描下方二维码完成登录\n"
                    f"⏱ 二维码有效期: {self.QR_CODE_TTL_SECONDS // 60} 分钟\n\n"
                    "若无法显示图片，可在手机浏览器打开：\n"
                    f"{login_url}"
                ),
                Image.fromFileSystem(qr_path),
            ]

            await event.send(event.chain_result(chain))
            logger.info("二维码已发送")

        except Exception as e:
            logger.warning(f"发送二维码图片失败: {e}，回退为纯文本")
            await event.send(
                event.plain_result(
                    f"📱 请在手机浏览器打开以下链接完成登录：\n{login_url}\n"
                    f"⏱ 链接有效期: {self.QR_CODE_TTL_SECONDS // 60} 分钟"
                )
            )

        finally:
            if qr_path and os.path.exists(qr_path):
                try:
                    os.remove(qr_path)
                except Exception:
                    pass

    @staticmethod
    def _create_qrcode_image(login_url: str) -> str:
        """创建二维码图片文件"""
        fd, qr_path = tempfile.mkstemp(prefix="bili_qr_", suffix=".png")
        os.close(fd)

        try:
            qr = qrcode.QRCode(
                version=None,
                error_correction=qrcode.constants.ERROR_CORRECT_M,
                box_size=8,
                border=4,
            )
            qr.add_data(login_url)
            qr.make(fit=True)
            qr.make_image(fill_color="black", back_color="white").save(qr_path)
            return qr_path
        except Exception:
            try:
                os.remove(qr_path)
            except Exception:
                pass
            raise

    async def _poll_login_and_notify(self, qrcode_key: str, unified_msg_origin: str):
        """轮询登录状态并通知结果"""
        try:
            deadline = time.monotonic() + self.QR_CODE_TTL_SECONDS
            headers = {
                "User-Agent": UA,
                "Referer": "https://www.bilibili.com",
                "Origin": "https://www.bilibili.com",
            }

            timeout = aiohttp.ClientTimeout(total=10)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                while time.monotonic() < deadline:
                    await asyncio.sleep(2)

                    try:
                        async with session.get(
                            self.QRCODE_POLL_URL,
                            params={"qrcode_key": qrcode_key},
                            headers=headers,
                        ) as resp:
                            poll_data = await resp.json()
                            poll_result = poll_data.get("data", {}) or {}
                            code = poll_result.get("code")

                            if code == 0:
                                # 登录成功
                                cookie = self._extract_cookie(resp)
                                if cookie:
                                    await self.config_save_callback(cookie)
                                    await self.context.send_message(
                                        unified_msg_origin,
                                        "✅ B站扫码登录成功，Cookie已自动更新！"
                                    )
                                    logger.info("B站登录成功，Cookie已更新")
                                else:
                                    await self.context.send_message(
                                        unified_msg_origin,
                                        "❌ 登录成功但未获取到有效Cookie，请手动配置"
                                    )
                                    logger.error("登录成功但Cookie提取失败")
                                return

                            elif code == 86038:
                                # 二维码已过期
                                await self.context.send_message(
                                    unified_msg_origin,
                                    "⏰ 二维码已过期，请重新发起登录"
                                )
                                logger.info("二维码已过期")
                                return

                            elif code in (86090, 86101):
                                # 86090: 未扫描, 86101: 已扫描未确认
                                continue

                    except Exception as e:
                        logger.error(f"轮询登录状态失败: {e}")
                        await asyncio.sleep(2)
                        continue

                # 超时
                await self.context.send_message(
                    unified_msg_origin,
                    "⏰ 扫码登录超时，请重新发起"
                )
                logger.info("扫码登录超时")

        except Exception as e:
            logger.error(f"扫码登录流程异常: {e}")
            try:
                await self.context.send_message(
                    unified_msg_origin,
                    f"❌ 扫码登录失败: {e}"
                )
            except Exception:
                pass

        finally:
            self._login_in_progress = False

    @staticmethod
    def _extract_cookie(resp: aiohttp.ClientResponse) -> str:
        """从登录响应中提取Cookie"""
        cookies = {}

        # 提取Set-Cookie头
        set_cookie_headers = resp.headers.getall("Set-Cookie", [])
        for set_cookie in set_cookie_headers:
            # 简单解析：key=value; ...
            parts = set_cookie.split(";")
            if parts:
                kv = parts[0].split("=", 1)
                if len(kv) == 2:
                    cookies[kv[0].strip()] = kv[1].strip()

        # 构建Cookie字符串
        required_keys = ["SESSDATA", "bili_jct", "DedeUserID", "DedeUserID__ckMd5"]
        cookie_parts = []
        for key in required_keys:
            if key in cookies:
                cookie_parts.append(f"{key}={cookies[key]}")

        if not cookie_parts:
            return ""

        return "; ".join(cookie_parts)
