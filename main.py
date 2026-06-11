import asyncio
import json
from collections import deque
from datetime import datetime

import aiohttp

from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.star import Context, Star, StarTools, register

PLUGIN_NAME = "astrbot_plugin_X_forward"

# X Activity API (XAA)：控制台配置的 Event subscriptions 经此流投递
ACTIVITY_STREAM_URL = "https://api.x.com/2/activity/stream"
ACTIVITY_SUBSCRIPTIONS_URL = "https://api.x.com/2/activity/subscriptions"

# Filtered Stream：基于规则表达式的推文流（可选模式）
FILTERED_STREAM_URL = "https://api.x.com/2/tweets/search/stream"
FILTERED_RULES_URL = "https://api.x.com/2/tweets/search/stream/rules"

# 流式连接以空行作为 keep-alive，超过该时间没有任何数据则视为断线重连
SOCK_READ_TIMEOUT = 40

REF_TYPE_LABEL = {
    "retweeted": "🔁 转推",
    "quoted": "💬 引用",
    "replied_to": "↩️ 回复",
}


@register(
    "astrbot_plugin_X_forward",
    "Nicr0n",
    "订阅 X Activity 事件流 / Filtered Stream，按会话订阅名单将新推文转发到对应会话",
    "v1.3.0",
)
class XForwardPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config
        self._task: asyncio.Task | None = None
        self._seen_ids: deque[str] = deque(maxlen=500)
        self._status = "未启动"
        self._last_tweet_at: str = "无"
        self._forwarded_count = 0

        self._data_dir = StarTools.get_data_dir(PLUGIN_NAME)
        self._subs_file = self._data_dir / "subscriptions.json"
        # { unified_msg_origin: [username, ...] }，用户名统一小写；"*" 表示订阅全部
        self._subs: dict[str, list[str]] = self._load_subs()

        self._register_web_apis()

    def _load_subs(self) -> dict[str, list[str]]:
        try:
            if self._subs_file.exists():
                return json.loads(self._subs_file.read_text("utf-8"))
        except Exception as e:
            logger.error(f"[X Forward] 读取订阅数据失败: {e}")
        return {}

    def _save_subs(self):
        try:
            self._subs_file.write_text(
                json.dumps(self._subs, ensure_ascii=False, indent=2), "utf-8"
            )
        except Exception as e:
            logger.error(f"[X Forward] 保存订阅数据失败: {e}")

    def _stream_mode(self) -> str:
        mode = (self.config.get("stream_mode") or "activity").strip().lower()
        return mode if mode in ("activity", "filtered") else "activity"

    async def initialize(self):
        self._task = asyncio.create_task(self._stream_loop())

    async def terminate(self):
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    # ---------------- WebUI 接口 ----------------

    def _register_web_apis(self):
        """注册 WebUI 插件页面（pages/subscriptions）使用的后端接口"""
        try:
            self.context.register_web_api(
                f"/{PLUGIN_NAME}/overview", self._api_overview, ["GET"], "订阅总览"
            )
            self.context.register_web_api(
                f"/{PLUGIN_NAME}/subscribe", self._api_subscribe, ["POST"], "添加订阅"
            )
            self.context.register_web_api(
                f"/{PLUGIN_NAME}/unsubscribe", self._api_unsubscribe, ["POST"], "移除订阅"
            )
            self.context.register_web_api(
                f"/{PLUGIN_NAME}/events", self._api_events, ["GET"], "X 端事件订阅"
            )
        except Exception as e:
            logger.warning(
                f"[X Forward] 注册 WebUI 接口失败（AstrBot 版本可能不支持插件页面）: {e}"
            )

    async def _api_overview(self):
        from quart import jsonify

        return jsonify(
            {
                "status": self._status,
                "mode": self._stream_mode(),
                "last_tweet_at": self._last_tweet_at,
                "forwarded_count": self._forwarded_count,
                "subscriptions": self._subs,
            }
        )

    async def _api_events(self):
        from quart import jsonify

        try:
            mode, items = await self._fetch_remote_subscriptions()
            return jsonify({"ok": True, "mode": mode, "items": items})
        except Exception as e:
            return jsonify({"ok": False, "message": str(e)}), 502

    async def _api_subscribe(self):
        from quart import jsonify, request

        body = await request.get_json(force=True) or {}
        umo = (body.get("umo") or "").strip()
        raw = body.get("usernames") or []
        if isinstance(raw, str):
            raw = raw.replace("，", " ").replace(",", " ").split()
        usernames = [u.lstrip("@").strip().lower() for u in raw if u.lstrip("@").strip()]
        if not umo or not usernames:
            return jsonify({"ok": False, "message": "缺少会话标识或用户名"}), 400
        subs = set(self._subs.get(umo, []))
        subs.update(usernames)
        self._subs[umo] = sorted(subs)
        self._save_subs()
        return jsonify({"ok": True, "subscriptions": self._subs})

    async def _api_unsubscribe(self):
        from quart import jsonify, request

        body = await request.get_json(force=True) or {}
        umo = (body.get("umo") or "").strip()
        username = (body.get("username") or "").lstrip("@").strip().lower()
        if not umo or umo not in self._subs:
            return jsonify({"ok": False, "message": "会话不存在"}), 400
        if username:
            subs = set(self._subs[umo])
            subs.discard(username)
            if subs:
                self._subs[umo] = sorted(subs)
            else:
                self._subs.pop(umo, None)
        else:
            # 不带 username 表示移除整个会话
            self._subs.pop(umo, None)
        self._save_subs()
        return jsonify({"ok": True, "subscriptions": self._subs})

    # ---------------- 指令 ----------------

    @staticmethod
    def _parse_usernames(event: AstrMessageEvent, subcmd: str) -> list[str]:
        """取出子指令之后的所有用户名参数，去掉可能带的 @ 前缀并统一小写"""
        tokens = event.message_str.split()
        if subcmd in tokens:
            tokens = tokens[tokens.index(subcmd) + 1 :]
        return [t.lstrip("@").lower() for t in tokens if t.lstrip("@")]

    @filter.command_group("xfwd")
    def xfwd(self):
        """X 推文转发插件管理指令"""

    @filter.permission_type(filter.PermissionType.ADMIN)
    @xfwd.command("sub")
    async def sub(self, event: AstrMessageEvent):
        """为当前会话订阅 X 用户，例: /xfwd sub elonmusk NASA。订阅 * 表示接收全部"""
        usernames = self._parse_usernames(event, "sub")
        if not usernames:
            yield event.plain_result("用法: /xfwd sub <用户名> [用户名...]\n用户名为 X 的 @handle（不含 @），订阅 * 表示接收流中的全部推文。")
            return
        umo = event.unified_msg_origin
        subs = set(self._subs.get(umo, []))
        added = [u for u in usernames if u not in subs]
        subs.update(usernames)
        self._subs[umo] = sorted(subs)
        self._save_subs()
        yield event.plain_result(
            f"已为本会话新增订阅: {', '.join(added) if added else '（均已存在）'}\n"
            f"当前订阅: {', '.join(self._subs[umo])}\n"
            f"注意: 用户需已包含在 X 端配置的事件订阅/流规则中，本插件只做按会话分发。"
        )

    @filter.permission_type(filter.PermissionType.ADMIN)
    @xfwd.command("unsub")
    async def unsub(self, event: AstrMessageEvent):
        """取消当前会话对某些 X 用户的订阅，例: /xfwd unsub elonmusk"""
        usernames = self._parse_usernames(event, "unsub")
        if not usernames:
            yield event.plain_result("用法: /xfwd unsub <用户名> [用户名...]")
            return
        umo = event.unified_msg_origin
        subs = set(self._subs.get(umo, []))
        removed = [u for u in usernames if u in subs]
        subs.difference_update(usernames)
        if subs:
            self._subs[umo] = sorted(subs)
        else:
            self._subs.pop(umo, None)
        self._save_subs()
        remain = ", ".join(self._subs.get(umo, [])) or "（无，本会话将不再收到推文）"
        yield event.plain_result(
            f"已取消订阅: {', '.join(removed) if removed else '（本会话未订阅这些用户）'}\n当前订阅: {remain}"
        )

    @filter.permission_type(filter.PermissionType.ADMIN)
    @xfwd.command("list")
    async def list_subs(self, event: AstrMessageEvent):
        """查看当前会话订阅的 X 用户"""
        subs = self._subs.get(event.unified_msg_origin, [])
        if not subs:
            yield event.plain_result("本会话尚未订阅任何 X 用户，使用 /xfwd sub <用户名> 订阅。")
        else:
            yield event.plain_result("本会话订阅的 X 用户:\n" + "\n".join(f"  - {u}" for u in subs))

    @filter.permission_type(filter.PermissionType.ADMIN)
    @xfwd.command("events")
    async def events(self, event: AstrMessageEvent):
        """查看 X 端配置的事件订阅 (Event subscriptions) 或流规则"""
        try:
            mode, items = await self._fetch_remote_subscriptions()
        except Exception as e:
            yield event.plain_result(f"获取 X 端订阅失败: {e}")
            return
        title = (
            "X Activity 事件订阅 (Event subscriptions)"
            if mode == "activity"
            else "Filtered Stream 流规则"
        )
        if not items:
            yield event.plain_result(f"{title}: X 端当前没有任何配置。")
            return
        lines = [f"{title}，共 {len(items)} 条:"]
        for it in items:
            tag = f" 🏷️{it['tag']}" if it.get("tag") else ""
            lines.append(f"  - [{it.get('type', '')}] {it.get('content', '')}{tag}")
            lines.append(f"    id: {it.get('id', '')}")
        yield event.plain_result("\n".join(lines))

    @filter.permission_type(filter.PermissionType.ADMIN)
    @xfwd.command("status")
    async def status(self, event: AstrMessageEvent):
        """查看流连接状态与所有会话的订阅情况"""
        lines = [
            "X 转发插件状态",
            f"流模式: {self._stream_mode()}",
            f"连接状态: {self._status}",
            f"最近收到推文: {self._last_tweet_at}",
            f"累计转发: {self._forwarded_count} 条",
            f"订阅会话: {len(self._subs)} 个",
        ]
        for umo, subs in self._subs.items():
            lines.append(f"  - {umo}: {', '.join(subs)}")
        yield event.plain_result("\n".join(lines))

    @filter.permission_type(filter.PermissionType.ADMIN)
    @xfwd.command("test")
    async def test(self, event: AstrMessageEvent):
        """向所有有订阅的会话发送一条测试消息"""
        if not self._subs:
            yield event.plain_result("尚无任何会话订阅，请先在目标会话发送 /xfwd sub <用户名>")
            return
        for umo in self._subs:
            try:
                await self.context.send_message(
                    umo, MessageChain().message("✅ [X Forward] 测试消息，转发链路正常。")
                )
            except Exception as e:
                logger.error(f"[X Forward] 向 {umo} 发送测试消息失败: {e}")
        yield event.plain_result(f"已向 {len(self._subs)} 个会话发送测试消息。")

    # ---------------- X 端订阅查询 ----------------

    def _auth_ctx(self) -> tuple[str, str | None]:
        token = (self.config.get("bearer_token") or "").strip()
        if not token:
            raise RuntimeError("未配置 Bearer Token")
        proxy = (self.config.get("proxy") or "").strip() or None
        return token, proxy

    async def _fetch_remote_subscriptions(self) -> tuple[str, list[dict]]:
        """查询 X 端配置的订阅。

        activity 模式返回 Event subscriptions (GET /2/activity/subscriptions)，
        filtered 模式返回流规则 (GET /2/tweets/search/stream/rules)。
        统一为 {id, type, content, tag} 列表。
        """
        mode = self._stream_mode()
        token, proxy = self._auth_ctx()
        headers = {"Authorization": f"Bearer {token}"}
        timeout = aiohttp.ClientTimeout(total=30)
        items: list[dict] = []
        async with aiohttp.ClientSession(timeout=timeout) as session:
            if mode == "activity":
                params: dict = {"max_results": "1000"}
                while True:
                    async with session.get(
                        ACTIVITY_SUBSCRIPTIONS_URL, headers=headers, params=params, proxy=proxy
                    ) as resp:
                        body = await resp.json(content_type=None)
                        if resp.status != 200:
                            detail = body.get("detail") or body.get("title") or str(body)[:300]
                            raise RuntimeError(f"HTTP {resp.status}: {detail}")
                        for s in body.get("data", []):
                            flt = s.get("filter") or {}
                            parts = []
                            if flt.get("user_id"):
                                parts.append(f"user_id={flt['user_id']}")
                            if flt.get("keyword"):
                                parts.append(f"keyword={flt['keyword']}")
                            if flt.get("direction"):
                                parts.append(f"direction={flt['direction']}")
                            items.append(
                                {
                                    "id": s.get("subscription_id", ""),
                                    "type": s.get("event_type", ""),
                                    "content": ", ".join(parts) or "-",
                                    "tag": s.get("tag", ""),
                                }
                            )
                        next_token = (body.get("meta") or {}).get("next_token")
                        if not next_token:
                            break
                        params["pagination_token"] = next_token
            else:
                async with session.get(
                    FILTERED_RULES_URL, headers=headers, proxy=proxy
                ) as resp:
                    body = await resp.json(content_type=None)
                    if resp.status != 200:
                        detail = body.get("detail") or body.get("title") or str(body)[:300]
                        raise RuntimeError(f"HTTP {resp.status}: {detail}")
                    for r in body.get("data", []):
                        items.append(
                            {
                                "id": r.get("id", ""),
                                "type": "rule",
                                "content": r.get("value", ""),
                                "tag": r.get("tag", ""),
                            }
                        )
        return mode, items

    # ---------------- 流式连接 ----------------

    def _build_params(self, mode: str) -> dict:
        params: dict = {}
        if mode == "filtered":
            params = {
                "tweet.fields": self.config.get(
                    "tweet_fields", "id,text,created_at,author_id,attachments,referenced_tweets"
                ),
                "expansions": self.config.get(
                    "expansions", "author_id,attachments.media_keys"
                ),
                "user.fields": self.config.get("user_fields", "name,username"),
                "media.fields": self.config.get("media_fields", "type,url,preview_image_url"),
            }
        backfill = int(self.config.get("backfill_minutes", 0) or 0)
        if backfill > 0:
            params["backfill_minutes"] = str(min(backfill, 5))
        return params

    async def _stream_loop(self):
        # 等待各平台适配器加载完成，避免启动初期推送失败
        await asyncio.sleep(5)
        net_backoff = 0.0
        http_backoff = 5.0
        rate_backoff = 60.0
        while True:
            token = (self.config.get("bearer_token") or "").strip()
            if not token:
                self._status = "未配置 Bearer Token"
                logger.warning("[X Forward] 未配置 bearer_token，请在插件配置中填写，60 秒后重试")
                await asyncio.sleep(60)
                continue

            proxy = (self.config.get("proxy") or "").strip() or None
            try:
                await self._connect_and_consume(token, proxy)
                # 正常返回说明服务端主动断开，重置退避后立即重连
                net_backoff = 0.0
                http_backoff = 5.0
                rate_backoff = 60.0
            except asyncio.CancelledError:
                self._status = "已停止"
                raise
            except aiohttp.ClientResponseError as e:
                if e.status in (401, 403):
                    self._status = f"认证失败 (HTTP {e.status})"
                    logger.error(
                        f"[X Forward] 认证失败 (HTTP {e.status})，请检查 Bearer Token "
                        f"以及开发者套餐权限，10 分钟后重试"
                    )
                    await asyncio.sleep(600)
                elif e.status == 402:
                    self._status = "API 额度耗尽 (HTTP 402)"
                    logger.error(
                        "[X Forward] X API credits 已耗尽 (HTTP 402)，"
                        "请前往 X 开发者控制台充值或升级套餐，30 分钟后重试"
                    )
                    await asyncio.sleep(1800)
                elif e.status == 429:
                    self._status = "触发限流 (HTTP 429)"
                    logger.warning(f"[X Forward] 触发限流，{rate_backoff:.0f} 秒后重连")
                    await asyncio.sleep(rate_backoff)
                    rate_backoff = min(rate_backoff * 2, 900)
                else:
                    self._status = f"服务端错误 (HTTP {e.status})"
                    logger.warning(f"[X Forward] HTTP {e.status}，{http_backoff:.0f} 秒后重连")
                    await asyncio.sleep(http_backoff)
                    http_backoff = min(http_backoff * 2, 320)
            except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as e:
                self._status = f"网络错误: {type(e).__name__}"
                net_backoff = min(net_backoff + 1, 16)
                logger.warning(f"[X Forward] 网络异常 ({e})，{net_backoff:.0f} 秒后重连")
                await asyncio.sleep(net_backoff)
            except Exception as e:
                self._status = f"未知错误: {type(e).__name__}"
                logger.error(f"[X Forward] 流处理出现未知异常: {e}", exc_info=True)
                await asyncio.sleep(30)

    async def _connect_and_consume(self, token: str, proxy: str | None):
        mode = self._stream_mode()
        url = ACTIVITY_STREAM_URL if mode == "activity" else FILTERED_STREAM_URL
        timeout = aiohttp.ClientTimeout(total=None, sock_connect=30, sock_read=SOCK_READ_TIMEOUT)
        headers = {"Authorization": f"Bearer {token}"}
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(
                url, headers=headers, params=self._build_params(mode), proxy=proxy
            ) as resp:
                if resp.status != 200:
                    body = (await resp.text())[:500]
                    logger.error(f"[X Forward] 连接失败 HTTP {resp.status}: {body}")
                    raise aiohttp.ClientResponseError(
                        resp.request_info, resp.history, status=resp.status, message=body
                    )
                self._status = f"已连接 ({mode})"
                logger.info(f"[X Forward] {url} 已连接，等待事件...")
                async for raw_line in resp.content:
                    line = raw_line.strip()
                    if not line:
                        continue  # keep-alive 空行
                    try:
                        payload = json.loads(line)
                    except json.JSONDecodeError:
                        logger.warning(f"[X Forward] 收到无法解析的数据: {line[:200]!r}")
                        continue
                    await self._dispatch(mode, payload)
        logger.info("[X Forward] 服务端断开连接，准备重连")
        self._status = "连接断开，重连中"

    async def _dispatch(self, mode: str, payload: dict):
        if "errors" in payload and "data" not in payload:
            logger.warning(f"[X Forward] 流内错误事件: {payload['errors']}")
            return
        data = payload.get("data")
        if not data:
            return
        if mode == "activity":
            event_type = data.get("event_type", "")
            if event_type == "post.create":
                tweet = data.get("payload") or {}
                includes = data.get("includes") or payload.get("includes") or {}
                await self._handle_tweet(tweet, includes, [])
            elif event_type.startswith("post.delete"):
                pass  # 暂不处理删除事件
            elif event_type:
                logger.info(f"[X Forward] 收到未处理的事件类型: {event_type}")
        else:
            await self._handle_tweet(
                data, payload.get("includes", {}), payload.get("matching_rules", [])
            )

    # ---------------- 推文处理 ----------------

    def _match_targets(self, username: str) -> list[str]:
        """返回订阅了该作者（或订阅了 *）的会话列表"""
        username = username.lower()
        return [
            umo
            for umo, subs in self._subs.items()
            if "*" in subs or username in subs
        ]

    def _author_of(self, tweet: dict, includes: dict) -> tuple[str, str]:
        """返回 (username, 显示名)。Activity 事件的 payload 自带 username"""
        users = {u.get("id"): u for u in includes.get("users", [])}
        author = users.get(tweet.get("author_id"), {})
        username = tweet.get("username") or author.get("username", "")
        name = author.get("name") or username
        return username, name

    async def _handle_tweet(self, tweet: dict, includes: dict, matching_rules: list):
        tweet_id = tweet.get("id", "")
        if tweet_id and tweet_id in self._seen_ids:
            return  # 重连 backfill 可能产生重复
        if tweet_id:
            self._seen_ids.append(tweet_id)

        username, _ = self._author_of(tweet, includes)
        if not username:
            logger.warning(
                f"[X Forward] 推文 {tweet_id} 缺少作者信息，仅发送给订阅了 * 的会话"
            )

        targets = self._match_targets(username) if username else [
            umo for umo, subs in self._subs.items() if "*" in subs
        ]

        self._last_tweet_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if not targets:
            logger.info(f"[X Forward] @{username or '?'} 的推文没有会话订阅，已忽略")
            return

        chain = self._build_message(tweet, includes, matching_rules)
        for umo in targets:
            try:
                await self.context.send_message(umo, chain)
                self._forwarded_count += 1
            except Exception as e:
                logger.error(f"[X Forward] 转发到 {umo} 失败: {e}")

    def _build_message(self, tweet: dict, includes: dict, matching_rules: list) -> MessageChain:
        username, name = self._author_of(tweet, includes)
        username = username or "unknown"
        name = name or username

        ref_label = ""
        for ref in tweet.get("referenced_tweets", []):
            label = REF_TYPE_LABEL.get(ref.get("type"))
            if label:
                ref_label = f"{label} | "
                break

        lines = [f"🐦 {ref_label}{name} (@{username})", "", tweet.get("text", "")]

        created_at = tweet.get("created_at")
        if created_at:
            try:
                local = datetime.fromisoformat(created_at.replace("Z", "+00:00")).astimezone()
                lines += ["", f"🕒 {local.strftime('%Y-%m-%d %H:%M:%S')}"]
            except ValueError:
                pass

        tags = [r["tag"] for r in matching_rules if r.get("tag")]
        if tags:
            lines.append(f"🏷️ 命中规则: {', '.join(tags)}")

        lines.append(f"🔗 https://x.com/{username}/status/{tweet.get('id', '')}")

        chain = MessageChain().message("\n".join(lines))

        if self.config.get("send_media", True):
            media_map = {m["media_key"]: m for m in includes.get("media", []) if "media_key" in m}
            for key in tweet.get("attachments", {}).get("media_keys", []):
                media = media_map.get(key, {})
                url = media.get("url") or media.get("preview_image_url")
                if url:
                    chain.url_image(url)
        return chain
