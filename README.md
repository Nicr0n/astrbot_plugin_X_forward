# astrbot_plugin_X_forward

订阅 [X API Filtered Stream](https://docs.x.com/x-api/posts/filtered-stream/introduction)（`GET /2/tweets/search/stream`），**按会话各自的订阅名单**将新推文转发到对应会话（QQ / Telegram / Discord 等 AstrBot 支持的平台）：每个群订阅不同的 X 用户，推文按**作者用户名**路由，只发给订阅了该作者的群。

订阅以流规则中的 `from:` 用户为有效索引：订阅时实时解析所有规则中的 `from:用户名`，只允许订阅已被规则覆盖的用户（杜绝"订阅了但规则没覆盖、永远收不到推文"的静默失效）；规则被删除后，相关订阅会在 `/xfwd list` 和 WebUI 中标注为已失效。

插件只维持一条流式连接并按作者分发——群里订阅的用户必须已包含在流规则中（如 `from:user1 OR from:user2`），否则流里根本不会有 ta 的推文。流规则可以直接在插件的 WebUI 页面中可视化增删，也可以在 X 开发者控制台配置。

## 配置

安装插件后，在 **AstrBot 管理面板 → 插件 → X 推文转发 → 插件配置** 中填写：

| 配置项 | 说明 |
| --- | --- |
| `bearer_token` | **必填**。X 开发者控制台 App → Keys and tokens 页面生成的 OAuth 2.0 App-Only Bearer Token |
| `proxy` | 可选。无法直连 `api.x.com` 时填写 HTTP 代理，如 `http://127.0.0.1:7890` |
| `send_media` | 是否附带推文图片（视频发送封面图），默认开启 |
| `backfill_minutes` | 断线重连时回补最近 N 分钟错过的推文（0-5，需 Pro 及以上套餐），默认关闭 |
| `credit_cost_per_tweet` | 每接收 1 条 Post 的费用单价（按套餐定价填写），用于费用估算，默认 1 |
| `tweet_fields` 等 | 高级选项，自定义请求字段，一般无需修改 |

保存配置后插件会自动重载并建立连接。

## 使用

| 指令 | 权限 | 说明 |
| --- | --- | --- |
| `/xfwd sub <用户名> [用户名...]` | 所有人 | 为**当前会话**订阅 X 用户（@handle，不含 @），用户必须已在某条流规则的 `from:` 条件中；订阅 `*` 表示接收流中全部推文 |
| `/xfwd unsub <用户名> [用户名...]` | 所有人 | 取消当前会话对某些用户的订阅 |
| `/xfwd status` | 所有人 | 查看流连接状态、累计转发数和所有会话的订阅情况 |
| `/xfwd list` | 所有人 | 查看当前会话的订阅名单（标注已失效的订阅） |
| `/xfwd rules` | 所有人 | 查看 X 上配置的全部流规则（`GET /2/tweets/search/stream/rules`） |
| `/xfwd test` | 管理员 | 向所有有订阅的会话发送测试消息 |

新增/删除流规则等管理操作均在 WebUI 插件页面完成（需登录管理面板）。

订阅数据保存在 `data/plugin_data/astrbot_plugin_X_forward/subscriptions.json`，重启不丢失。

### WebUI 订阅管理页

在 **WebUI → 插件管理 → X 推文转发 → 插件详情 → Pages → subscriptions** 中可以（需要支持插件 Pages 的 AstrBot 版本）：

- 查看流连接状态、X 上配置的全部流规则（规则表达式 / tag / ID）和各会话的订阅名单；
- **可视化构建并新增流规则**：按"来自用户 / 关键词 / 话题标签 / 提及 / 语言 / 类型条件（排除转推、含媒体等）"分项填写，自动拼接成规则表达式（组间 AND、组内 OR），可在预览框手动微调后提交（`POST /2/tweets/search/stream/rules`）；
- 删除任意一条流规则；
- 添加 / 移除会话订阅：页面会列出当前可订阅的用户（规则中全部 `from:` 用户），添加时校验，规则删除后失效的订阅会标红提示；
- **费用统计**：最近 30 天每日费用柱状图 + 按规则 ID 的累计接收条数与估算费用表（含已删除规则）。每条到达的推文按命中的规则 ID 本地计数并持久化保存 90 天（`usage.json`），费用 = 条数 × `credit_cost_per_tweet`；
- 状态卡片显示**本月剩余额度**（`GET /2/usage/tweets` 的月度上限 − 已用，10 分钟缓存），`/xfwd status` 同样显示。注意 X API 未提供实时 credits 余额端点，此处以 Post 用量额度作为余额展示。

## 行为说明

- 流式连接每 20 秒收到一次 keep-alive，超过 40 秒无数据自动重连。
- 按官方建议对不同错误退避重连：网络错误线性退避（最长 16s）、HTTP 5xx 指数退避（最长 320s）、HTTP 429 限流指数退避（最长 15min）；401/403 认证失败时每 10 分钟重试一次，请检查 Token。
- 注意：流式接口对开发者套餐/credits 有要求（额度耗尽会返回 402 CreditsDepleted，插件将每 30 分钟重试一次）。
