---
name: tujia-oversea-sql
version: 0.3.0
description: 途家海外项目 SQL 输出专用技能。用于生成、改写、校验海外民宿/海外酒店/C平台/T平台/Q平台、携程酒店/非标、城市经营、流量转化、订单 GMV/间夜/ADR、供给库存、房源动销、用户画像、复购、节假日复盘、城市优先级、单城市专项、销售运营和邮件监控 SQL。用户提到海外、境外、非大陆城市、C酒店、C非标、携程海外、海外房源、海外订单、海外流量时触发。
agents: [claude, codex]
author: jianyangz
source: vibe
---

# 途家海外 SQL 输出 Skill

## 目标

把用户的海外业务分析需求转成可执行、口径稳定、字段中文可读的 Hive/Spark SQL，并在输出 SQL 后给出简短口径说明和风险提示。

## 触发词

用户提到以下任一内容时使用本 skill：
- 海外、境外、非大陆城市、香港、澳门、海外民宿、海外酒店
- C平台、T平台、Q平台、携程海外、去哪儿海外
- C酒店、C非标、携程酒店、携程非标
- 海外城市经营、海外流量、海外订单、海外 GMV、海外间夜、海外供给、海外库存、海外动销
- 海外节假日复盘、五一、暑期、春节、城市优先级、单城市专项、新加坡间夜

## 内置参考

本 skill 已改为单文件迁移版，所有表清单、指标口径、SQL 模板、语料索引和维护边界都内置在本文件下方。使用时先按前半部分流程生成 SQL；遇到字段、指标或模板不确定时，在本文件的内置参考区查找。

## 执行硬规则

- 所有底层 SQL 执行必须走 `whale-smart` skill / Whale Query Open API。
- 禁止本机 `spark-sql`、`beeline`、Hive CLI、Dolphin SQL 执行器作为兜底。
- 禁止读取其他用户路径下的 Whale Query token；只能使用当前用户 `jianyangz` 自己路径下的 token。
- 找不到 token 时，让用户登录 Whale Query 自行复制粘贴，不跨用户翻找。
- 海外业务口径以 jianyangz 为准；与通用 SQL skill 冲突时，以本 skill 为准。

## 工作流程

1. 复述用户要的分析对象、时间窗口、维度和指标。
2. 判断任务类型：经营表现、流量转化、携程对比、供给库存、用户画像、节假日复盘、城市优先级、实验策略、销售运营。
3. 从本文件“内置参考：table_inventory.md”选底表，并明确订单、流量、房屋、用户是否要拆开聚合。
4. 从本文件“内置参考：metrics_and_dimensions.md”确认口径：离店/支付、GMV 字段、间夜字段、UV 粒度、C酒店/C非标定义、海外过滤。
5. 需要写 SQL 时，从本文件“内置参考：sql_patterns.md”选最接近模板，直接替换硬编码条件。
6. 输出 SQL 前做检查：分区、海外过滤、日期、关联键、distinct 粒度、金额单位、结尾无分号。
7. 输出 SQL 后补 3-6 条口径说明和风险提示。

## SQL 编写硬规则

- 只使用 `left join` 和 `inner join`；不要使用 `right join`、`full join`、`cross join`。
- SQL 末尾不要加分号。
- 严禁参数化 SQL：不要写 `params` CTE、`${date}`、`:date`、`{{date}}` 等占位符；日期、城市、平台等条件必须直接写在 `where` 里。
- 订单口径和流量口径不要混写；先分别聚合到同一维度，再关联计算转化率。
- 多日 UV 默认用 `count(distinct concat(dt,'|',uid))` 或 `count(distinct concat(d,'|',uid))`，不要直接跨日 `count(distinct uid)`，除非用户明确要去重自然人。
- 最终 `select` 默认输出中文别名；字段名不要带 `%`、`_`、`&`、`*` 等特殊符号，比例字段用“占比”“转化率”“比值”等中文。
- 金额、间夜、订单、UV 的分母必须用 `nullif(...,0)` 保护。

## 默认底表路由

| 需求 | 首选底表 | 默认过滤 / 关联 |
|---|---|---|
| 途家海外订单、GMV、间夜、ADR | `dws.dws_order` | `is_overseas = 1`、支付未取消、按 `checkout_date` 离店 |
| 途家海外 LDBO 流量、归因订单 | `dws.dws_path_ldbo_d` | `is_oversea = 1`、`wrapper_name in ('携程','途家','去哪儿')`、`source = '102'`、`user_type = '用户'` |
| 携程海外订单 | `app_ctrip.edw_htl_order_all_split` | `d = date_sub(current_date,1)`、`ordertype = 2`、`orderstatus in ('P','S')`、按 `departure` 离店 |
| 携程列表/宫格流量 | `app_ctrip.cdm_traf_ht_ctrip_list_qid_day` | 按 `d`，城市用 `excel_upload.dim_ctrip_list_qid_city` 映射 |
| C酒店/C非标拆分 | `app_ctrip.dimmasterhotel` | `is_standard = 1` 为 C酒店，`is_standard in (0,-1)` 为 C非标 |
| 途家房屋供给、直采/C接、单多居 | `dws.dws_house_d` | 最新分区，`house_is_oversea = 1`、在线房源 |
| 用户跨平台/画像 | `ods_tujia_member.third_user_mapping`、画像/LTV/ADR 表 | 先按用户去重，再关联订单或流量 |
| 日期同比/节假日 | `tujia_dim.date_week_of_year_yoy`、`tujia_dim.dim_date_info` | 先确认用户要自然日、离店日还是入住日 |

## 核心口径

### 途家订单

- 默认用 `dws.dws_order`。
- 海外过滤：`is_overseas = 1`。
- 支付未取消：`is_paysuccess_order = 1` 且 `nvl(is_cancel_order,0) = 0`。
- 离店口径：`to_date(checkout_date)`。
- 订单数：`count(distinct order_no)`。
- 用户数：`count(distinct user_id)` 或按场景使用 `uid`。
- 间夜：`sum(order_room_night_count)`。
- GMV：默认 `sum(room_total_amount)`；实收另行说明 `real_pay_amount`。

### 途家流量

- 默认用 `dws.dws_path_ldbo_d`。
- 海外过滤：`is_oversea = 1`。
- 主流程过滤：`wrapper_name in ('携程','途家','去哪儿')`、`cast(source as string) = '102'`、`user_type = '用户'`。
- 曝光：`count(1)` / `count(distinct concat(dt,'|',uid))`。
- 点击：`detail_uid is not null`。
- 归因订单：`without_risk_access_order_num`、`without_risk_access_order_room_night`、`without_risk_access_order_gmv`。
- 曝光价格：`final_price`，只统计大于 0 的价格。

### 携程订单

- 默认用 `app_ctrip.edw_htl_order_all_split`。
- 最新快照：`d = date_sub(current_date,1)`。
- 订单类型：`ordertype = 2`。
- 状态：`orderstatus in ('P','S')`；如用户要含取消再加 `C` 并说明。
- 海外过滤：`(country <> 1 or cityname in ('香港','澳门'))`。
- 离店口径：`to_date(departure)`。
- 订单数：`count(distinct orderid)`。
- 间夜：`sum(ciiquantity)`。
- GMV：`sum(ciireceivable)`。
- 默认不限制 `submitfrom` / `distributer`，除非用户明确要求。

### C酒店 / C非标

- 维表：`app_ctrip.dimmasterhotel`。
- 关联键：`cast(masterhotelid as bigint)`。
- `is_standard = 1`：C酒店。
- `is_standard in (0,-1)`：C非标。
- 订单拆分可 `inner join` 维表；流量拆分推荐 `left join` 后按类型聚合，避免丢未知流量。

### 供给与动销

- 默认用 `dws.dws_house_d` 最新快照。
- 海外在线：`house_is_oversea = 1` 且在线字段为 1。字段名可能是 `house_is_online` 或 `is_online`，以表元数据为准。
- 城市：优先 `house_city_name`。
- 房源：`count(distinct house_id)`。
- 门店：`count(distinct hotel_id)`。
- 直采：`landlord_channel_name = '平台商户'` 或 `landlord_channel = 1`。
- 动销：同窗口支付未取消成交房源数 / 最新在线房源数。

## 场景路由

| 场景 | 写法 |
|---|---|
| 城市经营宽表 | 途家流量、途家订单、携程流量、携程订单、房屋供给分别按城市聚合，再用城市全集 left join |
| 城市优先级 | 先确定收益指标和机会指标，常用近 30 天途家 GMV / 近 14 天空搜 LUV；输出分层和排序 |
| 单城市间夜专项 | 拆入住晚数、单均间夜、ADR、房型、单多居、渠道、新老客、长住潜力 |
| 携程 C酒店/C非标对比 | 订单和流量都关联 `dimmasterhotel`，分别输出酒店、非标和整体 |
| 用户画像 | 先用订单或流量圈用户池，再分年龄、设备、ADR、入住人数、晚数、浏览偏好；推断标签必须说明 |
| 节假日复盘 | 明确离店日/入住日/支付日，使用同比日期维表；订单和流量分开聚合 |
| Q 平台 | 只用 LDBO `wrapper_name='去哪儿'` 时可写；Q 独立底表默认标风险或注释 |

## 输出格式

生成 SQL 时按这个结构回答：

1. SQL 代码块。
2. 口径说明：底表、时间窗口、过滤条件、关联键。
3. 指标说明：订单、GMV、间夜、UV、转化率、价格。
4. 风险提示：未知字段、金额单位、C酒店/C非标覆盖、Q 端依赖、推断标签。

## SQL 自检清单

- 是否所有大表都有分区或日期过滤。
- 是否明确海外过滤：途家 `is_overseas/is_oversea/house_is_oversea = 1`，携程 `(country <> 1 or cityname in ('香港','澳门'))`。
- 是否区分订单离店日、支付日、入住日和流量曝光日。
- 是否先聚合再 join，避免订单和流量明细互相放大。
- 是否 `uid` 为空、0、unknown 时做过滤。
- 是否多日 UV 使用日维度拼接去重。
- 是否比例分母使用 `nullif(...,0)`。
- 是否 C酒店/C非标有 `dimmasterhotel` 最新分区。
- 是否 SQL 末尾没有分号。


---

# 内置参考：table_inventory.md

> 原来源：`途家海外梳理/references/table_inventory.md`

# 表清单

本清单来自 `途家代码合集/sql` 下海外 SQL 语料的高频表和代表性文件。优先使用高频稳定表，低频实验表只在对应策略场景使用。

## 一、途家订单与经营表现

| 表名 | 用途 | 常见分区/过滤 | 关键字段 | 典型场景 |
|---|---|---|---|---|
| `dws.dws_order` | 途家订单主表，海外离店/支付/预定口径核心表 | `checkout_date`、`create_date`、`is_overseas = 1`、`is_paysuccess_order = 1`、`is_cancel_order = 0` | `order_no`、`order_room_night_count`、`room_total_amount`、`real_pay_amount`、`city_name`、`country_name`、`house_id`、`user_id`、`uid`、`wrapper_name` | GMV、间夜、订单数、ADR、预定周期、新老客、城市复盘 |
| `dws.dws_order_day` | 按天订单聚合 | 日期分区、海外标识 | 订单、GMV、间夜聚合字段 | 邮件监控、周/月大盘 |
| `dwd.dwd_order_product_d` | 订单商品明细，处理入住日期、部分退款、房型商品等细粒度 | `d`、`checkin_date_new`、`checkout_date` | `order_no`、`product_id`、`house_id`、`checkin_date_new` | 房屋等级、实验效果、日粒度入住展开 |
| `dwd.dwd_order_d` | 订单 DWD 明细 | `d`、订单状态 | 订单状态、支付状态、渠道字段 | 订单校验、补充字段 |
| `tujia_dim.dim_order_channel` | 渠道维表 | 最新分区或全量 | 渠道编码、渠道名称 | 订单渠道映射 |

## 二、途家流量与 LDBO

| 表名 | 用途 | 常见分区/过滤 | 关键字段 | 典型场景 |
|---|---|---|---|---|
| `dws.dws_path_ldbo_d` | 途家 LDBO 主流程流量与归因订单核心表 | `dt`、`is_oversea = 1`、`wrapper_name in ('携程','途家','去哪儿')`、`source = '102'` | `uid`、`detail_uid`、`booking_uid`、`house_id`、`city_name`、`checkout_date`、`checkin_date`、`final_price`、`without_risk_access_order_num`、`without_risk_access_order_room_night`、`without_risk_access_order_gmv` | LPV/LUV/DPV/DUV、L2D、曝光价格、归因订单、流量转化、策略实验 |
| `ads.ads_abtest_user_key_uid` | AB 实验用户分桶 | 实验标识、日期 | `uid`、实验组字段 | 金特牌、低流高转、首尔 C接选房等实验 |
| `pdb_analysis_c.ads_flow_city_dynamic_loc_or_other_d` | 城市/商圈流量位置分析 | `d`、城市 | 城市、商圈、流量位置字段 | 流量错配、城市商圈机会 |
| `ads.ads_flow_tujia_redbook_recommend_d` | 推荐/内容流量 | `d` | 推荐曝光点击字段 | 特定推荐场景 |
| `dwd.dwd_flow_cpc_poi_click_d` | CPC POI 点击曝光 | `d`、城市 | `exposure_count`、点击字段 | CPC 加权、营销曝光 |

## 三、携程 C 平台订单与流量

| 表名 | 用途 | 常见分区/过滤 | 关键字段 | 典型场景 |
|---|---|---|---|---|
| `app_ctrip.edw_htl_order_all_split` | 携程酒店订单明细，C 端订单核心表 | `d = date_sub(current_date,1)`、`ordertype = 2`、`orderstatus in ('P','S')`、`to_date(departure)`、`country != 1 or cityname in ('香港','澳门')`；默认不限制 `submitfrom` / `distributer` | `orderid`、`masterhotel`/`masterhotelid`、`ciireceivable`、`ciiquantity`、`arrival`、`departure`、`uid`、`cityname`、`countryname` | 携程离店订单、GMV、间夜、城市转化、远期取消 |
| `app_ctrip.cdm_traf_ht_ctrip_list_qid_day` | 携程列表/宫格流量 | `d`、海外城市、价格过滤 | `uid`、`qid`、`m_city`、`masterhotelid`、`fh_price`、`is_has_click` | 携程 LPV/LUV/DUV、C酒店/C非标流量 |
| `app_ctrip.dimmasterhotel` | 携程母酒店维表，区分 C酒店/C非标 | `d = date_sub(current_date,1)`、`masterhotelid > 0`、全量拆分用 `is_standard in (1,0,-1)`；非标默认用 `is_standard in (0,-1)` | `masterhotelid`、`is_standard`、`countryname`、`cityname` | C酒店/C非标拆分、携程流量/订单关联 |
| `app_ctrip.v_edw_inpr_aa_ovs_ord_d` | 携程海外订单/入境游相关订单 | 按日期/国家城市过滤 | 订单、国家城市、间夜金额字段 | 入境游、携程海外大盘 |
| `excel_upload.dim_ctrip_list_qid_city` | 携程 qid 城市映射 | 无固定分区，先去重 | `m_city`、`cityname`、`countryname` | 携程流量城市映射 |
| `app_ctrip.edw_bnb_dna_user_label_all` | 携程用户标签/画像 | 最新分区或标签快照 | 用户标签、年龄、偏好 | 用户画像、复购相似用户 |

## 四、房屋、供给、库存、价格

| 表名 | 用途 | 常见分区/过滤 | 关键字段 | 典型场景 |
|---|---|---|---|---|
| `dws.dws_house_d` | 途家房屋主维表 | `d = date_sub(current_date,1)`、`house_is_oversea = 1`、`is_online = 1` | `house_id`、`hotel_id`、`country_name`、`house_city_name`、`house_type`、`bedroom_count`、`landlord_channel_name`、`dynamic_business`、`avaliable_count` | 房屋画像、直采/C接、单多居、库存、城市房源 |
| `pdb_analysis_else.dws_house_oversea_l0_v2_d` | 海外房屋 L0/L1 分类 | `d` | 房屋分层字段 | 海外供给层级 |
| `dwd.dwd_house_daily_price_d` | 房屋每日价格 | `d`、入住日期 | `house_id`、价格字段 | 曝光价格、价格对标 |
| `dim_tujiaproduct.unit_inventory_log` | 库存日志 | 日期、house/unit | 库存、可售字段 | 日历可售率、请求可售率 |
| `ods_houseimport_config.api_unit` | C接/API 房型配置 | 状态、最新数据 | `unit_id`、`hotel_id`、物理房型 | C接库存、携程接入房屋 |
| `ods_houseimport_config.api_hotel` | C接/API 酒店配置 | 状态、最新数据 | `hotel_id`、供应商字段 | C接酒店维度 |
| `dwd.dwd_house_gctrip_origin_hotel_info_d` | 携程接入酒店/房屋映射 | `d` | 途家房屋与携程酒店映射 | 携程接入房源表现 |
| `ods_distributionmanager.ctrip_pre_analyze_room` | 携程预解析房型 | 去重后使用 | `room_id`、物理房型 | C接房型、等级规则 |

## 五、房屋质量、等级、生态

| 表名 | 用途 | 常见分区/过滤 | 关键字段 | 典型场景 |
|---|---|---|---|---|
| `pdb_analysis_c.dwd_house_infor_score_oversea_d` | 海外房屋信息分，语料最高频质量表 | `d`、海外房屋 | 信息分、基础分、扩展 JSON | 海外等级、规则迭代、房屋质量 |
| `pdb_analysis_c.dwd_house_reward_score_oversea_d` | 房屋奖励分 | `d` | 奖励分、规则字段 | 等级线上版本、奖励分 |
| `pdb_analysis_c.dwd_landlord_credit_score_new_oversea_d` | 海外房东诚信/信用分 | `d` | `hotel_id`、`credit_score`、`integrity` | 诚信分、房屋等级 |
| `pdb_analysis_c.ads_house_score_rank_bottom_oversea_d` | 海外房屋等级/底表 | `d` | 综合分、排名 | 等级汇总、运营规则 |
| `pdb_analysis_b.dwd_house_label_1000487_d` | 房屋标签 | `d` | 标签字段 | 金特牌、优选、宝藏 |
| `pdb_analysis_b.dwd_house_label_1000488_d` | 房屋标签 | `d` | 标签字段 | 金特牌、优选、宝藏 |
| `excel_upload.overseas_house_quality13` | 手工质量/运营规则表 | 上传快照 | 房屋质量规则字段 | 等级规则、运营规则 |
| `excel_upload.oversea_city_level` | 海外城市等级 | 上传快照 | 城市、等级 | 城市分层、策略优先级 |

## 六、用户、会员、复购画像

| 表名 | 用途 | 常见分区/过滤 | 关键字段 | 典型场景 |
|---|---|---|---|---|
| `ods_tujia_member.third_user_mapping` | 三方用户映射，连接携程 uid 与途家用户 | 最新可用分区或全量去重 | `third_uid`、`uid`、`user_id`、平台字段 | 跨平台用户、携程/途家复购 |
| `pdb_analysis_c.ads_user_ltv_detail_d` | 用户 LTV/价值明细 | `d` | 用户价值、订单价值 | 用户价值分层 |
| `pdb_analysis_c.ads_flow_tj_uid_adr_d` | 途家用户 ADR | `d` | `uid`、ADR | 跨平台 ADR 画像 |
| `tujia_share.dw_alita_user_main_tujia` | 用户基础画像 | 最新分区 | 年龄、性别、用户属性 | 基础画像 |
| `tujia_tmp.member_d` | 会员临时/快照表 | `d` | 会员等级、用户字段 | 会员分析 |

## 七、营销、活动、补贴

| 表名 | 用途 | 常见分区/过滤 | 关键字段 | 典型场景 |
|---|---|---|---|---|
| `dwd.dwd_tns_salespromotion_activity_detail_d` | 活动参与明细 | `d`、活动时间 | 活动、房屋、优惠字段 | 早鸟、连住、参活表现 |
| `ads.ads_house_activity_categories_mapping` | 房屋活动类目映射 | `d` | 活动类目、房屋 | 参活、活动底表 |
| `ods_tujiaonlinepromo.promo` | 优惠/红包/券 | 活动时间、状态 | 券、优惠字段 | 营销补贴券 |
| `ods_ploutos_fas.sub_account` | 资金/账户 | 账户状态 | 子账户、资金字段 | 补贴、结算辅助 |

## 八、码表与日期

| 表名 | 用途 | 常见分区/过滤 | 关键字段 | 典型场景 |
|---|---|---|---|---|
| `tujia_dim.date_week_of_year_yoy` | 同比日期映射 | 日期范围 | 当前日期、去年同期日期、周信息 | YOY 周期对齐 |
| `tujia_dim.dim_date_info` | 日期维表 | 日期范围 | 日期、周、月、节假日 | 节假日复盘 |
| `pdb_analysis_c.ads_flow_dim_date_info_d` | 流量侧日期维表 | `d` | 日期属性 | 流量监控 |
| `tujia_dim.dim_region` | 地域维表 | 最新分区 | 国家、城市、区域 | 城市映射 |
| `ods_tujia_pg.city` | 城市码表 | 最新/全量 | 城市 ID、城市名 | 城市补充 |
| `途家代码合集/sql/码表/城市码表.sql` | 本地城市码表示例 | 文件级参考 | 海外重点城市 | 城市列表和排序 |

## 九、Q 平台注意

项目中存在 `海外民宿/q平台数据/流量数据.sql` 和 `订单数据.sql`，但部分 Q 相关底表不在 Whale 上。生成新 SQL 时：
- 如果只使用 `dws.dws_path_ldbo_d` 的 `wrapper_name='去哪儿'`，可以保留。
- 如果要使用 Q 独立底表，先确认当前执行环境可访问；不能确认时默认注释 Q 段。

---

# 内置参考：metrics_and_dimensions.md

> 原来源：`途家海外梳理/references/metrics_and_dimensions.md`

# 指标口径与关键维度

## 一、订单类核心指标

| 指标 | 推荐 SQL 口径 | 说明 |
|---|---|---|
| 离店订单数 | `count(distinct order_no)` | 途家用 `dws.dws_order`；携程用 `count(distinct orderid)` |
| 支付订单数 | `count(distinct case when is_paysuccess_order = 1 then order_no end)` | 适用于支付口径，不等同离店口径 |
| GMV | `sum(room_total_amount)` 或 `sum(real_pay_amount)` | 途家复盘常用 `room_total_amount`；支付实收看 `real_pay_amount` |
| 携程 GMV | `sum(ciireceivable)` | Ctrip 订单表常用字段，单位需按源表确认 |
| 间夜 | `sum(order_room_night_count)` | 途家订单表常用字段 |
| 携程间夜 | `sum(ciiquantity)` | `app_ctrip.edw_htl_order_all_split` 无 `nights` 字段，`ciiquantity` 为间夜 |
| ADR | `sum(gmv) / nullif(sum(night),0)` | 房晚均价，注意 GMV 与间夜同口径 |
| 单均间夜 | `sum(night) / nullif(count(distinct order_no),0)` | 判断连住能力 |
| 取消订单数 | `count(distinct case when is_cancel_order = 1 then order_no end)` | 取消口径不要与未取消 GMV 混用 |
| 远期预定周期 | `datediff(to_date(checkin_date),to_date(create_date))` | 常分 T0、T1-7、T8-14、T15-30、T31+ |

## 二、流量与转化指标

| 指标 | 推荐 SQL 口径 | 说明 |
|---|---|---|
| LPV | `count(1)` 或 `count(uid)` | L 页曝光 PV，取决于源表是否一行一曝光 |
| LUV | `count(distinct concat(dt,'|',uid))` | 跨天 UV 要拼日期，避免多天去重被低估 |
| DPV | `count(case when detail_uid is not null then 1 end)` | D 页点击/详情 PV |
| DUV | `count(distinct case when detail_uid is not null then concat(dt,'|',uid) end)` | D 页点击 UV |
| L2D | `duv / nullif(luv,0)` | L 到 D 点击转化 |
| 归因订单 | `sum(without_risk_access_order_num)` | LDBO 风险剔除归因订单 |
| 归因间夜 | `sum(without_risk_access_order_room_night)` | LDBO 归因间夜 |
| 归因 GMV | `sum(without_risk_access_order_gmv)` | LDBO 归因 GMV |
| L2O | `sum(without_risk_access_order_num) / nullif(count(distinct concat(dt,'|',uid)),0)` | 流量到订单转化 |
| 每 LUV GMV | `sum(attr_gmv) / nullif(luv,0)` | 用户价值/流量质量 |
| 曝光价格 | 途家 `avg(final_price)`；携程 `avg(fh_price)` | 仅统计大于 0 的曝光价格；途家来自 `dws_path_ldbo_d`，携程来自 `cdm_traf_ht_ctrip_list_qid_day` |
| 下单用户平均间夜金额曝光价格比值 | `订单 ADR / 下单用户曝光均价` | 下单用户曝光均价需先按同城下单用户去重集合标记曝光行，避免订单明细 join 放大 |

## 三、房屋与供给指标

| 指标 | 推荐口径 | 说明 |
|---|---|---|
| 在线房屋数 | `count(distinct house_id)` with `is_online = 1` | 需加 `house_is_oversea = 1` |
| 有效库存 | `sum(avaliable_count)` | 字段拼写以源表 `avaliable_count` 为准 |
| 可售率 | `sum(可售天数) / nullif(sum(总天数),0)` | 日历和请求可售率口径不同，不能混用 |
| 单居/多居 | `case when bedroom_count = 1 then '单居' when bedroom_count >= 2 then '多居' else '未知居室' end` | 订单关联房屋维表后计算 |
| 直采/C接 | `case when landlord_channel_name = '平台商户' then '直采' else '接入' end` | 老 SQL 里也见 `landlord_channel`/`source_type` 变体 |
| 房屋类型 | `house_type` | 常见：标准酒店、青旅、其他类型、民宿等，以源表为准 |
| 商圈 | `dynamic_business` | 新加坡/日韩/泰国城市机会分析常用 |

## 四、用户与复购指标

| 指标 | 推荐口径 | 说明 |
|---|---|---|
| 新客/老客 | 当前窗口下单用户是否在历史窗口有过成功订单 | 必须说明历史窗口，例如去年同期、过去 180 天、T0/Tn |
| 复购用户 | 历史有订单且当前有订单的用户 | 可按城市、国家、平台拆历史订单地区 |
| 相似未下单用户 | 有浏览/曝光/画像相似，但当前窗口无订单 | 适合营销圈选，不等同自然转化 |
| 跨平台用户 | `third_user_mapping` 将 Ctrip uid 与途家 uid/user_id 打通 | 映射表需先去重，避免一对多放大 |
| 用户 ADR | 用户历史 GMV / 历史间夜 | 画像字段，注意跨平台与途家内口径差异 |
| 易转化/难转化 | 基于 `attr_order_per_luv`、`attr_night_per_luv`、`duv/luv` 打推断标签 | 这是分析推断，不是用户原生标签 |

## 五、C平台/C酒店/C非标口径

Ctrip 订单和流量要区分 C酒店/C非标时，统一依赖 `app_ctrip.dimmasterhotel`：

```sql
case when is_standard = 1 then 'C酒店'
     when is_standard in (0,-1) then 'C非标'
     else 'C未知' end as c_product_type
```

常见要求：
- 订单：`app_ctrip.edw_htl_order_all_split` 先按 `d`、`departure`、`ordertype = 2`、`orderstatus in ('P','S')`、海外国家城市收敛；默认不限制 `submitfrom` / `distributer`。
- 非标订单：默认 inner join `app_ctrip.dimmasterhotel`，`is_standard in (0,-1)`；全量拆分时 `is_standard = 1` 为 `C酒店`，`is_standard in (0,-1)` 为 `C非标`。
- 流量：`app_ctrip.cdm_traf_ht_ctrip_list_qid_day` 先按 `d`、城市、`fh_price > 0` 收敛，再关联城市码表和 `dimmasterhotel`。
- 不要把 Ctrip 酒店宫格独立流量与途家 LDBO 的 `wrapper_name='携程'` 混为同一来源；两者源表、曝光定义不同。

## 六、关键维度

| 维度 | 字段/写法 | 备注 |
|---|---|---|
| 平台 | `wrapper_name in ('携程','途家','去哪儿')` | LDBO 常用 C/Q/T 三端 |
| 来源 | `source = '102'` | 主流程海外常见过滤；字段有字符串/数字写法差异，建议统一 cast |
| 国家 | `country_name`、`countryname` | 途家与携程字段名不同 |
| 城市 | `city_name`、`house_city_name`、`cityname` | 订单、房屋、携程流量字段名不同 |
| 海外标识 | `is_overseas = 1`、`is_oversea = 1`、`house_is_oversea = 1` | 表不同字段不同 |
| 日期 | `checkout_date`、`create_date`、`dt`、`d`、`departure` | 先明确离店/创建/流量日期 |
| 房屋 | `house_id`、`hotel_id`、`masterhotelid` | 途家房屋、门店、携程母酒店不要混 |
| 居室 | `bedroom_count` | 1 居/2 居以上/未知 |
| 商户类型 | `landlord_channel_name`、`landlord_channel`、`source_type` | 直采/C接核心维度 |
| 商圈 | `dynamic_business` | 城市内经营机会 |
| 用户 | `uid`、`user_id`、`booking_uid`、`third_uid` | 浏览用户、下单用户、跨平台用户需分清 |
| 实验 | `ads.ads_abtest_user_key_uid` 的实验组字段 | AB 复盘先收敛分桶再关联流量/订单 |

## 七、日期窗口建议

| 场景 | 日期口径 | 建议 |
|---|---|---|
| 五一/节假日复盘 | 离店日期为节假日窗口 | 同比要用实际对应假期窗口，不要只简单减 365 天 |
| 近 30 天经营 | `date_sub(current_date,30)` 到 `date_sub(current_date,1)` | 当天数据通常不完整 |
| 流量监控 | `dt` 或 `d` | 与订单离店日期分开聚合 |
| 预定周期 | 订单创建日到入住日 | 输出分桶和均值两个视角 |
| 复购 | 历史窗口 + 当前窗口 | 必须写清历史窗口是否含去年同期/过去 180 天/过去 365 天 |

---

# 内置参考：sql_patterns.md

> 原来源：`途家海外梳理/references/sql_patterns.md`

# SQL 编写模板与规范

## 一、通用骨架

要求：所有底表先收敛 where，再关联；日期、城市、平台等条件直接固定写进 where，不写任何参数占位；只用 `left join` / `inner join`；SQL 末尾不加分号。

```sql
with source_a as (
    select
        key_id
        ,metric_a
    from some_db.some_table a
    where a.d between '2026-05-01' and '2026-05-05'
      and a.city_name = '新加坡'
)
,source_b as (
    select
        key_id
        ,dim_b
    from some_db.some_dim b
    where b.d = date_sub(current_date,1)
)
select
    a.key_id
    ,b.dim_b
    ,sum(a.metric_a) as metric_a
from source_a a
left join source_b b
  on a.key_id = b.key_id
group by a.key_id, b.dim_b
```

## 二、途家海外订单基础模板

```sql
with order_base as (
    select
        order_no
        ,user_id
        ,uid
        ,house_id
        ,country_name
        ,city_name
        ,wrapper_name
        ,to_date(create_date) as create_date
        ,to_date(checkin_date) as checkin_date
        ,to_date(checkout_date) as checkout_date
        ,cast(order_room_night_count as double) as night
        ,cast(room_total_amount as double) as gmv
        ,case when datediff(to_date(checkin_date),to_date(create_date)) = 0 then 'T0'
              when datediff(to_date(checkin_date),to_date(create_date)) between 1 and 7 then 'T1-7'
              when datediff(to_date(checkin_date),to_date(create_date)) between 8 and 14 then 'T8-14'
              when datediff(to_date(checkin_date),to_date(create_date)) between 15 and 30 then 'T15-30'
              when datediff(to_date(checkin_date),to_date(create_date)) >= 31 then 'T31+'
              else '未知' end as booking_window_bucket
    from dws.dws_order o
    where to_date(o.checkout_date) between '2026-05-01' and '2026-05-05'
      and o.is_overseas = 1
      and o.is_paysuccess_order = 1
      and nvl(o.is_cancel_order,0) = 0
)
select
    city_name
    ,wrapper_name
    ,count(distinct order_no) as order_cnt
    ,sum(night) as night
    ,round(sum(gmv),2) as gmv
    ,round(sum(gmv) / nullif(sum(night),0),2) as adr
    ,round(sum(night) / nullif(count(distinct order_no),0),2) as avg_night_per_order
from order_base
group by city_name, wrapper_name
```

## 三、途家 LDBO 流量基础模板

```sql
with ldbo_base as (
    select
        dt
        ,uid
        ,detail_uid
        ,booking_uid
        ,house_id
        ,city_name
        ,wrapper_name
        ,cast(without_risk_access_order_num as double) as attr_order_cnt
        ,cast(without_risk_access_order_room_night as double) as attr_night
        ,cast(without_risk_access_order_gmv as double) as attr_gmv
    from dws.dws_path_ldbo_d a
    where a.dt between '2026-05-01' and '2026-05-05'
      and a.is_oversea = 1
      and a.wrapper_name in ('携程','途家','去哪儿')
      and cast(a.source as string) = '102'
)
select
    city_name
    ,wrapper_name
    ,count(1) as lpv
    ,count(distinct concat(dt,'|',uid)) as luv
    ,count(case when detail_uid is not null then 1 end) as dpv
    ,count(distinct case when detail_uid is not null then concat(dt,'|',uid) end) as duv
    ,sum(attr_order_cnt) as attr_order_cnt
    ,sum(attr_night) as attr_night
    ,round(sum(attr_gmv),2) as attr_gmv
    ,round(count(distinct case when detail_uid is not null then concat(dt,'|',uid) end) / nullif(count(distinct concat(dt,'|',uid)),0),4) as l2d
    ,round(sum(attr_order_cnt) / nullif(count(distinct concat(dt,'|',uid)),0),4) as l2o
from ldbo_base
group by city_name, wrapper_name
```

## 四、Ctrip 订单按 C酒店/C非标拆分

```sql
with ctrip_hotel_dim as (
    select
        masterhotelid
        ,case when is_standard = 1 then 'C酒店'
              when is_standard in (0,-1) then 'C非标'
              else 'C未知' end as c_product_type
        ,countryname
        ,cityname
    from app_ctrip.dimmasterhotel h
    where h.d = date_sub(current_date,1)
      and h.masterhotelid > 0
      and h.is_standard in (1,0,-1)
)
,ctrip_order_base as (
    select
        o.orderid
        ,o.uid
        ,cast(o.masterhotel as bigint) as masterhotelid
        ,to_date(o.arrival) as checkin_date
        ,to_date(o.departure) as checkout_date
        ,o.countryname
        ,o.cityname
        ,cast(o.ciireceivable as double) as gmv
        ,cast(o.ciiquantity as double) as night
    from app_ctrip.edw_htl_order_all_split o
    where o.d = date_sub(current_date,1)
      and to_date(o.departure) between '2026-05-01' and '2026-05-05'
      and o.ordertype = 2
      and o.orderstatus in ('P','S')
      and (o.country != 1 or o.cityname in ('香港','澳门'))
)
select
    nvl(h.c_product_type,'C未知') as c_product_type
    ,nvl(h.countryname,o.countryname) as country_name
    ,nvl(h.cityname,o.cityname) as city_name
    ,count(distinct o.orderid) as order_cnt
    ,sum(o.night) as night
    ,round(sum(o.gmv),2) as gmv
    ,round(sum(o.gmv) / nullif(sum(o.night),0),2) as adr
from ctrip_order_base o
left join ctrip_hotel_dim h
  on o.masterhotelid = h.masterhotelid
group by nvl(h.c_product_type,'C未知'), nvl(h.countryname,o.countryname), nvl(h.cityname,o.cityname)
```

### Ctrip 非标订单默认模板

```sql
with ctrip_nonstd_hotel_dim as (
    select
        masterhotelid
        ,'C非标' as c_product_type
    from app_ctrip.dimmasterhotel h
    where h.d = date_sub(current_date,1)
      and h.masterhotelid > 0
      and h.is_standard in (0,-1)
)
select
    o.cityname as city_name
    ,count(distinct o.orderid) as order_cnt
    ,count(distinct o.uid) as user_cnt
    ,sum(cast(nvl(o.ciiquantity,0) as double)) as night
    ,sum(cast(nvl(o.ciireceivable,0) as double)) as gmv
from app_ctrip.edw_htl_order_all_split o
inner join ctrip_nonstd_hotel_dim h
  on o.masterhotelid = h.masterhotelid
where o.d = date_sub(current_date,1)
  and to_date(o.departure) between '2026-05-01' and '2026-05-05'
  and o.ordertype = 2
  and o.orderstatus in ('P','S')
  and (o.country != 1 or o.cityname in ('香港','澳门'))
group by o.cityname
```

## 五、Ctrip 流量按 C酒店/C非标拆分

```sql
with ctrip_city_dim as (
    select distinct
        m_city
        ,countryname
        ,cityname
    from excel_upload.dim_ctrip_list_qid_city
)
,ctrip_hotel_dim as (
    select
        masterhotelid
        ,case when is_standard = '1' then 'C酒店'
              when is_standard = '0' then 'C非标'
              else 'C未知' end as c_product_type
    from app_ctrip.dimmasterhotel h
    where h.d = date_sub(current_date,1)
      and h.masterhotelid > 0
      and h.is_standard != '-1'
)
,ctrip_flow_base as (
    select
        f.d as dt
        ,f.uid
        ,f.masterhotelid
        ,f.m_city
        ,f.is_has_click
    from app_ctrip.cdm_traf_ht_ctrip_list_qid_day f
    where f.d between '2026-05-01' and '2026-05-05'
      and f.fh_price > 0
)
select
    nvl(c.countryname,'未知国家') as country_name
    ,nvl(c.cityname,'未知城市') as city_name
    ,nvl(h.c_product_type,'C未知') as c_product_type
    ,count(1) as lpv
    ,count(distinct concat(f.dt,'|',f.uid)) as luv
    ,count(distinct case when f.is_has_click = 1 then concat(f.dt,'|',f.uid) end) as duv
from ctrip_flow_base f
left join ctrip_city_dim c
  on f.m_city = c.m_city
left join ctrip_hotel_dim h
  on f.masterhotelid = h.masterhotelid
group by nvl(c.countryname,'未知国家'), nvl(c.cityname,'未知城市'), nvl(h.c_product_type,'C未知')
```

## 六、房屋维表先收敛再关联

```sql
with house_dim as (
    select
        house_id
        ,hotel_id
        ,country_name
        ,house_city_name as city_name
        ,house_type
        ,dynamic_business
        ,case when bedroom_count = 1 then '单居'
              when bedroom_count >= 2 then '多居'
              else '未知居室' end as room_type_bucket
        ,case when landlord_channel_name = '平台商户' then '直采' else '接入' end as landlord_channel_type
    from dws.dws_house_d h
    where h.d = date_sub(current_date,1)
      and h.house_is_oversea = 1
)
select
    h.city_name
    ,h.room_type_bucket
    ,count(distinct h.house_id) as house_cnt
from house_dim h
group by h.city_name, h.room_type_bucket
```

## 七、新老客与去年地区回溯模板

```sql
with cur_user_order as (
    select
        user_id
        ,uid
        ,count(distinct order_no) as cur_order_cnt
        ,sum(order_room_night_count) as cur_night
        ,sum(room_total_amount) as cur_gmv
    from dws.dws_order o
    where to_date(o.checkout_date) between '2026-05-01' and '2026-05-05'
      and o.is_overseas = 1
      and o.is_paysuccess_order = 1
      and nvl(o.is_cancel_order,0) = 0
    group by user_id, uid
)
,last_user_region as (
    select
        user_id
        ,uid
        ,country_name as last_country_name
        ,city_name as last_city_name
        ,count(distinct order_no) as last_order_cnt
        ,sum(order_room_night_count) as last_night
    from dws.dws_order o
    where to_date(o.checkout_date) between '2025-05-01' and '2025-05-05'
      and o.is_overseas = 1
      and o.is_paysuccess_order = 1
      and nvl(o.is_cancel_order,0) = 0
    group by user_id, uid, country_name, city_name
)
select
    case when l.user_id is not null or l.uid is not null then '老客' else '新客' end as user_type
    ,nvl(l.last_country_name,'去年未预订') as last_country_name
    ,nvl(l.last_city_name,'去年未预订') as last_city_name
    ,count(distinct nvl(cast(c.user_id as string),c.uid)) as user_cnt
    ,sum(c.cur_order_cnt) as cur_order_cnt
    ,sum(c.cur_night) as cur_night
    ,round(sum(c.cur_gmv),2) as cur_gmv
from cur_user_order c
left join last_user_region l
  on nvl(cast(c.user_id as string),c.uid) = nvl(cast(l.user_id as string),l.uid)
group by case when l.user_id is not null or l.uid is not null then '老客' else '新客' end
    ,nvl(l.last_country_name,'去年未预订')
    ,nvl(l.last_city_name,'去年未预订')
```

## 八、SQL 改写检查清单

改完 SQL 后逐项检查：
- 是否没有 `params` CTE、参数注释、`${date}`、`${start_date}`、`:date`、`{{date}}` 等参数占位；日期是否已固定写进 `where` 条件。
- 每个底表 CTE 是否有分区/日期条件。
- 海外过滤是否使用了对应表正确字段：`is_overseas`、`is_oversea`、`house_is_oversea`。
- 关联前是否已经降粒度或去重，避免一对多放大。
- 是否只用了 `left join` / `inner join`。
- Q 端独立底表是否已确认可用，否则注释。
- 流量 UV 是否按 `concat(dt,'|',uid)` 计算。
- 曝光价格是否按途家 `final_price`、携程 `fh_price` 取大于 0 的均价；下单用户曝光价是否先用去重下单用户集合标记曝光行。
- 订单 GMV、间夜、订单数是否同一口径。
- Ctrip C酒店/C非标是否通过 `dimmasterhotel` 拆分。
- 业务交付 SQL 的最终输出字段是否使用中文别名，且字段名不带 `%`、`_`、`&`、`*` 等特殊符号。
- 末尾是否没有 `;`。

---

# 内置参考：source_sql_index.md

> 原来源：`途家海外梳理/references/source_sql_index.md`

# 源 SQL 索引

## 语料范围

- 来源目录：`途家代码合集/sql`
- SQL 文件数：161
- 主要目录：
  - `海外民宿/C平台数据`
  - `海外民宿/T平台数据`
  - `海外民宿/q平台数据`
  - `海外民宿/经营表现数据`
  - `海外民宿/邮件基建`
  - `海外民宿/营销活动`
  - `海外民宿/销售运营`
  - `House damn`
  - `码表`

## 高频表统计

| 频次 | 表名 | 说明 |
|---:|---|---|
| 155 | `dws.dws_house_d` | 房屋主维表，海外房屋/城市/直采C接/居室/库存基础 |
| 152 | `pdb_analysis_c.dwd_house_infor_score_oversea_d` | 海外房屋信息分和等级规则核心表 |
| 149 | `dws.dws_order` | 途家订单核心表 |
| 120 | `dws.dws_path_ldbo_d` | LDBO 流量和归因订单核心表 |
| 72 | `app_ctrip.edw_htl_order_all_split` | 携程酒店订单核心表 |
| 40 | `app_ctrip.dimmasterhotel` | 携程母酒店维表，C酒店/C非标拆分 |
| 32 | `excel_upload.overseas_house_quality13` | 海外房屋质量运营规则上传表 |
| 28 | `ods_houseimport_config.api_unit` | API/C接房型配置 |
| 24 | `ods_tujia_member.third_user_mapping` | 跨平台用户映射 |
| 22 | `tujia_dim.date_week_of_year_yoy` | YOY 日期映射 |
| 22 | `app_ctrip.v_edw_inpr_aa_ovs_ord_d` | 携程海外/入境订单相关 |
| 21 | `ods_tujiacustomer.comment` | 点评表现 |
| 20 | `app_ctrip.cdm_traf_ht_ctrip_list_qid_day` | 携程列表/宫格流量 |
| 18 | `pdb_analysis_b.dwd_house_label_1000487_d` | 房屋标签 |
| 18 | `ods_distributionmanager.ctrip_pre_analyze_room` | 携程预解析房型 |
| 17 | `excel_upload.houses_level_info0312v1` | 房屋等级规则上传表 |
| 16 | `pdb_analysis_b.dwd_house_label_1000488_d` | 房屋标签 |
| 16 | `excel_upload.overseasrm` | 海外运营上传表 |
| 14 | `excel_upload.oversea_city_level` | 海外城市等级 |
| 14 | `excel_upload.dim_ctrip_list_qid_city` | Ctrip qid 城市映射 |

## 代表性 SQL 文件

### C 平台与携程

| 文件 | 可复用能力 |
|---|---|
| `海外民宿/C平台数据/携程支付订单.sql` | 携程支付/订单基础取数 |
| `海外民宿/C平台数据/携程离店订单.sql` | 携程离店订单口径 |
| `海外民宿/C平台数据/携程曝光价格.sql` | 携程曝光价格与流量 |
| `海外民宿/C平台数据/海外by城市转化总览.sql` | Ctrip 城市流量与订单转化总览 |
| `海外民宿/C平台数据/空搜途家vs携程L页曝光价格对比.sql` | 途家 vs 携程曝光价格对比 |
| `海外民宿/C平台数据/新加坡携程酒店宫格与途家民宿交叉曝光用户.sql` | 携程酒店宫格与途家民宿交叉曝光 |
| `海外民宿/C平台数据/重点城市预定周期与单多居同比_近30天.sql` | 重点城市预定周期、单多居 YOY |
| `海外民宿/C平台数据/携程接入房屋/携程接入房屋经营表现.sql` | 携程接入房源经营表现 |

### T 平台大盘与客户

| 文件 | 可复用能力 |
|---|---|
| `海外民宿/T平台数据/大盘相关/by城市流量分布.sql` | 城市流量分布 |
| `海外民宿/T平台数据/大盘相关/by城市离店订单创建分布.sql` | 城市离店/创建订单分布 |
| `海外民宿/T平台数据/大盘相关/主流程转化国内海外近14天.sql` | 主流程国内海外转化对比 |
| `海外民宿/T平台数据/客户相关/用户行为/新老客.sql` | 新老客判断 |
| `海外民宿/T平台数据/客户相关/用户行为/用户预定周期.sql` | 预定周期 |
| `海外民宿/T平台数据/客户相关/用户行为/连住.sql` | 连住与间夜 |
| `海外民宿/T平台数据/客户相关/用户需求及意向价格/需求价格表现.sql` | 用户意向价格与曝光价格 |

### 房屋、库存、等级、生态

| 文件 | 可复用能力 |
|---|---|
| `海外民宿/T平台数据/房屋相关/库存/库存表现.sql` | 库存表现 |
| `海外民宿/T平台数据/房屋相关/库存/日历可售率.sql` | 日历可售率 |
| `海外民宿/T平台数据/房屋相关/库存/请求可售率.sql` | 请求可售率 |
| `海外民宿/T平台数据/房屋相关/房屋明细/房屋画像.sql` | 房屋画像 |
| `海外民宿/T平台数据/房屋相关/房屋明细/点评表现.sql` | 点评表现 |
| `海外民宿/T平台数据/房屋相关/房屋转化排名/房屋转化排名.sql` | 房屋转化排名 |
| `海外民宿/T平台数据/房屋相关/等级/等级线上版本/final_商户运营规则房屋等级分.sql` | 海外房屋等级规则 |
| `海外民宿/T平台数据/房屋相关/生态/优选/优选规则迭代.sql` | 优选规则迭代 |
| `海外民宿/T平台数据/房屋相关/生态/宝藏/宝藏房屋调度.sql` | 宝藏房屋调度 |

### 策略实验与流量机会

| 文件 | 可复用能力 |
|---|---|
| `海外民宿/T平台数据/海外策略及实验验证/低流高转实验/选房规则.sql` | 低流高转选房 |
| `海外民宿/T平台数据/海外策略及实验验证/流量错配项目-cr机会/东京流量错配.sql` | 城市流量错配与 CR 机会 |
| `海外民宿/T平台数据/海外策略及实验验证/流量错配项目-cr机会/价格流量加权线上.sql` | 价格流量加权 |
| `海外民宿/T平台数据/海外策略及实验验证/稀缺房加权/稀缺房加降权配置.sql` | 稀缺房加降权 |
| `海外民宿/T平台数据/海外策略及实验验证/金特牌实验/金特盘实验.sql` | 金特牌实验复盘 |
| `海外民宿/T平台数据/海外策略及实验验证/首尔c接选房策略/C接选房首尔复盘.sql` | 首尔 C接选房复盘 |

### 经营表现与邮件监控

| 文件 | 可复用能力 |
|---|---|
| `海外民宿/经营表现数据/五一表现/五一YOY经营业绩梳理v2.sql` | 五一 YOY 经营表现 |
| `海外民宿/经营表现数据/五一表现/五一YoY流量表现tab1.sql` | 五一流量表现 |
| `海外民宿/经营表现数据/假期复盘数据/26年春节数据.sql` | 春节复盘 |
| `海外民宿/经营表现数据/热门季节表现/枫叶季流量订单表现.sql` | 枫叶季流量订单表现 |
| `海外民宿/邮件基建/生态监控/海外流量监控重构-流量.sql` | 海外流量监控 |
| `海外民宿/邮件基建/生态监控/海外流量监控重构-转化.sql` | 海外转化监控 |
| `海外民宿/邮件基建/途家携程流量订单yoy/途家流量.sql` | 途家流量 YOY |
| `海外民宿/邮件基建/途家携程流量订单yoy/携程流量.sql` | 携程流量 YOY |
| `海外民宿/邮件基建/途家携程流量订单yoy/途家订单.sql` | 途家订单 YOY |
| `海外民宿/邮件基建/途家携程流量订单yoy/携程订单.sql` | 携程订单 YOY |

### 营销与销售运营

| 文件 | 可复用能力 |
|---|---|
| `海外民宿/营销活动/早鸟+连住 直采房屋参活表现.sql` | 早鸟、连住活动参活 |
| `海外民宿/营销活动/营销补贴券使用情况.sql` | 补贴券使用 |
| `海外民宿/营销活动/房屋调价及经营表现.sql` | 调价与经营表现 |
| `海外民宿/销售运营/绩效相关/GMV达成.sql` | 销售运营 GMV 达成 |
| `海外民宿/销售运营/绩效相关/上房数量.sql` | 上房数量与可订天数 |
| `海外民宿/销售运营/绩效相关/参活.sql` | 销售运营活动参与 |

## 语料共性

- 订单和流量通常分开写，再按城市、平台、房屋或用户维度关联。
- 海外过滤字段不统一：订单多为 `is_overseas`，流量多为 `is_oversea`，房屋多为 `house_is_oversea`。
- LDBO 主流程常见过滤：`wrapper_name in ('携程','途家','去哪儿')` 和 `source = '102'`。
- 携程订单通常要用最新快照分区 `d = date_sub(current_date,1)`，再用 `departure` 控制离店窗口。
- 携程 C酒店/C非标需要关联 `app_ctrip.dimmasterhotel` 的 `is_standard`。
- 多日 UV 不建议 `count(distinct uid)`，推荐 `count(distinct concat(dt,'|',uid))`。
- Q 独立底表存在非 Whale 风险，除 LDBO wrapper 口径外默认谨慎。

---

# 内置参考：maintenance.md

> 原来源：`途家海外梳理/references/maintenance.md`

# 维护与边界声明（M8）

## 维护人

- 海外项目第一负责人：**jianyangz**（无并行 owner）
- 海外业务相关 SQL 口径全部以 jianyangz 为准
- 任何海外口径变更需经 jianyangz 确认后再落库

## 依赖与重叠

- 本 skill 不依赖任何外部 skill 的海外口径
- 与通用 SQL skill（tujia-sql-base、tujia-flow-sql、tujia-main-flow-sql、tujia-pr-sql、tujia-commission 等）如有口径差异，**以本 skill 为准**
- 其他 skill 涉及海外业务（is_overseas / is_oversea / house_is_oversea = 1）时，应优先调用本 skill

## 变更日志

| 日期 | 版本 | 变更 | 备注 |
|---|---|---|---|
| 2026-05-13 | 0.2.0 | 注入 M0（whale-smart 强制执行） + 明确 M1 口径权威性 + 新增 M8 维护与边界 | jianyangz 对齐 |
| 2026-05-06 | 0.1.0 | 首版：基于途家代码合集 161 个海外 SQL 沉淀 SKILL.md + 4 个 reference（table_inventory、metrics_and_dimensions、sql_patterns、source_sql_index） | jianyangz |

## 待补清单（遇到再补，不预先编造）

- **M2 字段枚举**：`wrapper_name`、`source`、`landlord_channel_name`、`orderstatus`、`is_standard`、`house_type`、`ordertype`、`submitfrom` 的完整枚举值与含义
- **M3 金额单位**：`room_total_amount`、`real_pay_amount`、`ciireceivable` 等金额字段的单位（元 / 分）与货币口径
- **M4 易踩坑**：实战中遇到的坑按"问题 + 表现 + 正确写法"沉淀，每条具体到能避免一次错误
- **M6 项目特化模板**：新加坡间夜专项、城市优先级 ROI（近30天途家GMV / 近14天空搜LUV）、节假日 YOY 对齐
- **M7 分析场景索引**：节假日复盘、城市优先级、单城市间夜专项、五一/暑期/春节 YOY、用户画像分层等场景到模板的映射
