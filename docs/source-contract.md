# 来源资料约定

外部离线作业协议与来源范围资料，不包含数仓编排实现。

所有人员、机构、地址、业务事件及数值均为合成，不来自真实企业或个人。UTF-8 编码，日期为 ISO 8601，时间附明确时区。有效区间左闭右开，空结束值为尚无结束记录，不据此推断未来。金额使用整数分，非整数数量用十进制字符串；未知不是零。

周从周一开始，周需求读取该周及其开始日前28天日事实；完整性声明覆盖的业务窗口可以撤回。承诺参与 demand_week 的月覆盖依赖，承诺变化影响与其区间相交的需求周和下游月份。外部演练程序使用 python support/job_worker.py --request 请求路径 --directory 输出路径，JSON请求键固定为 run_token、job_id、scope、window、input_fingerprints，window含from_date/until_date。默认返回0且落盘 artifact.json 与 receipt.json；--mode fail-before 在写入前退出23；exit-after 在回执落盘后退出24。回执含运行授权、实际输入指纹、产物路径和摘要。演练程序只把请求内容写成产物，不承担生产变换、依赖传播或发布判断。验收其边界不等于编排已正确。

## deliveries

来源交付声明。

- delivery_id：投递
- source：来源
- town：乡镇
- from_date：开始
- until_date：结束
- fingerprint：内容标识
- ready：齐全声明
- received_at：收到

## jobs

读取规则是作业已有声明。

- job_id：作业
- inputs：上游作业或source来源
- lookback_days：向前读取天数
- output_grain：daily或weekly
- scope：town或county

## commitments

承诺的覆盖月份独立声明。

- commitment_id：承诺
- town：乡镇
- from_date：开始
- until_date：结束
- seats：服务位

## publication_groups

独立乡镇不被县级控制缺口阻挡。

- group_id：发布组
- scope：范围
- members：一致发布作业

## population

population来源的实际演练数据。

- record_id：来源行
- town：乡镇
- person_id：人员
- from_date：开始
- until_date：结束
- eligible：照护资格

## sites

site来源的实际资料。

- site_id：服务点
- town：乡镇
- from_date：开始
- until_date：结束
- seats：服务位

## service_rows

service来源的实际资料，原始来源不因资格不足而删除。

- service_id：服务
- town：乡镇
- person_id：人员
- site_id：服务点
- date：服务日

## changes

修订内容与齐全声明分开到达。C1修订P1在明确窗口内的资格，不改变窗口外事实；C2为该窗口资料重新齐全声明。

- change_id：变化
- source：来源
- scope：业务范围
- new_values：来源更正值
- received_at：收到

承诺月数据读取与月份相交的需求周及当月服务点可用范围；source:commitment来自commitments资料，分区完整性由承诺有效段声明。月粒度使用自然月，需求周跨月时同一周可以被两个实际读取它的月份引用，不能重复生成一份需求事实。来源fingerprint是交付标识，实际产物回执必须记录被读取资料的内容摘要。
