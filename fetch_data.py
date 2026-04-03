#!/usr/bin/env python3
"""
fetch_data.py — 从 THS iFinD 批量查询基金数据，生成静态 JS 数据文件

用法:
    python3 fetch_data.py [--date YYYY-MM-DD]

默认报告期为当前日期（THS 自动取最近交易日数据）。
输出到同目录: data.js, classification.js, cls_history.js, nav_curves.js
"""

import subprocess, json, os, sys, math, argparse
from datetime import datetime, timedelta

OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))
BATCH_SIZE = 50

# 目标管理人关键词（与 index.html 的 COMP_POOLS 对应）
COMPANY_KEYWORDS = [
    '富国', '易方达', '中欧', '广发', '汇添富',
    '华夏', '景顺长城', '嘉实', '兴证全球', '永赢',
    '南方', '工银瑞信', '大成', '鹏华', '华安',
]

# 只保留权益类（股票型/混合型），过滤债券/货币/QDII纯债等
EQUITY_L1_KEYWORDS = ['股票', '混合']

# 基础指标（用 --date 查询）
BASE_INDICATORS = ";".join([
    "ths_fund_short_name_fund",           # 基金简称
    "ths_fs_short_name_fund",             # 管理人简称
    "ths_fund_manager_current_fund",      # 基金经理(现任)
    "ths_fund_scale_fund",                # 规模（元，需除以1e8换亿）
    "ths_invest_type_first_classi_fund",  # 一级分类
    "ths_invest_type_second_classi_fund", # 二级分类
    "ths_tzlxsjfl_fund",                  # 三级分类
    "ths_yeild_ytd_fund",                 # 今年以来收益率（--date 返回正确值）
    "ths_retracement_fund",               # 回撤（相对前期高点，%）
])

# 周期收益率指标：需要用 --params "start_date,end_date" 才能返回正确值
# (key, days_back, fund_field)
YIELD_PERIODS = [
    ("ths_yeild_1w_fund",  7,   "return_1w"),
    ("ths_yeild_1m_fund",  30,  "return_1m"),
    ("ths_yeild_3m_fund",  90,  "return_3m"),
    ("ths_yeild_6m_fund",  180, "return_6m"),
    ("ths_yeild_1y_fund",  365, "return_1y"),
]

# ============================================================
# 基金代码种子表（15家机构主力权益产品，约 380 只）
# 脚本会通过 THS 实际数据自动验证并过滤掉无效/非目标条目
# ============================================================
SEED_CODES = [
    # ===== 富国基金 =====
    "161005.OF",  # 富国天惠精选成长混合(LOF)
    "100062.OF",  # 富国高新技术产业股票
    "100061.OF",  # 富国天瑞强势地区精选
    "100020.OF",  # 富国天益价值
    "100076.OF",  # 富国改革动力
    "005267.OF",  # 富国科技创新A
    "006751.OF",  # 富国价值优势A
    "003711.OF",  # 富国成长优选A
    "006098.OF",  # 富国新动力A
    "002406.OF",  # 富国消费主题A
    "010213.OF",  # 富国均衡优选A
    "004975.OF",  # 富国优势精选A
    "007301.OF",  # 富国卓远价值
    "008712.OF",  # 富国成长精选A
    "010397.OF",  # 富国新兴产业A
    "100038.OF",  # 富国天博创新主题
    "001974.OF",  # 富国创业板指数增强A
    "006195.OF",  # 富国价值驱动A
    "002803.OF",  # 富国价值量化
    "015016.OF",  # 富国产业精选A
    "010223.OF",  # 富国新能源汽车主题A
    "005580.OF",  # 富国研究精选A
    "007762.OF",  # 富国中证ESG
    "001147.OF",  # 富国证券公司ETF联接A
    "100068.OF",  # 富国低碳新经济
    "002610.OF",  # 富国均衡成长A
    "004696.OF",  # 富国新经济A
    "008717.OF",  # 富国先进制造A
    "012554.OF",  # 富国医疗保健A
    "100022.OF",  # 富国大中华精选
    "016499.OF",  # 富国优化成长A
    "010565.OF",  # 富国互联科技A
    "004485.OF",  # 富国新能源A

    # ===== 易方达基金 =====
    "005827.OF",  # 易方达蓝筹精选混合
    "110022.OF",  # 易方达消费行业股票
    "110011.OF",  # 易方达中小盘混合
    "007119.OF",  # 易方达科技创新混合A
    "004744.OF",  # 易方达优质精选混合A
    "110006.OF",  # 易方达价值精选混合
    "009965.OF",  # 易方达竞争优势企业混合A
    "110025.OF",  # 易方达行业领先股票A
    "001856.OF",  # 易方达优质企业三年持有混合
    "006282.OF",  # 易方达新经济混合A
    "007935.OF",  # 易方达成长精选混合
    "110003.OF",  # 易方达上证50A
    "004856.OF",  # 易方达医疗行业股票A
    "159915.SZ",  # 易方达创业板ETF
    "001766.OF",  # 易方达科讯混合A
    "002939.OF",  # 易方达供给改革混合A
    "009316.OF",  # 易方达高质量严选三年持有混合A
    "006479.OF",  # 易方达平衡成长混合
    "009609.OF",  # 易方达研究精选股票A
    "110020.OF",  # 易方达科技先锋股票
    "004745.OF",  # 易方达优质精选混合C
    "007383.OF",  # 易方达优质甄选混合A
    "009236.OF",  # 易方达悦享A
    "001087.OF",  # 易方达新常态灵活配置A
    "110026.OF",  # 易方达科技创新混合
    "001942.OF",  # 易方达新丝路灵活配置混合

    # ===== 中欧基金 =====
    "166002.OF",  # 中欧趋势混合A
    "001938.OF",  # 中欧时代先锋股票发起式A
    "002263.OF",  # 中欧潜力价值灵活配置混合A
    "003095.OF",  # 中欧医疗健康混合A
    "006821.OF",  # 中欧远见混合A
    "007641.OF",  # 中欧成长优选混合A
    "007305.OF",  # 中欧科技创新混合A
    "001158.OF",  # 中欧价值发现混合A
    "005666.OF",  # 中欧芯科技混合A
    "006193.OF",  # 中欧消费主题混合A
    "001811.OF",  # 中欧电子信息产业沪港深A
    "004746.OF",  # 中欧互联网创新混合A
    "004446.OF",  # 中欧红利优享混合A
    "008714.OF",  # 中欧养老产业混合A
    "002549.OF",  # 中欧品质生活混合A
    "005928.OF",  # 中欧新蓝筹混合A
    "001079.OF",  # 中欧量化优选混合A
    "166005.OF",  # 中欧周期配置混合A
    "009968.OF",  # 中欧价值智选混合A
    "015293.OF",  # 中欧优质成长混合A
    "003483.OF",  # 中欧新生代消费混合A
    "004919.OF",  # 中欧惠利灵活配置A
    "008750.OF",  # 中欧成长领航混合A
    "011538.OF",  # 中欧数字经济混合A

    # ===== 广发基金 =====
    "270002.OF",  # 广发稳健增长混合A
    "270042.OF",  # 广发小盘成长混合A
    "000592.OF",  # 广发医疗保健股票A
    "002943.OF",  # 广发高端制造股票A
    "004997.OF",  # 广发多元新兴股票A
    "005263.OF",  # 广发价值领先混合A
    "006741.OF",  # 广发均衡价值混合A
    "001810.OF",  # 广发创新升级混合A
    "008294.OF",  # 广发研究精选混合A
    "002809.OF",  # 广发品质生活混合A
    "001048.OF",  # 广发价值成长混合A
    "003735.OF",  # 广发双擎升级混合A
    "007190.OF",  # 广发科技先锋混合A
    "001064.OF",  # 广发新经济混合A
    "009189.OF",  # 广发行业严选三年持有混合A
    "270007.OF",  # 广发聚优成长混合A
    "270028.OF",  # 广发中盘成长混合A
    "007460.OF",  # 广发鑫享灵活配置混合
    "004263.OF",  # 广发百发精选混合
    "011056.OF",  # 广发优势成长混合A
    "270049.OF",  # 广发资产配置混合
    "009895.OF",  # 广发瑞泰混合A
    "001128.OF",  # 广发稳恒回报混合A

    # ===== 汇添富基金 =====
    "519069.OF",  # 汇添富价值精选混合A
    "519704.OF",  # 汇添富蓝筹稳健混合
    "004276.OF",  # 汇添富价值精选混合A
    "004580.OF",  # 汇添富美好生活混合A
    "004270.OF",  # 汇添富社会责任混合A
    "005982.OF",  # 汇添富医疗服务混合A
    "007505.OF",  # 汇添富中盘价值精选混合A
    "006823.OF",  # 汇添富新经济灵活配置混合A
    "001895.OF",  # 汇添富全球消费混合
    "003567.OF",  # 汇添富医疗创新混合A
    "009441.OF",  # 汇添富臻选消费混合A
    "001966.OF",  # 汇添富中证价值混合
    "001594.OF",  # 汇添富消费升级混合A
    "001299.OF",  # 汇添富移动互联网混合A
    "007648.OF",  # 汇添富核心优势混合A
    "004813.OF",  # 汇添富研究精选混合A
    "008549.OF",  # 汇添富优势成长混合A
    "004519.OF",  # 汇添富未来趋势混合A
    "519068.OF",  # 汇添富均衡增长混合
    "519771.OF",  # 汇添富策略回报灵活配置混合
    "006624.OF",  # 汇添富新材料新能源混合A
    "009745.OF",  # 汇添富行业优选混合A
    "010965.OF",  # 汇添富中证新能源汽车联接A

    # ===== 华夏基金 =====
    "000001.OF",  # 华夏成长混合
    "001102.OF",  # 华夏新经济灵活配置混合A
    "004124.OF",  # 华夏行业景气混合A
    "005643.OF",  # 华夏科技成长混合A
    "007861.OF",  # 华夏产业升级混合A
    "002959.OF",  # 华夏兴和混合A
    "006852.OF",  # 华夏智胜价值成长混合A
    "001409.OF",  # 华夏大盘精选混合
    "001010.OF",  # 华夏回报二号混合A
    "510050.SH",  # 华夏上证50ETF
    "001410.OF",  # 华夏社会责任混合A
    "003971.OF",  # 华夏数字经济混合A
    "007868.OF",  # 华夏智胜先锋混合A
    "002482.OF",  # 华夏兴发灵活配置混合
    "000041.OF",  # 华夏大盘精选混合(LOF)
    "512880.SH",  # 华夏中证证券公司ETF
    "009253.OF",  # 华夏先锋混合A
    "010577.OF",  # 华夏质量回报三年持有混合A
    "004282.OF",  # 华夏行业优选混合A
    "003547.OF",  # 华夏经济转型股票A
    "009689.OF",  # 华夏兴融灵活配置混合A
    "007897.OF",  # 华夏周期驱动股票A
    "006008.OF",  # 华夏国证计算机行业ETF联接A

    # ===== 景顺长城基金 =====
    "260104.OF",  # 景顺长城内需增长混合A
    "260108.OF",  # 景顺长城新兴成长混合A
    "260112.OF",  # 景顺长城优质成长混合A
    "260116.OF",  # 景顺长城动力平衡混合A
    "000948.OF",  # 景顺长城沪港深核心优势混合
    "001975.OF",  # 景顺长城大数据100股票A
    "002230.OF",  # 景顺长城价值边际混合A
    "004836.OF",  # 景顺长城绩优成长混合A
    "006587.OF",  # 景顺长城创新成长混合A
    "008939.OF",  # 景顺长城价值与成长混合A
    "003765.OF",  # 景顺长城新能源产业股票A
    "005986.OF",  # 景顺长城ESG可持续竞争力股票A
    "009733.OF",  # 景顺长城价值先锋混合A
    "001316.OF",  # 景顺长城量化小盘股票A
    "000893.OF",  # 景顺长城内需增长贰号混合
    "260107.OF",  # 景顺长城资源垄断混合A
    "008550.OF",  # 景顺长城研究精选混合A
    "000697.OF",  # 景顺长城内需增长混合(LOF)
    "011100.OF",  # 景顺长城中国制造混合A
    "009893.OF",  # 景顺长城优选成长混合A
    "005228.OF",  # 景顺长城优质成长混合C

    # ===== 嘉实基金 =====
    "070002.OF",  # 嘉实增长混合
    "070011.OF",  # 嘉实稳健混合A
    "001579.OF",  # 嘉实新能源新材料股票A
    "001653.OF",  # 嘉实农业产业股票A
    "006252.OF",  # 嘉实核心成长混合A
    "007291.OF",  # 嘉实智能汽车混合A
    "003816.OF",  # 嘉实新兴产业混合
    "004709.OF",  # 嘉实价值精选混合
    "162003.OF",  # 嘉实沪深300ETF联接A
    "070018.OF",  # 嘉实主题新动力混合
    "001628.OF",  # 嘉实量化阿尔法混合
    "005910.OF",  # 嘉实科技创新混合A
    "001651.OF",  # 嘉实医药健康股票
    "009625.OF",  # 嘉实研究优选混合A
    "009387.OF",  # 嘉实远见成长混合A
    "003969.OF",  # 嘉实物流产业混合A
    "011227.OF",  # 嘉实成长增强混合A
    "070009.OF",  # 嘉实优化红利混合A
    "070022.OF",  # 嘉实泰和混合A
    "070041.OF",  # 嘉实价值增长混合
    "013310.OF",  # 嘉实消费精选混合A
    "015262.OF",  # 嘉实科技创新与成长混合A

    # ===== 兴证全球基金 =====
    "163402.OF",  # 兴全趋势投资混合(LOF)
    "340006.OF",  # 兴全有机增长混合
    "340007.OF",  # 兴全轻资产混合
    "000498.OF",  # 兴全商业模式优选混合A
    "001478.OF",  # 兴全合宜混合A
    "003987.OF",  # 兴全合润混合A
    "004246.OF",  # 兴全社会责任混合
    "163417.OF",  # 兴全绿色投资混合(LOF)
    "009778.OF",  # 兴证全球视野混合A
    "002984.OF",  # 兴证全球旗舰成长混合A
    "340001.OF",  # 兴全可转债混合
    "015350.OF",  # 兴全择优混合A
    "000713.OF",  # 兴全合润混合(LOF)
    "009997.OF",  # 兴证全球优选均衡混合A
    "010648.OF",  # 兴全睿泽混合A
    "012282.OF",  # 兴证全球成长优选混合A
    "007994.OF",  # 兴全多维价值混合A
    "010459.OF",  # 兴全优化进取混合A
    "008811.OF",  # 兴证全球量化优选混合A
    "001498.OF",  # 兴全小盘混合(LOF)
    "003263.OF",  # 兴证全球创新增长混合A

    # ===== 永赢基金 =====
    "007113.OF",  # 永赢高端制造混合A
    "008919.OF",  # 永赢科技驱动混合A
    "008920.OF",  # 永赢科技驱动混合C
    "010562.OF",  # 永赢成长领航混合A
    "010563.OF",  # 永赢成长领航混合C
    "011004.OF",  # 永赢鑫盛混合A
    "011093.OF",  # 永赢宏泽一年定开混合
    "014598.OF",  # 永赢合享混合发起式A
    "015079.OF",  # 永赢成长远航一年持有期混合A

    # ===== 南方基金 =====
    "202001.OF",  # 南方稳健成长混合A
    "202003.OF",  # 南方高增长混合
    "202019.OF",  # 南方优选价值混合
    "001052.OF",  # 南方文娱传媒混合A
    "001300.OF",  # 南方医药保健灵活配置混合A
    "004686.OF",  # 南方科技创新混合A
    "000563.OF",  # 南方中小盘成长混合A
    "002851.OF",  # 南方优选成长混合A
    "162202.OF",  # 南方中证500ETF联接A
    "004475.OF",  # 南方消费活力混合A
    "006139.OF",  # 南方科技主题混合A
    "007844.OF",  # 南方研究优选混合A
    "002400.OF",  # 南方价值精选混合A
    "005584.OF",  # 南方新能源主题混合A
    "009551.OF",  # 南方成长先锋混合A
    "010582.OF",  # 南方慧选成长混合A
    "009987.OF",  # 南方研究精选混合A
    "001156.OF",  # 南方新优享灵活配置混合A
    "202022.OF",  # 南方隆元产业混合A
    "001011.OF",  # 南方行业配置混合A
    "001348.OF",  # 南方健康中国混合A
    "008132.OF",  # 南方科创先锋混合A

    # ===== 工银瑞信基金 =====
    "485111.OF",  # 工银瑞信精选平衡混合A
    "485110.OF",  # 工银瑞信核心价值混合A
    "001714.OF",  # 工银瑞信文体娱乐混合A
    "002218.OF",  # 工银瑞信医疗保健行业混合A
    "004405.OF",  # 工银瑞信价值精选混合A
    "006006.OF",  # 工银瑞信科技创新混合A
    "007961.OF",  # 工银瑞信研究精选混合A
    "008977.OF",  # 工银瑞信数字经济混合A
    "001878.OF",  # 工银瑞信新金融灵活配置混合
    "003474.OF",  # 工银瑞信信息技术行业混合A
    "006752.OF",  # 工银瑞信新能源汽车混合A
    "000863.OF",  # 工银瑞信国家战略主题混合
    "009592.OF",  # 工银瑞信创新成长混合A
    "000991.OF",  # 工银瑞信战略新兴产业混合A
    "002790.OF",  # 工银瑞信医疗保健股票A
    "009892.OF",  # 工银瑞信优质成长混合A
    "008131.OF",  # 工银瑞信新能源汽车主题股票A
    "005259.OF",  # 工银瑞信创新动力混合A
    "011533.OF",  # 工银瑞信价值增长混合A
    "485122.OF",  # 工银瑞信大盘蓝筹股票A
    "004655.OF",  # 工银瑞信养老产业股票A
    "007173.OF",  # 工银瑞信科技金融混合A

    # ===== 大成基金 =====
    "090001.OF",  # 大成价值增长混合
    "090003.OF",  # 大成精选增值混合A
    "090017.OF",  # 大成新锐产业混合
    "001069.OF",  # 大成中小盘股票A
    "000900.OF",  # 大成新能源主题股票
    "006247.OF",  # 大成卓越企业混合A
    "004234.OF",  # 大成核心双动力混合A
    "009699.OF",  # 大成策略优化混合A
    "002124.OF",  # 大成高科技产业股票A
    "001603.OF",  # 大成股息红利混合
    "090014.OF",  # 大成消费主题混合A
    "007490.OF",  # 大成健康产业混合A
    "005289.OF",  # 大成国企改革混合A
    "003369.OF",  # 大成景气成长混合A
    "090020.OF",  # 大成优化收益混合
    "010592.OF",  # 大成长赢量化混合A
    "002174.OF",  # 大成富锐混合A
    "090002.OF",  # 大成精选混合
    "011066.OF",  # 大成高鑫股票A（56亿主力产品）
    "010178.OF",  # 大成企业能力驱动混合A
    "006038.OF",  # 大成景恒混合A
    "009069.OF",  # 大成睿鑫股票A
    "001144.OF",  # 大成互联网思维混合A
    "012045.OF",  # 大成医药健康股票A
    "012184.OF",  # 大成创新趋势混合A

    # ===== 鹏华基金 =====
    "206001.OF",  # 鹏华价值优势混合A
    "002221.OF",  # 鹏华新兴产业混合A
    "003104.OF",  # 鹏华优质治理混合A
    "001616.OF",  # 鹏华医疗保健股票A
    "005354.OF",  # 鹏华价值共赢混合A
    "006441.OF",  # 鹏华行业成长混合A
    "000847.OF",  # 鹏华竞争优势混合A
    "001982.OF",  # 鹏华中国50混合
    "007739.OF",  # 鹏华产业精选混合A
    "009995.OF",  # 鹏华优质成长混合A
    "013297.OF",  # 鹏华研究创新混合A
    "002215.OF",  # 鹏华领先成长混合A
    "005258.OF",  # 鹏华战略新兴混合A
    "008128.OF",  # 鹏华医药科技混合A
    "003269.OF",  # 鹏华创新驱动混合A
    "206014.OF",  # 鹏华现代农业股票
    "000862.OF",  # 鹏华A股通混合
    "004433.OF",  # 鹏华碳中和主题混合A
    "003241.OF",  # 鹏华弘泰混合A
    "009533.OF",  # 鹏华价值先锋混合A
    "008134.OF",  # 鹏华优选价值股票A（27.9亿）
    "005028.OF",  # 鹏华研究精选混合
    "001122.OF",  # 鹏华弘利混合A
    "001188.OF",  # 鹏华改革红利股票
    "003165.OF",  # 鹏华弘嘉混合A
    "007146.OF",  # 鹏华研究智选混合
    "009086.OF",  # 鹏华价值共赢两年持有期混合
    "012057.OF",  # 鹏华品质成长混合A
    "012093.OF",  # 鹏华创新升级混合A

    # ===== 华安基金 =====
    "040004.OF",  # 华安策略优选混合A
    "040021.OF",  # 华安核心成长混合A
    "001092.OF",  # 华安媒体互联网混合A
    "001694.OF",  # 华安智能生活混合A
    "003610.OF",  # 华安优质成长混合A
    "040033.OF",  # 华安宏利混合A
    "007825.OF",  # 华安行业轮换混合A
    "008187.OF",  # 华安汇丰科技成长混合A
    "006826.OF",  # 华安文体健康混合A
    "009561.OF",  # 华安研究精选混合A
    "004539.OF",  # 华安数字未来混合A
    "040035.OF",  # 华安新机遇混合A
    "010282.OF",  # 华安量化优选混合A
    "007993.OF",  # 华安成长创新混合A
    "001842.OF",  # 华安新丝路混合A
    "009265.OF",  # 华安科技创新混合A
    "001042.OF",  # 华安逆向策略混合A
    "040060.OF",  # 华安大盘精选混合A
    "040062.OF",  # 华安红利精选混合A
    "040020.OF",  # 华安国际配置混合A
]


# ============================================================
# 核心逻辑
# ============================================================

def get_default_date() -> str:
    d = datetime.today()
    # 往前找最近工作日
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d.strftime('%Y-%m-%d')


def _parse_ths_output(output: str) -> list:
    """从 ths-cli 输出中解析 tables 列表（去掉 daemon 日志行）"""
    json_lines = []
    in_json = False
    for line in output.strip().split('\n'):
        if line.startswith('{') or in_json:
            in_json = True
            json_lines.append(line)
    if not json_lines:
        return []
    data = json.loads('\n'.join(json_lines))
    if data.get('errorcode', 0) != 0:
        print(f"  [WARN] THS 错误 {data.get('errorcode')}: {data.get('errmsg')}", file=sys.stderr)
        return []
    return data.get('tables', [])


def query_batch(codes: list, date: str) -> list:
    """调用 ths-cli 批量查询基础指标（--date），返回 tables 列表"""
    codes_str = ",".join(codes)
    cmd = ["ths-cli", "query", codes_str, BASE_INDICATORS, "--date", date, "--json"]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=180)
        if result.returncode != 0:
            print(f"  [WARN] ths-cli 返回非零退出码: {result.stderr[:200]}", file=sys.stderr)
            return []
        return _parse_ths_output(result.stdout)
    except json.JSONDecodeError as e:
        print(f"  [ERROR] JSON 解析失败: {e}", file=sys.stderr)
        return []
    except Exception as e:
        print(f"  [ERROR] {e}", file=sys.stderr)
        return []


def query_batch_yield(codes: list, indicator: str, start_date: str, end_date: str) -> dict:
    """用 --params start,end 查询单个收益率指标，返回 {code: value} 映射"""
    codes_str = ",".join(codes)
    params_str = f"{start_date},{end_date}"
    cmd = ["ths-cli", "query", codes_str, indicator, "--params", params_str, "--json"]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=180)
        if result.returncode != 0:
            print(f"  [WARN] ths-cli 返回非零退出码: {result.stderr[:200]}", file=sys.stderr)
            return {}
        tables = _parse_ths_output(result.stdout)
        out = {}
        for tbl in tables:
            code = tbl.get('thscode', '')
            lst = tbl.get('table', {}).get(indicator, [])
            val = lst[0] if lst else None
            if code and val is not None:
                out[code] = val
        return out
    except json.JSONDecodeError as e:
        print(f"  [ERROR] JSON 解析失败: {e}", file=sys.stderr)
        return {}
    except Exception as e:
        print(f"  [ERROR] {e}", file=sys.stderr)
        return {}


def parse_float(v) -> float | None:
    if v is None or v == '' or v == '-':
        return None
    try:
        f = float(v)
        return None if math.isnan(f) or math.isinf(f) else f
    except (TypeError, ValueError):
        return None


def parse_table(table: dict) -> dict | None:
    """将 THS tables 中的一条记录转为 fund dict"""
    code = table.get('thscode', '')
    if not code:
        return None
    t = table.get('table', {})

    def get_val(key):
        lst = t.get(key, [])
        return lst[0] if lst else None

    short_name = get_val('ths_fund_short_name_fund')
    company    = get_val('ths_fs_short_name_fund')
    manager    = get_val('ths_fund_manager_current_fund')
    l1         = get_val('ths_invest_type_first_classi_fund')
    l2         = get_val('ths_invest_type_second_classi_fund')
    cls3       = get_val('ths_tzlxsjfl_fund')

    scale_raw  = parse_float(get_val('ths_fund_scale_fund'))
    scale      = round(scale_raw / 1e8, 2) if scale_raw else None

    return {
        'code':            code,
        'short_name':      short_name or code,
        'company':         company or '',
        'manager':         manager or '',
        'scale':           scale,
        'l1':              l1 or '',
        'l2':              l2 or '',
        'cls3':            cls3 or '',
        'return_1w':       None,   # 由 query_batch_yield 填入
        'return_1m':       None,
        'return_3m':       None,
        'return_6m':       None,
        'return_1y':       None,
        'return_ytd':      parse_float(get_val('ths_yeild_ytd_fund')),
        'max_drawdown_1y': parse_float(get_val('ths_retracement_fund')),
    }


def is_valid_fund(fund: dict) -> bool:
    """过滤规则：目标公司 + 权益类 + 规模>0"""
    company = fund.get('company', '')
    if not any(kw in company for kw in COMPANY_KEYWORDS):
        return False
    l1 = fund.get('l1', '')
    if not any(kw in l1 for kw in EQUITY_L1_KEYWORDS):
        return False
    scale = fund.get('scale')
    if not scale or scale <= 0:
        return False
    return True


def build_classification_data(funds: list) -> dict:
    """按 cls3 分组，计算各时段排名，生成 CLASSIFICATION_DATA"""
    periods = [
        ('return_1w',  '近1周'),
        ('return_1m',  '近1月'),
        ('return_3m',  '近3月'),
        ('return_6m',  '近6月'),
        ('return_1y',  '近1年'),
        ('return_ytd', '年初至今'),
    ]

    # 按 cls3 分组
    groups: dict[str, dict] = {}
    for f in funds:
        cls3 = f.get('cls3') or '其他'
        if cls3 not in groups:
            groups[cls3] = {
                'l1': f.get('l1', ''),
                'l2': f.get('l2', ''),
                'l3': cls3,
                'funds': [],
            }
        groups[cls3]['funds'].append(f)

    result = {}
    for cls3, grp in groups.items():
        if len(grp['funds']) < 3:
            continue
        periods_data = {}
        for pkey, pname in periods:
            valid = [(f['code'], f[pkey]) for f in grp['funds']
                     if f.get(pkey) is not None]
            if len(valid) < 3:
                continue
            # 按收益率从高到低排名（rank=1 最好）
            valid.sort(key=lambda x: x[1], reverse=True)
            total = len(valid)
            all_funds = []
            for rank, (code, ret) in enumerate(valid, 1):
                pct = round(rank / total * 100, 1)
                all_funds.append({
                    'code':  code,
                    'rank':  rank,
                    'ret':   round(ret, 4),
                    'pct':   pct,
                    'total': total,
                })
            periods_data[pname] = {
                'total':     total,
                'all_funds': all_funds,
            }
        if periods_data:
            result[cls3] = {
                'l1':      grp['l1'],
                'l2':      grp['l2'],
                'l3':      cls3,
                'periods': periods_data,
            }
    return result


def write_js(filename: str, var_name: str, data, header_comment: str = ''):
    path = os.path.join(OUTPUT_DIR, filename)
    with open(path, 'w', encoding='utf-8') as f:
        if header_comment:
            f.write(f'// {header_comment}\n')
        f.write(f'window.{var_name} = ')
        json.dump(data, f, ensure_ascii=False, separators=(',', ':'))
        f.write(';\n')
    size_kb = os.path.getsize(path) // 1024
    print(f"  → {filename} ({size_kb} KB)")


def main():
    parser = argparse.ArgumentParser(description='生成基金行业分析静态数据文件')
    parser.add_argument('--date', default=None,
                        help='报告期 YYYY-MM-DD（默认：最近工作日）')
    args = parser.parse_args()

    report_date = args.date or get_default_date()
    end_dt = datetime.strptime(report_date, '%Y-%m-%d')
    print(f"[fetch_data] 报告期: {report_date}")

    # 去重
    seed_codes = list(dict.fromkeys(SEED_CODES))
    print(f"[fetch_data] 种子代码: {len(seed_codes)} 只（去重后）")

    # ——— 阶段1：查询基础指标（--date）———
    funds_map: dict[str, dict] = {}
    total_batches = math.ceil(len(seed_codes) / BATCH_SIZE)
    print(f"[fetch_data] 阶段1: 基础指标查询（{total_batches} 批次）")
    for i in range(0, len(seed_codes), BATCH_SIZE):
        batch = seed_codes[i:i + BATCH_SIZE]
        batch_no = i // BATCH_SIZE + 1
        print(f"  批次 {batch_no}/{total_batches}: 查询 {len(batch)} 只...", end=' ', flush=True)
        tables = query_batch(batch, report_date)
        valid_in_batch = 0
        for tbl in tables:
            fund = parse_table(tbl)
            if fund and is_valid_fund(fund):
                funds_map[fund['code']] = fund
                valid_in_batch += 1
        print(f"有效 {valid_in_batch} 只")

    valid_codes = list(funds_map.keys())
    print(f"\n[fetch_data] 有效基金: {len(valid_codes)} 只，开始收益率查询...")

    # ——— 阶段2：查询各周期收益率（--params start,end）———
    print(f"[fetch_data] 阶段2: 周期收益率查询（{len(YIELD_PERIODS)} 个周期）")
    for indicator, days_back, field in YIELD_PERIODS:
        start_dt = end_dt - timedelta(days=days_back)
        start_str = start_dt.strftime('%Y-%m-%d')
        total_yield_batches = math.ceil(len(valid_codes) / BATCH_SIZE)
        print(f"  {field} ({start_str} → {report_date}): {total_yield_batches} 批次...", end=' ', flush=True)
        filled = 0
        for i in range(0, len(valid_codes), BATCH_SIZE):
            batch = valid_codes[i:i + BATCH_SIZE]
            yield_map = query_batch_yield(batch, indicator, start_str, report_date)
            for code, val in yield_map.items():
                if code in funds_map:
                    funds_map[code][field] = parse_float(val)
                    filled += 1
        print(f"填入 {filled} 条")

    all_funds = list(funds_map.values())

    print(f"\n[fetch_data] 有效基金总数: {len(all_funds)}")

    # 各公司统计
    company_count: dict[str, int] = {}
    for f in all_funds:
        c = f.get('company', '未知')
        company_count[c] = company_count.get(c, 0) + 1
    for c, n in sorted(company_count.items(), key=lambda x: -x[1]):
        print(f"    {c}: {n} 只")

    # ——— 构建分类数据 ———
    cls_data = build_classification_data(all_funds)
    print(f"\n[fetch_data] 三级分类数: {len(cls_data)}")
    for k, v in sorted(cls_data.items()):
        n = next(iter(v['periods'].values()), {}).get('total', 0) if v.get('periods') else 0
        print(f"    {k}: {n} 只")

    # ——— 生成内联数据块 ———
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    def js_block(var_name, data, comment=''):
        lines = []
        if comment:
            lines.append(f'// {comment}')
        lines.append(f'window.{var_name} = {json.dumps(data, ensure_ascii=False, separators=(",", ":"))};')
        return '\n'.join(lines)

    inline_script = '\n'.join([
        js_block('FUNDS_DATA', all_funds,
                 f'生成时间: {now_str}  报告期: {report_date}  共 {len(all_funds)} 只基金'),
        f"window.DATA_UPDATE_TIME = '{now_str}';",
        js_block('CLASSIFICATION_DATA', cls_data, f'报告期: {report_date}'),
        js_block('CLS_HISTORY', [], '历史快照（暂留空）'),
        js_block('NAV_CURVES', {}, '净值曲线（暂留空）'),
    ])

    # ——— 注入到 index.html，生成自包含的 dashboard.html ———
    template_path = os.path.join(OUTPUT_DIR, 'index.html')
    output_path   = os.path.join(OUTPUT_DIR, 'dashboard.html')

    with open(template_path, encoding='utf-8') as f:
        html = f.read()

    SCRIPT_BLOCK = (
        '<script src="data.js"></script>\n'
        '<script src="classification.js"></script>\n'
        '<script src="cls_history.js"></script>\n'
        '<script src="nav_curves.js"></script>'
    )
    injected = html.replace(SCRIPT_BLOCK, f'<script>\n{inline_script}\n</script>')

    if injected == html:
        print("[WARN] 未找到 <script src> 占位块，dashboard.html 可能无数据", file=sys.stderr)

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(injected)

    size_kb = os.path.getsize(output_path) // 1024
    print(f"\n[fetch_data] 写出 dashboard.html ({size_kb} KB)")
    print(f"\n[fetch_data] 完成！共 {len(all_funds)} 只基金，{len(cls_data)} 个分类。")
    print(f"  打开仪表盘: open {output_path}")
    print(f"  刷新数据:   python3 fetch_data.py [--date YYYY-MM-DD]")


if __name__ == '__main__':
    main()
