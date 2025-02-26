import akshare as ak
import yfinance as yf
import pandas as pd
import indicator_factory as indf
import sys
import Message as msg
from datetime import datetime, date, timedelta

settings = {
    "akshare": 
        {
            "futures_main_sina": ["ZC0","JM0","I0","CU0","P0","SR0","CF0","LH0"],
            "stock_zh_index_daily": ["sh000001","sz399006","sz399001"],
            "stock_zh_a_hist":["600482",
                                "688472",
                                "600377",
                                "002028",
                                "600160",
                                "300502",
                                "000630",
                                "000975",
                                "002422",
                                "688169",
                                "300394",
                                "600066",
                                "601127",
                                "688506",
                                "601136",
                                "002463",
                                "600415",
                                "300832",
                                "688082",
                                "300418",
                                "000807",
                                "600161",
                                "688009",
                                "001965",
                                "603296",
                                "300442",
                                "600026",
                                "600027",
                                "600372",
                                "600489",
                                "600023",
                                "000999",
                                "600515",
                                "301269",
                                "601059",
                                "688271",
                                "300308",
                                "601916",
                                "688041",
                                "688256",
                                "301269",
                                "601699",
                                "000617",
                                "000983",
                                "601607",
                                "600875",
                                "688223",
                                "601872",
                                "601689",
                                "600674",
                                "688303",
                                "600039",
                                "688187",
                                "600233",
                                "002180",
                                "605117",
                                "600803",
                                "601117",
                                "300979",
                                "002920",
                                "600188",
                                "000408",
                                "001289",
                                "600219",
                                "002648",
                                "002074",
                                "000792",
                                "300661",
                                "600941",
                                "600460",
                                "600089",
                                "688981",
                                "002466",
                                "601898",
                                "002709",
                                "605499",
                                "601865",
                                "300896",
                                "002459",
                                "601868",
                                "600905",
                                "300782",
                                "601728",
                                "300316",
                                "300759",
                                "300760",
                                "601868",
                                "002459",
                                "300750",
                                "002709",
                                "605499",
                                "688396",
                                "688126",
                                "688111",
                                "603806",
                                "603659",
                                "601995",
                                "601799",
                                "600426",
                                "300450",
                                "300274",
                                "000800",
                                "600600",
                                "688012",
                                "603195",
                                "002049",
                                "600150",
                                "603392",
                                "688036",
                                "600584",
                                "600845",
                                "002812",
                                "600918",
                                "688008",
                                "000977",
                                "601816",
                                "600745",
                                "601100",
                                "000708",
                                "003816",
                                "002129",
                                "002371",
                                "003816",
                                "300014",
                                "300628",
                                "601658",
                                "002916",
                                "300347",
                                "600183",
                                "600989",
                                "601236",
                                "601698",
                                "603501",
                                "000596",
                                "002938",
                                "300413",
                                "300498",
                                "601319",
                                "603019",
                                "603986",
                                "002179",
                                "000661",
                                "002001",
                                "601138",
                                "002311",
                                "600760",
                                "601066",
                                "002271",
                                "603259",
                                "300408",
                                "000786",
                                "601808",
                                "600176",
                                "600438",
                                "600809",
                                "601360",
                                "600025",
                                "600346",
                                "603288",
                                "601238",
                                "600346",
                                "603288",
                                "601238",
                                "601838",
                                "601012",
                                "002601",
                                "603799",
                                "600011",
                                "603833",
                                "002460",
                                "601878",
                                "300122",
                                "300015",
                                "002352",
                                "002555",
                                "600436",
                                "600919",
                                "600926",
                                "601229",
                                "601881",
                                "002714",
                                "300033",
                                "601877",
                                "000938",
                                "002027",
                                "600061",
                                "001979",
                                "601211",
                                "601985",
                                "002736",
                                "300059",
                                "600958",
                                "601021",
                                "601788",
                                "601919",
                                "000166",
                                "600570",
                                "300124",
                                "601225",
                                "002252",
                                "002475",
                                "600018",
                                "002230",
                                "000333",
                                "000963",
                                "600332",
                                "603993",
                                "002236",
                                "600886",
                                "601800",
                                "000725",
                                "002241",
                                "601336",
                                "601633",
                                "601669",
                                "601901",
                                "002594",
                                "601377",
                                "000776",
                                "002415",
                                "600115",
                                "600276",
                                "600406",
                                "600887",
                                "600893",
                                "601818",
                                "601288",
                                "002304",
                                "600999",
                                "601688",
                                "601888",
                                "601989",
                                "002007",
                                "601618",
                                "601668",
                                "601766",
                                "000100",
                                "600588",
                                "601186",
                                "601899",
                                "601390",
                                "601601",
                                "601939",
                                "000338",
                                "000895",
                                "002142",
                                "600111",
                                "601009",
                                "601169",
                                "601857",
                                "601088",
                                "600837",
                                "000876",
                                "601328",
                                "601998",
                                "601600",
                                "601318",
                                "601166",
                                "601628",
                                "600547",
                                "601111",
                                "600048",
                                "601398",
                                "601006",
                                "601988",
                                "000768",
                                "000538",
                                "600690",
                                "600660",
                                "600585",
                                "600519",
                                "600050",
                                "000858",
                                "000625",
                                "000651",
                                "000568",
                                "000425",
                                "000157",
                                "000063",
                                "000001",
                                "600009",
                                "600010",
                                "600015",
                                "600016",
                                "600019",
                                "600028",
                                "600029",
                                "600030",
                                "600031",
                                "600036",
                                "600050",
                                "000858",
                                "000625",
                                "000651",
                                "000568",
                                "000425",
                                "000157",
                                "000063",
                                "000001",
                                "000002"]
        },
    "yfinance": 
        {
            "BTC": ["BTC-USD","USDT-USD","ETH-USD"],
            "GOLD": ["GC=F","XAU=F","CL=F","NG=F"],
            "MONEY": ["DX=F","USDCNY=X","EURCNY=X","USDJPY=X","USDEUR=X","USDGBP=X","USDHKD=X","USDAUD=X","USDCAD=X"],
            "US_INDEX": ["^GSPC","^DJI","^IXIC","^SPX","^GDAXI","^N225","^FTSE","^HSI","^HSCE"],
            "FA_STOCK": ["AAPL","TSLA","NVDA","AMZN","INTC","MSFT","GOOGL","META"],
            "CN_STOCK": ["BIDU","BABA","0700.HK","PDD"]
        },
        "view_name": {
            "数字货币":
                {"BTC-USD": "比特币美元指数",
                 "USDT-USD": "USDT-USD",
                 "ETH-USD": "ETH-USD"},
            "大宗商品":
                {"GC=F": "纽约黄金",
                 "XAU=F": "伦敦黄金",
                 "CL=F": "纽约原油",
                 "NG=F": "美国天然气",
                 "S=F": "美国大豆",
                 "ZC0": "动力煤",
                 "JM0": "焦煤",
                 "I0": "铁矿石",
                 "CU0": "铜",
                 "P0": "棕榈油",
                 "SR0": "白糖",
                 "CF0": "棉花",
                 "LH0": "生猪"},
            "货币汇率":{
                "USDCNY=X": "美元兑人民币",
                "EURCNY=X": "欧元兑人民币",
                "USDJPY=X": "美元兑日元",
                "USDEUR=X": "美元兑欧元",
                "USDGBP=X": "美元兑英镑",
                "USDHKD=X": "美元兑港币",
                "USDAUD=X": "美元兑澳元",
                "USDCAD=X": "美元兑加拿大元",
                "DX=F": "美元指数"           
            },
            "全球指数":{
                "sh000001": "上证指数",
                "sz399001": "深证指数",
                "sz399006": "创业板指数",
                "^HSI": "恒生指数",
                "^HSCE": "恒生中国企业指数",
                "^DJI": "道琼斯",
                "^IXIC": "纳斯达克",
                "^SPX": "标普",
                "^GSPC": "GSPC",
                "^GDAXI": "德国DAX30",
                "^N225": "日经225",
                "^FTSE": "英国富时100"},
            "海外知名企业":{
                "AAPL": "APPLE",
                "TSLA": "TSLA",
                "NVDA": "NVDA",
                "AMZN": "AMZN",
                "INTC": "INTC",
                "MSFT": "MSFT",
                "GOOGL": "GOOGL",
                "META": "META",
                "BIDU": "BIDU",
                "BABA": "BABA",
                "0700.HK": "QQ",
                "PDD": "PDD"},
            "沪深300":{"600482": "中国动力",
                    "688472": "阿特斯",
                    "600377": "宁沪高速",
                    "002028": "思源电气",
                    "600160": "巨化股份",
                    "300502": "新易盛",
                    "000630": "铜陵有色",
                    "000975": "山金国际",
                    "002422": "科伦药业",
                    "688169": "石头科技",
                    "300394": "天孚通信",
                    "600066": "宇通客车",
                    "601127": "赛力斯",
                    "688506": "百利天恒",
                    "601136": "首创证券",
                    "002463": "沪电股份",
                    "600415": "小商品城",
                    "300832": "新产业",
                    "688082": "盛美上海",
                    "300418": "昆仑万维",
                    "000807": "云铝股份",
                    "600161": "天坛生物",
                    "688009": "中国通号",
                    "001965": "招商公路",
                    "603296": "华勤技术",
                    "300442": "润泽科技",
                    "600026": "中远海能",
                    "600027": "华电国际",
                    "600372": "中航机载",
                    "600489": "中金黄金",
                    "600023": "浙能电力",
                    "000999": "华润三九",
                    "600515": "海南机场",
                    "301269": "华大九天",
                    "601059": "信达证券",
                    "688271": "联影医疗",
                    "300308": "中际旭创",
                    "601916": "浙商银行",
                    "688041": "海光信息",
                    "688256": "寒武纪",
                    "601699": "潞安环能",
                    "000617": "中油资本",
                    "000983": "山西焦煤",
                    "601607": "上海医药",
                    "600875": "东方电气",
                    "688223": "晶科能源",
                    "601872": "招商轮船",
                    "601689": "拓普集团",
                    "600674": "川投能源",
                    "688303": "大全能源",
                    "600039": "四川路桥",
                    "688187": "时代电气",
                    "600233": "圆通速递",
                    "002180": "纳思达",
                    "605117": "德业股份",
                    "600803": "新奥股份",
                    "601117": "中国化学",
                    "300979": "华利集团",
                    "002920": "德赛西威",
                    "600188": "兖矿能源",
                    "000408": "藏格矿业",
                    "001289": "龙源电力",
                    "600219": "南山铝业",
                    "002648": "卫星化学",
                    "002074": "国轩高科",
                    "000792": "盐湖股份",
                    "300661": "圣邦股份",
                    "600941": "中国移动",
                    "600460": "士兰微",
                    "600089": "特变电工",
                    "688981": "中芯国际",
                    "002466": "天齐锂业",
                    "601898": "中煤能源",
                    "002709": "天赐材料",
                    "605499": "东鹏饮料",
                    "601865": "福莱特",
                    "300896": "爱美客",
                    "002459": "晶澳科技",
                    "601868": "中国能建",
                    "600905": "三峡能源",
                    "300782": "卓胜微",
                    "601728": "中国电信",
                    "300316": "晶盛机电",
                    "300759": "康龙化成",
                    "300760": "迈瑞医疗",
                    "300750": "宁德时代",
                    "688396": "华润微",
                    "688126": "沪硅产业",
                    "688111": "金山办公",
                    "603806": "福斯特",
                    "603659": "璞泰来",
                    "601995": "中金公司",
                    "601799": "星宇股份",
                    "600426": "华鲁恒升",
                    "300450": "先导智能",
                    "300274": "阳光电源",
                    "000800": "一汽解放",
                    "600600": "青岛啤酒",
                    "688012": "中微公司",
                    "603195": "公牛集团",
                    "002049": "紫光国微",
                    "600150": "中国船舶",
                    "603392": "万泰生物",
                    "688036": "传音控股",
                    "600584": "长电科技",
                    "600845": "宝信软件",
                    "002812": "恩捷股份",
                    "600918": "中泰证券",
                    "688008": "澜起科技",
                    "000977": "浪潮信息",
                    "601816": "京沪高铁",
                    "600745": "闻泰科技",
                    "601100": "恒立液压",
                    "000708": "中信特钢",
                    "003816": "中国广核",
                    "002129": "中环股份",
                    "002371": "北方华创",
                    "300014": "亿纬锂能",
                    "300628": "亿联网络",
                    "601658": "邮储银行",
                    "002916": "深南电路",
                    "300347": "泰格医药",
                    "600183": "生益科技",
                    "600989": "宝丰能源",
                    "601236": "红塔证券",
                    "601698": "中国卫通",
                    "603501": "韦尔股份",
                    "000596": "古井贡酒",
                    "002938": "鹏鼎控股",
                    "300413": "芒果超媒",
                    "300498": "温氏股份",
                    "601319": "中国人保",
                    "603019": "中科曙光",
                    "603986": "兆易创新",
                    "002179": "中航光电",
                    "000661": "长春高新",
                    "002001": "新和成",
                    "601138": "工业富联",
                    "002311": "海大集团",
                    "600760": "中航沈飞",
                    "601066": "中信建投",
                    "002271": "东方雨虹",
                    "603259": "药明康德",
                    "300408": "三环集团",
                    "000786": "北新建材",
                    "601808": "中海油服",
                    "600176": "中国巨石",
                    "600438": "通威股份",
                    "600809": "山西汾酒",
                    "601360": "三六零",
                    "600025": "华能水电",
                    "600346": "恒力股份",
                    "603288": "海天味业",
                    "601238": "广汽集团",
                    "601838": "成都银行",
                    "601012": "隆基股份",
                    "002601": "龙蟒佰利",
                    "603799": "华友钴业",
                    "600011": "华能国际",
                    "603833": "欧派家居",
                    "002460": "赣锋锂业",
                    "601878": "浙商证券",
                    "300122": "智飞生物",
                    "300015": "爱尔眼科",
                    "002352": "顺丰控股",
                    "002555": "三七互娱",
                    "600436": "片仔癀",
                    "600919": "江苏银行",
                    "600926": "杭州银行",
                    "601229": "上海银行",
                    "601881": "中国银河",
                    "002714": "牧原股份",
                    "300033": "同花顺",
                    "601877": "正泰电器",
                    "000938": "紫光股份",
                    "002027": "分众传媒",
                    "600061": "国投安信",
                    "001979": "招商蛇口",
                    "601211": "国泰君安",
                    "601985": "中国核电",
                    "002736": "国信证券",
                    "300059": "东方财富",
                    "600958": "东方证券",
                    "601021": "春秋航空",
                    "601788": "光大证券",
                    "601919": "中国远洋",
                    "000166": "申万宏源",
                    "600570": "恒生电子",
                    "300124": "汇川技术",
                    "601225": "陕西煤业",
                    "002252": "上海莱士",
                    "002475": "立讯精密",
                    "600018": "上港集团",
                    "002230": "科大讯飞",
                    "000333": "美的集团",
                    "000963": "华东医药",
                    "600332": "广州药业",
                    "603993": "洛阳钼业",
                    "002236": "大华股份",
                    "600886": "国投电力",
                    "601800": "中国交建",
                    "000725": "京东方A",
                    "002241": "歌尔声学",
                    "601336": "新华保险",
                    "601633": "长城汽车",
                    "601669": "中国水电",
                    "601901": "方正证券",
                    "002594": "比亚迪",
                    "601377": "兴业证券",
                    "000776": "广发证券",
                    "002415": "海康威视",
                    "600115": "东方航空",
                    "600276": "恒瑞医药",
                    "600406": "国电南瑞",
                    "600887": "伊利股份",
                    "600893": "航空动力",
                    "601818": "光大银行",
                    "601288": "农业银行",
                    "002304": "洋河股份",
                    "600999": "招商证券",
                    "601688": "华泰证券",
                    "601888": "中国国旅",
                    "601989": "中国重工",
                    "002007": "华兰生物",
                    "601618": "中国中冶",
                    "601668": "中国建筑",
                    "601766": "中国南车",
                    "000100": "TCL集团",
                    "600588": "用友软件",
                    "601186": "中国铁建",
                    "601899": "紫金矿业",
                    "601390": "中国中铁",
                    "601601": "中国太保",
                    "601939": "建设银行",
                    "000338": "潍柴动力",
                    "000895": "双汇发展",
                    "002142": "宁波银行",
                    "600111": "包钢稀土",
                    "601009": "南京银行",
                    "601169": "北京银行",
                    "601857": "中国石油",
                    "601088": "中国神华",
                    "600837": "海通证券",
                    "000876": "新希望",
                    "601328": "交通银行",
                    "601998": "中信银行",
                    "601600": "中国铝业",
                    "601318": "中国平安",
                    "601166": "兴业银行",
                    "601628": "中国人寿",
                    "600547": "山东黄金",
                    "601111": "中国国航",
                    "600048": "保利地产",
                    "601398": "工商银行",
                    "601006": "大秦铁路",
                    "601988": "中国银行",
                    "000768": "西飞国际",
                    "000538": "云南白药",
                    "600690": "青岛海尔",
                    "600660": "福耀玻璃",
                    "600585": "海螺水泥",
                    "600519": "贵州茅台",
                    "600050": "中国联通",
                    "000858": "五粮液",
                    "000625": "长安汽车",
                    "000651": "格力电器",
                    "000568": "泸州老窖",
                    "000425": "徐工科技",
                    "000157": "中联重科",
                    "000063": "中兴通讯",
                    "000001": "深发展A",
                    "600009": "上海机场",
                    "600010": "包钢股份",
                    "600015": "华夏银行",
                    "600016": "民生银行",
                    "600019": "宝钢股份",
                    "600028": "中国石化",
                    "600029": "南方航空",
                    "600030": "中信证券",
                    "600031": "三一重工",
                    "600036": "招商银行",
                    "000002": "万科A"}
            }
}


def Make_SINGLE(data):
    day_data = data.tail(20)
    day_data[['DAY_BOLL','DAY_UB','DAY_LB']] = indf.QA_indicator_BOLL(day_data)
    day_data = day_data.assign(PCT_CHANGE=day_data.close.pct_change())
    week_data = data['close'].resample('W').ohlc().tail(20)
    week_data[['WK_BOLL','WK_UB','WK_LB']] = indf.QA_indicator_BOLL(week_data)
    res = pd.DataFrame(pd.concat([day_data.iloc[-1],week_data.iloc[-1][['WK_BOLL','WK_UB','WK_LB']]])).T
    res = res.assign(DAY_RNG=res.DAY_UB/res.DAY_LB -1
                     , DAY_LB=res.close/res.DAY_LB-1
                     , DAY_UB=res.close/res.DAY_UB-1
                     , DAY_BOLL=res.close/res.DAY_BOLL-1
                     , WK_RNG=res.WK_UB/res.WK_LB -1
                     , WK_LB=res.close/res.WK_LB-1
                     , WK_UB=res.close/res.WK_UB-1
                     , WK_BOLL=res.close/res.WK_BOLL-1
                     )
    return (res)


def AK_INDEX_BOLL(symbol, start, end):
    df = ak.stock_zh_index_daily(symbol=symbol)
    df.rename(columns={'日期':'date','开盘价':'open','最高价':'high','最低价':'low','收盘价':'close','成交量':'volume','持仓量':'keep_volume','动态结算价':'price'}, inplace=True)
    df['date']=pd.to_datetime(df['date'])
    df.set_index('date', inplace=True)
    res = Make_SINGLE(df)
    res = res.assign(symbol={k1:v1 for k,v in settings.get('view_name').items() for k1, v1 in v.items()}[symbol])
    return res


def AK_FU_BOLL(symbol, start, end):
    df = ak.futures_main_sina(symbol=symbol, start_date=start.replace('-',''), end_date=end.replace('-',''))
    df.rename(columns={'日期':'date','开盘价':'open','最高价':'high','最低价':'low','收盘价':'close','成交量':'volume','持仓量':'keep_volume','动态结算价':'price'}, inplace=True)
    df['date']=pd.to_datetime(df['date'])
    df.set_index('date', inplace=True)
    res = Make_SINGLE(df)
    res = res.assign(symbol={k1:v1 for k,v in settings.get('view_name').items() for k1, v1 in v.items()}[symbol])
    return res


def AK_STOCK_BOLL(symbol, start, end):
    df = ak.stock_zh_a_hist(symbol=symbol, start_date=start.replace('-',''), end_date=end.replace('-',''),period="daily", adjust="qfq")
    df.rename(columns={'日期':'date','股票代码':'symbol',
                       '开盘价':'open','最高价':'high','最低价':'low','收盘价':'close','成交量':'volume',
                       '开盘':'open','最高':'high','最低':'low','收盘':'close',
                       '持仓量':'keep_volume','动态结算价':'price'}, inplace=True)
    df['date']=pd.to_datetime(df['date'])
    df.set_index('date', inplace=True)
    res = Make_SINGLE(df)
    res = res.assign(symbol={k1:v1 for k,v in settings.get('view_name').items() for k1, v1 in v.items()}[symbol])
    return res


def YF_BOLL(symbol, start, end):
    data = yf.download(symbol, start=start, end=end, actions=True, group_by="ticker")
    data.columns = pd.MultiIndex.from_tuples([(a, b.lower()) for (a,b) in data.columns])
    res_list = []
    for i in symbol:
        try:
            df = data.get(i).dropna()
            res = Make_SINGLE(df)
            res = res.assign(symbol={k1:v1 for k,v in settings.get('view_name').items() for k1, v1 in v.items()}[i])
            res_list.append(res)
        except Exception:
            print('failed:' + i)
    return pd.concat(res_list)[['symbol','close','PCT_CHANGE','DAY_RNG','DAY_LB','DAY_BOLL','DAY_UB','WK_RNG','WK_LB','WK_BOLL','WK_UB']]


def AK_REPORT(start, end):
    data_list = []
    for key, values in settings.get('akshare').items():

        if key == 'futures_main_sina':
            func = AK_FU_BOLL
        elif key == 'stock_zh_index_daily':
            func = AK_INDEX_BOLL
        elif key == 'stock_zh_a_hist':
            func = AK_STOCK_BOLL

        for value in values:
            try:
                print('akshare: ', value)
                data = func(value, start, end)[['symbol','close','PCT_CHANGE','DAY_RNG','DAY_LB','DAY_BOLL','DAY_UB','WK_RNG','WK_LB','WK_BOLL','WK_UB',]]
                data_list.append(data)
            except Exception:
                print('failed:' + key + ':' + value)
    return pd.concat(data_list)


def YF_REPORT(start, end):
    symbols_list = []
    for key, values in settings.get('yfinance').items():
        print('yfinance: ', values)
        symbols_list.extend(values)
    return YF_BOLL(symbols_list, start, end)


if __name__ == '__main__':
    start = (date.today() - timedelta(days=150)).strftime("%Y-%m-%d")
    end = date.today().strftime("%Y-%m-%d")
    smtpserver = sys.argv[1]
    smtpport = sys.argv[2]
    msg_from = sys.argv[3]
    msg_to = sys.argv[4]
    passwd = sys.argv[5]
    token = sys.argv[6]
    print('start: ', start, ' end: ', end)

    res1 = YF_REPORT(start, end)
    res2 = AK_REPORT(start, end)
    res = pd.concat([res1, res2])
    for title, values in settings.get('view_name').items():
        msg_title = f'{title} Market Report'
        data1 = res[res['symbol'].isin(list(values.values()))].sort_values(by='DAY_BOLL', ascending=True)
        data2 = res[res['symbol'].isin(list(values.values()))].sort_values(by='WK_BOLL', ascending=True)
        msg.send_email(msg_title, msg.build_email(msg.build_head(), 
                                            msg.build_table(data1, 'Day Summary'),
                                            msg.build_table(data2, 'Week Summary')), 
                    end, smtpserver, smtpport, msg_from, msg_to, passwd)
