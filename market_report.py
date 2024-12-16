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
            "stock_zh_index_daily": ["sh000001","sz399006","sz399001"]
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
        "view_name": 
        {
            "BTC-USD": "比特币美元指数",
            "USDT-USD": "USDT-USD",
            "ETH-USD": "ETH-USD",
            "GC=F": "纽约黄金",
            "XAU=F": "伦敦黄金",
            "CL=F": "纽约原油",
            "NG=F": "美国天然气",
            "S=F": "美国大豆",
            "USDCNY=X": "美元兑人民币",
            "EURCNY=X": "欧元兑人民币",
            "USDJPY=X": "美元兑日元",
            "USDEUR=X": "美元兑欧元",
            "USDGBP=X": "美元兑英镑",
            "USDHKD=X": "美元兑港币",
            "USDAUD=X": "美元兑澳元",
            "USDCAD=X": "美元兑加拿大元",
            "DX=F": "美元指数",
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
            "^FTSE": "英国富时100",
            "ZC0": "动力煤",
            "JM0": "焦煤",
            "I0": "铁矿石",
            "CU0": "铜",
            "P0": "棕榈油",
            "SR0": "白糖",
            "CF0": "棉花",
            "LH0": "生猪",
            "AAPL": "APPLE","TSLA": "TSLA","NVDA": "NVDA","AMZN": "AMZN","INTC": "INTC","MSFT": "MSFT","GOOGL": "GOOGL","META": "META",
            "BIDU": "BIDU","BABA": "BABA","0700.HK": "QQ","PDD": "PDD"
        }
}

def AK_BOLL(symbol, func=ak.stock_zh_index_daily):
    df = func(symbol=symbol)
    df.rename(columns={'日期':'date','开盘价':'open','最高价':'high','最低价':'low','收盘价':'close','成交量':'volume','持仓量':'keep_volume','动态结算价':'price'}, inplace=True)
    df['date']=pd.to_datetime(df['date'])
    df.set_index('date', inplace=True)
    day_data = df.tail(20)
    day_data[['DAY_BOLL','DAY_UB','DAY_LB']] = indf.QA_indicator_BOLL(day_data)
    week_data = df['close'].resample('W').ohlc().tail(20)
    week_data[['WK_BOLL','WK_UB','WK_LB']] = indf.QA_indicator_BOLL(week_data)
    res = pd.DataFrame(pd.concat([day_data.iloc[-1],week_data.iloc[-1][['WK_BOLL','WK_UB','WK_LB']]])).T
    res = res.assign(symbol=settings.get('view_name')[symbol]
                     , DAY_LB=res.close/res.DAY_LB-1
                     , DAY_UB=res.close/res.DAY_UB-1
                     , DAY_BOLL=res.close/res.DAY_BOLL-1
                     , WK_LB=res.close/res.WK_LB-1
                     , WK_UB=res.close/res.WK_UB-1
                     , WK_BOLL=res.close/res.WK_BOLL-1)
    return res

def YF_BOLL(symbol, start, end):
    data = yf.download(symbol, start=start, end=end, actions=True, group_by="ticker")
    data.columns = pd.MultiIndex.from_tuples([(a, b.lower()) for (a,b) in data.columns])
    res_list = []
    for i in symbol:
        try:
            df = data.get(i).dropna()
            day_data = df.tail(20)
            day_data[['DAY_BOLL','DAY_UB','DAY_LB']] = indf.QA_indicator_BOLL(day_data)
            week_data = df['close'].resample('W').ohlc().tail(20)
            week_data[['WK_BOLL','WK_UB','WK_LB']] = indf.QA_indicator_BOLL(week_data)
            res = pd.DataFrame(pd.concat([day_data.iloc[-1],week_data.iloc[-1][['WK_BOLL','WK_UB','WK_LB']]])).T
            res = res.assign(symbol=settings.get('view_name')[i]
                            , DAY_LB=res.close/res.DAY_LB-1
                            , DAY_UB=res.close/res.DAY_UB-1
                            , DAY_BOLL=res.close/res.DAY_BOLL-1
                            , WK_LB=res.close/res.WK_LB-1
                            , WK_UB=res.close/res.WK_UB-1
                            , WK_BOLL=res.close/res.WK_BOLL-1)
            res_list.append(res)
        except Exception:
            pass
    return pd.concat(res_list)[['symbol','close','DAY_LB','DAY_BOLL','DAY_UB','WK_LB','WK_BOLL','WK_UB']]


def AK_REPORT():
    data_list = []
    for key, values in settings.get('akshare').items():
        try:
            if key == 'futures_main_sina':
                func = ak.futures_main_sina
            elif key == 'stock_zh_index_daily':
                func = ak.stock_zh_index_daily
            for value in values:
                print('akshare: ',value)
                data = AK_BOLL(value, func)
                data_list.append(data)
        except Exception:
            pass
    return pd.concat(data_list)[['symbol','close','DAY_LB','DAY_BOLL','DAY_UB','WK_LB','WK_BOLL','WK_UB',]]


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
    res2 = AK_REPORT()
    res = pd.concat([res1, res2])
    msg.send_email('Financial Market Report', msg.build_email(msg.build_head(), 
                                           msg.build_table(res, 'Summary')), 
                   end, smtpserver, smtpport, msg_from, msg_to, passwd)
