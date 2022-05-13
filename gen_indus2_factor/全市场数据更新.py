# -*- coding: utf-8 -*-
"""
Created on Wed Jun  9 21:45:44 2021

"""
import pandas as pd

class gen_factor:
    
    def __init__(self):
        
        # 股票平均价格
        self.vwap = pd.read_pickle('data/daily/stock/S_DQ_AVGPRICE')
        
        # 数据文件和简称对应关系
        self.name_dict = {"定向增发":"AShareSEO",
                          "限售股解禁":"AShareCompRestricted",
                          "股票分红":"AShareDividend",
                          "股票回购":"AshareStockRepo",
                          "内部增减持":"AShareInsiderTrade",
                          "主要股东增减持":"AShareMjrHolderTrade"}
        
        # 读取数据
        self.AShareSEO = pd.read_pickle("data/money/AShareSEO") # 定向增发
        self.AShareCompRestricted = pd.read_pickle("data/money/AShareCompRestricted") # 限售股解禁
        self.AShareDividend = pd.read_pickle("data/money/AShareDividend") # 股票分红
        self.AshareStockRepo = pd.read_pickle("data/money/AshareStockRepo") # 股票回购
        self.AShareInsiderTrade = pd.read_pickle("data/money/AShareInsiderTrade") # 内部增减持  
        self.AShareMjrHolderTrade = pd.read_pickle("data/money/AShareMjrHolderTrade") # 主要股东增减持
                

        
    # ================================================================================
    # 指标计算
    # ================================================================================
    def standard_process(self, data_name):
        
        # 读取数据
        data = pd.read_pickle(f"data/money/{data_name}")
        
        # 计算资金流
        if data_name == "AShareSEO":
            
            # 定向增发：发行价格乘以发行数量，日期使用预案公告日
            date_name = "S_FELLOW_PREPLANDATE"
            data["AMOUNT"] = data['S_FELLOW_PRICE'] * data['S_FELLOW_AMOUNT']
            result = self.data_pivot(data,date_name)
        
        elif data_name == "AShareCompRestricted":
            
            # 限售股解禁：可流通数量乘以平均股票价格，日期使用可流通日期
            date_name = "S_INFO_LISTDATE"
            data["AMOUNT"] = data["S_SHARE_LST"]
            result = self.data_pivot(data,date_name)
            result *= self.vwap
        
        elif data_name == "AShareDividend":
            
            # 股票分红：税前每股派息乘以基准股本，日期使用预案公告日
            date_name = "S_DIV_PRELANDATE"
            data["AMOUNT"] = data["CASH_DVD_PER_SH_PRE_TAX"] * data["S_DIV_BASESHARE"]
            result = self.data_pivot(data,date_name)
            
        elif data_name == "AshareStockRepo":
            
            # 股票回购：回购金额，日期使用公告日期
            date_name = "ANN_DT"
            data["AMOUNT"] = data["AMT"]
            result = self.data_pivot(data,date_name)
        
        elif data_name == "AShareInsiderTrade":
            
            # 内部增减持：增减持数量乘以平均股票价格，日期使用交易日
            date_name = "TRADE_DT"
            data["AMOUNT"] = data["CHANGE_VOLUME"]
            
            positive = data[data["AMOUNT"] > 0]
            negative = data[data["AMOUNT"] < 0]
            p_res = self.data_pivot(positive,date_name)
            n_res = self.data_pivot(negative,date_name)
            p_res *= self.vwap
            n_res *= self.vwap
            
            result = [p_res,n_res]
        
        elif data_name == "AShareMjrHolderTrade":
            
            # 主要股东增减持：增减持数量乘以平均股票价格，日期使用交易开始日
            date_name = "TRANSACT_STARTDATE"
            data = data[data["IS_REANNOUNCED"] == 0]
            data["AMOUNT"] = data["TRANSACT_QUANTITY"] * data["TRANSACT_TYPE"].replace(["增持","减持"],[1,-1])
            
            # 正向策略
            positive = data[data["AMOUNT"] > 0]
            negative = data[data["AMOUNT"] < 0]
            p_res = self.data_pivot(positive,date_name)
            n_res = self.data_pivot(negative,date_name)
            p_res *= self.vwap
            n_res *= self.vwap
            
            result = [p_res,n_res]
                    
        return result
    
    # -------------------------------------------------------------------------
    # 数据展开
    # -------------------------------------------------------------------------
    def data_pivot(self,data,name):
        
        # 去掉日期列为空值的数据
        df = data.copy()
        df = df[~df[name].isna()]
        
        # 调整日期格式
        df[name] = pd.to_datetime(df[name],format="%Y%m%d")
        
        # 重新排列数据
        df = pd.pivot_table(df,index=name,columns="S_INFO_WINDCODE",values="AMOUNT",aggfunc="sum")
        df = df.reindex(index=self.vwap.index,columns=self.vwap.columns).fillna(0)
        
        return df



if __name__ == "__main__":
    
    model = gen_factor()

# # =============================================================================
# # ETF数据周度存储
# # =============================================================================
    
#     # 基金净值数据
#     fund_nav = pd.read_pickle("data/daily/fund/F_NAV_ADJUSTED")
#     fund_nav.to_csv("week_data/ETF收盘价.csv")
    
#     # 基金份额
#     FUNDSHARE_TOTAL = pd.read_pickle("data/daily/fund/FUNDSHARE")
#     FUNDSHARE_TOTAL.to_csv("week_data/ETF基金份额.csv")
    
# =============================================================================
# 基金发行和产业资本
# =============================================================================
  
    indicators = pd.DataFrame()    
    
    for name,data_name in model.name_dict.items():

        if name in ["产业资本（高管口径）","产业资本"]:
            [df1,df2] = model.standard_process(data_name)
            indicators[name+"增持"] = df1.sum(axis=1)
            indicators[name+"减持"] = df2.sum(axis=1)
            indicators[name+"净增减持"] = (df1+df2).sum(axis=1)
        
        elif name in ["基金发行"]:
            df = model.standard_process(data_name)
            df.columns = list(map(lambda s:f"{name}_{s}",df.columns))
            indicators = pd.concat([indicators,df],axis=1)
    
    # 保存数据
    indicators.to_excel("week_data/基金和产业资本数据.xlsx")

    # # 读取数据
    # data = pd.read_pickle("data/money/AShareInsiderTrade")
    # data.to_csv('AShareInsiderTrade(高管).csv')

    # data = pd.read_pickle("data/money/AShareMjrHolderTrade")
    # data.to_csv('AShareMjrHolderTrade.csv')


