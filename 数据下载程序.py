# -*- coding: utf-8 -*-
"""
Created on Thu Jan  9 14:46:35 2020

"""

import pandas as pd
import numpy as np
import pymongo


class data_model():
    
    # -------------------------------------------------------------------------
    # 加载数据库信息
    # -------------------------------------------------------------------------
    def __init__(self):
                
        # 获取MondoDB数据库链接
        self.client = pymongo.MongoClient("localhost", 27017)
        
        # 获取股票数据库对象
        self.stock_database = self.client["xquant_stock"]

        # 获取股票财报数据库对象
        self.stock_financial_database = self.client["xquant_stock_financial"]
        
        # 获取行业数据库对象
        self.indus_database = self.client["xquant_indus"]
        
        # 获取指数数据库对象
        self.index_database = self.client["xquant_index"]
        
        # 获取一致预取数据库对象
        self.est_database = self.client["xquant_est"]

        # 获取其他数据库对象
        self.other_database = self.client["xquant_other"]
        
        # 获取资金流数据库对象
        self.money_database = self.client["xquant_money"]
        
        # 获取基金数据库对象
        self.fund_database = self.client["xquant_fund"]
        
        
    # -------------------------------------------------------------------------
    # 加载数据库信息
    # cursor      mongodb数据标签cursor
    # chunk_size  划分片数
    # -------------------------------------------------------------------------
    def cursor2dataframe(self, cursor, chunk_size: int):
        
        records = []  # 记录单片数据，写入dataframe
        frames = []   # 记录不同dataframe，拼接起来
        
        # 记录数据
        for i, record in enumerate(cursor):
            records.append(record)
            if i % chunk_size == chunk_size - 1:
                frames.append(pd.DataFrame(records))
                records = []
                
        # dataframe合并  
        if records:
                frames.append(pd.DataFrame(records))
                
        return pd.concat(frames)


    # -------------------------------------------------------------------------
    # 获取特定时间范围内的某个特定数据
    # database    数据库
    # collection  数据集（表）
    # start_date  开始时间
    # end_date    终止时间 
    # date_name   日期类型
    # stock_name  股票类型
    # target      调取数据目标
    # -------------------------------------------------------------------------
    def get_specific_data(self, database, collection, start_date, end_date,
                             date_name, stock_name, target, code=None):
      
        # 获取股票市值以及估值数据
        db_collection = database[collection]
        
        # 转换成pandas格式
        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date)
        
        # 读取数据        
        if collection in ['AShareBalanceSheet','AShareCashFlow','AShareIncome']:
            
            # 读取408001000合并报表数据，部分被处罚公司此部分数据会被后来的公告修正
            cursor = db_collection.find({date_name:{
                                "$gte":start_date.strftime("%Y%m%d"),
                                "$lte":end_date.strftime("%Y%m%d")}, 'STATEMENT_TYPE':408001000},
                                {'_id':0, target:1, stock_name:1, date_name:1}).sort('$natural',1)
            
            # 读取数据, 存成DataFrame格式
            data = self.cursor2dataframe(cursor, 100000)
            data = data.pivot(index=date_name, columns=stock_name)[target]
        
            # 读取408005000合并报表(更正前)数据，此部分数据是公司最原始的数据
            cursor = db_collection.find({date_name:{
                                "$gte":start_date.strftime("%Y%m%d"),
                                "$lte":end_date.strftime("%Y%m%d")}, 'STATEMENT_TYPE':408005000},
                                {'_id':0, target:1, stock_name:1, date_name:1}).sort('$natural',1)
            
            # 读取数据, 存成DataFrame格式
            data_origin = self.cursor2dataframe(cursor, 100000)
            data_origin = data_origin.pivot(index=date_name, columns=stock_name)[target]
            
            # 数据替换，有旧数据的优先用旧数据
            data[~data_origin.isnull()] = data_origin
            
            # index重新改写
            data.index = pd.to_datetime(data.index)
            
            return data
        
        else:
            if code == None:
                cursor = db_collection.find({date_name:{
                                "$gte":start_date.strftime("%Y%m%d"),
                                "$lte":end_date.strftime("%Y%m%d")}},
                                {'_id':0, target:1, stock_name:1, date_name:1}).sort('$natural',1)
            else:
                cursor = db_collection.find({date_name:{
                                "$gte":start_date.strftime("%Y%m%d"),
                                "$lte":end_date.strftime("%Y%m%d")},
                                stock_name:{"$in":code}},
                                {'_id':0, target:1, stock_name:1, date_name:1}).sort('$natural',1)
            
            # 读取数据, 存成DataFrame格式
            data = self.cursor2dataframe(cursor, 100000)
        
            # 业绩预告数据容易出现重复
            if collection in ['AShareProfitNotice', 'HKIndexEODPrices', 'HKStockHSIndustriesMembers',
                              "AShareMoneyFlow","AShareEODDerivativeIndicator"]:
                
                # 相同股票数据去重
                data = data.sort_values(date_name)
                data.drop_duplicates(subset=[stock_name, date_name], keep='last', inplace=True) 
                
                
            # 重新整理数据index和columns
            data = data.pivot(index=date_name, columns=stock_name)[target]
                   
            # index重新改写
            data.index = pd.to_datetime(data.index)
            
            return data
        
    
    # -------------------------------------------------------------------------
    # 获取特定时间范围内的所有数据：
    # database    数据库
    # collection  数据集（表）
    # start_date  开始时间
    # end_date    终止时间 
    # -------------------------------------------------------------------------
    def get_all_data(self, database, collection, start_date, end_date, date_name):
      
        # 获取股票市值以及估值数据
        db_collection = database[collection]
        
        # 转换成pandas格式
        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date)
        
        # 读取数据                
        cursor = db_collection.find({date_name:{
                            "$gte":start_date.strftime("%Y%m%d"),
                            "$lte":end_date.strftime("%Y%m%d")}},
                            {'_id':0}).sort('$natural',1)
        
        # 读取数据, 存成DataFrame格式
        data = self.cursor2dataframe(cursor, 10000)
                
        return data

    
if __name__ == '__main__':
        
    model = data_model()
    
    start_date = '1988-01-01'
    end_date = '2021-12-31'


# # =============================================================================
# #  读取股票列表
# # =============================================================================

#     print("股票列表")
    
#     # 最新A股信息
#     stock_info = pd.DataFrame(list(model.stock_database["AShareDescription"].find({}, {'_id':0})))
#     stock_info = stock_info.set_index('S_INFO_WINDCODE')
    
#     # 剔除A和T开头的股票代码
#     stock_info = stock_info.loc[[not i.startswith('A') for i in stock_info.index], :]
#     stock_info = stock_info.loc[[not i.startswith('T') for i in stock_info.index], :]
    
#     # 上市日期替换
#     stock_info.loc[:,"S_INFO_LISTDATE"] = pd.to_datetime(stock_info.loc[:,"S_INFO_LISTDATE"])
        
#     # 股票代码顺序重置
#     stock_info.sort_index(inplace=True)
#     stock_info.to_pickle('data/basic/stock_info')
#     stock_info = pd.read_pickle("data/basic/stock_info")

    
# # =============================================================================
# #  读取index列表
# # =============================================================================

#     print("指数列表")

#     # 最新index信息
#     index_info = pd.DataFrame(list(model.index_database["AIndexDescription"].find({}, {'_id':0})))
#     index_info = index_info.set_index('S_INFO_WINDCODE')

#     # 股票代码顺序重置
#     index_info.sort_index(inplace=True)
#     index_info.to_pickle('data/basic/index_info')


# # =============================================================================
# # index收盘价数据 - 用于策略回测
# # =============================================================================

#     print("指数收盘价")

#     # 按照报告期读取数据
#     codes = ['000300.SH', '000905.SH', '000008.SH', '000002.SH', '000016.SH', '000985.CSI']
#     index_close = model.get_specific_data(database = model.index_database,
#                                         collection = 'AIndexEODPrices',
#                                         start_date = start_date,
#                                         end_date = end_date,
#                                         date_name='TRADE_DT',           # 按照交易日（TRADE_DT）读取数据
#                                         stock_name='S_INFO_WINDCODE',   # 股票代码
#                                         target='S_DQ_CLOSE',
#                                         code = codes)         # 读取收盘价数据
    
#     index_close = index_close.reindex(columns=codes)
#     index_close.to_pickle('data/daily/index/index_close')


# # =============================================================================
# # 下载股票数据
# # =============================================================================
        
#     # 股票收盘价、交易状态
#     for data_name in ["S_DQ_CLOSE", "S_DQ_AVGPRICE", "S_DQ_ADJCLOSE", "S_DQ_AMOUNT"]:
#         print("个股数据:",data_name)
#         data = model.get_specific_data(database = model.stock_database,
#                                           collection = 'AShareEODPrices',
#                                           start_date = start_date,
#                                           end_date = end_date,
#                                           date_name = 'TRADE_DT',
#                                           stock_name = 'S_INFO_WINDCODE',
#                                           target = data_name) 
        
#         data = data.reindex(columns=stock_info.index.tolist())
#         data.to_pickle("data/daily/stock/"+data_name)
    

        
#     # 股票流通市值、总市值
#     for data_name in ["S_DQ_MV","S_VAL_MV"]:
#         print("个股数据:",data_name)
#         data = model.get_specific_data(database = model.stock_database,
#                                           collection = 'AShareEODDerivativeIndicator',
#                                           start_date = start_date,
#                                           end_date = end_date,
#                                           date_name = 'TRADE_DT',
#                                           stock_name = 'S_INFO_WINDCODE',
#                                           target = data_name) 
       
#         data = data.reindex(columns=stock_info.index.tolist())
#         data.to_pickle("data/daily/stock/"+data_name)


# # =============================================================================
# # 下载指数成分股数据，不含权重（只包含成分股什么时候进入什么时候退出）
# # =============================================================================
        
#     # 指数成分股数据
#     IndexMember = model.get_all_data(database=model.index_database, 
#                                 collection='AIndexMembers',  # 中国A股指数成份股
#                                 start_date=start_date,
#                                 end_date=end_date, 
#                                 date_name='S_CON_INDATE')  # 按照数据日期读取数据
  
#     # 日期替换
#     IndexMember['S_CON_INDATE'] = pd.to_datetime(IndexMember['S_CON_INDATE'])
#     IndexMember['S_CON_OUTDATE'] = pd.to_datetime(IndexMember['S_CON_OUTDATE'])
#     IndexMember = IndexMember.loc[:, ['S_INFO_WINDCODE', 'S_CON_WINDCODE', 'S_CON_INDATE', 'S_CON_OUTDATE']]
    
#     # 数据名称整理
#     IndexMember.rename(columns={'S_INFO_WINDCODE':'指数代码', 'S_CON_WINDCODE':'股票代码',
#                                 'S_CON_INDATE':'纳入日期', 'S_CON_OUTDATE':'剔除日期'},inplace=True)
        
#     # 指数成分股数据存储
#     IndexMember.to_pickle('data/daily/stock/AShare_Index_member')
    
    
# # =============================================================================
# # 计算日频交易日序列
# # =============================================================================
    
#     # 读取日频序列
#     daily_dates_index = pd.read_pickle('data/daily/stock/S_DQ_ADJCLOSE').index
            
#     # 生成Series
#     daily_dates = pd.Series(daily_dates_index, index=daily_dates_index)
     
#     # 数据存储
#     daily_dates.to_pickle('data/basic/daily_dates')

    
# # =============================================================================
# # 行业信息读取
# # =============================================================================
    
#     # 中信行业信息
#     indus_info = pd.read_excel('data/basic/行业代码汇总.xlsx', index_col=0, sheet_name='映射关系')
    
#     # 华泰行业划分
#     target1_indus = indus_info[indus_info['类别'].str.contains('一级行业')]
#     target1_indus.to_pickle('data/basic/indus1_info')    
    
#     target2_indus = indus_info[indus_info['类别'].str.contains('二级行业')]
#     target2_indus.to_pickle('data/basic/indus2_info')
    
#     target3_indus = indus_info[indus_info['类别'].str.contains('三级行业')]
#     target3_indus = target3_indus[target3_indus['Wind代码'].str.contains('旧版代码沿用') == False]
#     target3_indus.to_pickle('data/basic/indus3_info')
    
#     # 底层库行业代码替换成行业名称
#     name_code_to_name = dict(zip(indus_info['行业代码'], indus_info['行业名称']))
#     name_code_to_name['nan'] = 'nan'
    
#     # Wind底层库行业代码替换成行业名称
#     name_windcode_to_name = dict(zip(indus_info['Wind代码'], indus_info['行业名称']))
#     name_windcode_to_name['nan'] = 'nan'    


# # =============================================================================
# # 行业归属
# # =============================================================================
    
#     print("中信行业归属")
    
#     # 中信一级行业归属
#     indus1_belong = model.get_specific_data(database = model.indus_database,
#                                       collection = 'AShareIndustriesClassCITICS',
#                                       start_date = start_date,
#                                       end_date = end_date,
#                                       date_name = 'date',
#                                       stock_name = 'stock_code',
#                                       target = 'cs_indus1_code') 
    
#     # 中信二级行业归属
#     indus2_belong = model.get_specific_data(database = model.indus_database,
#                                       collection = 'AShareIndustriesClassCITICS',
#                                       start_date = start_date,
#                                       end_date = end_date,
#                                       date_name = 'date',
#                                       stock_name = 'stock_code',
#                                       target = 'cs_indus2_code') 

#     # 中信三级行业归属
#     indus3_belong = model.get_specific_data(database = model.indus_database,
#                                       collection = 'AShareIndustriesClassCITICS',
#                                       start_date = start_date,
#                                       end_date = end_date,
#                                       date_name = 'date',
#                                       stock_name = 'stock_code',
#                                       target = 'cs_indus3_code') 
    
#     # 底层库代码替换成行业名称
#     indus1_belong = indus1_belong.astype('str')    
#     indus1_belong_name = indus1_belong.applymap(lambda x: name_code_to_name[x])
    
#     indus2_belong = indus2_belong.astype('str')    
#     indus2_belong_name = indus2_belong.applymap(lambda x: name_code_to_name[x])    

#     indus3_belong = indus3_belong.astype('str')    
#     indus3_belong_name = indus3_belong.applymap(lambda x: name_code_to_name[x]) 
    
#     stock_info = pd.read_pickle("data/basic/stock_info")

#     # 行业归属数据存储
#     indus1_belong_name = indus1_belong_name.reindex(columns=stock_info.index)
#     indus1_belong_name.to_pickle('data/daily/stock/indus1_belong')
    
#     # 行业归属数据存储
#     indus2_belong_name = indus2_belong_name.reindex(columns=stock_info.index) 
#     indus2_belong_name.to_pickle('data/daily/stock/indus2_belong')
    
#     # 行业归属数据存储
#     indus3_belong_name = indus3_belong_name.reindex(columns=stock_info.index) 
#     indus3_belong_name.to_pickle('data/daily/stock/indus3_belong')
    
    
# # =============================================================================
# # 行业收盘价汇总
# # =============================================================================

#     print("行业收盘价")
    
#     # 股票信息
#     stock_info = pd.read_pickle("data/basic/stock_info")
    
#     # 按照报告期读取数据
#     indus_close = model.get_specific_data(database = model.indus_database,
#                                     collection = 'AIndexIndustriesEODCITICS',
#                                     start_date = start_date,
#                                     end_date = end_date,
#                                     date_name='TRADE_DT',           # 按照交易日（TRADE_DT）读取数据
#                                     stock_name='S_INFO_WINDCODE',   # 股票代码
#                                     target='S_DQ_CLOSE')            # 读取收盘价数据
           
    
#     ## 一级行业收盘价
#     indus1_close = indus_close.reindex(columns=target1_indus['Wind代码'])   

#     # 行业Wind代码替换成名称
#     indus1_close.columns = list(map(lambda x: name_windcode_to_name[x], indus1_close.columns))

#     # 前向填充    
#     indus1_close = indus1_close.bfill()
    
#     # 数据存储
#     indus1_close = indus1_close.reindex(columns=target1_indus['行业名称'].tolist())
#     indus1_close.to_pickle('data/daily/indus/indus1_close') 
        
    
#     ## 二级行业收盘价
#     indus_map = pd.read_excel('data/basic/行业代码汇总.xlsx', sheet_name='二级行业映射关系')
#     indus_map = indus_map[indus_map['新版二级行业代码'] == indus_map['旧版二级行业代码']]
    
#     indus_map = indus_map.loc[:, ['新版wind代码','旧版wind代码']].dropna()
#     map_list = indus_map[indus_map['旧版wind代码'] != indus_map['新版wind代码']] 
            
#     for index in map_list.index:        
        
#         new_name = map_list.loc[index, '新版wind代码']
#         old_name = map_list.loc[index, '旧版wind代码']
        
#         indus_close.loc[:'2019-11-29', new_name] = indus_close.loc[:'2019-11-29', old_name] / \
#             indus_close.loc['2019-11-29', old_name] * indus_close.loc['2019-11-29', new_name] 
           
#     ## 提取二级行业收盘价
#     indus2_close = indus_close.reindex(columns=target2_indus['Wind代码'])

#     # 行业Wind代码替换成名称
#     indus2_close.columns = list(map(lambda x: name_windcode_to_name[x], indus2_close.columns))
#     indus2_close.to_pickle('data/daily/indus/indus2_close')
    
    
#     ## 三级行业收盘价
#     indus_map = pd.read_excel('data/basic/行业代码汇总.xlsx', sheet_name='三级行业映射关系')
#     indus_map = indus_map[indus_map['新版三级行业代码'] == indus_map['旧版三级行业代码']]
    
#     indus_map = indus_map.loc[:, ['新版wind代码','旧版wind代码']].dropna()
#     map_list = indus_map[indus_map['旧版wind代码'] != indus_map['新版wind代码']] 
            
#     for index in map_list.index:        
        
#         new_name = map_list.loc[index, '新版wind代码']
#         old_name = map_list.loc[index, '旧版wind代码']
        
#         indus_close.loc[:'2019-11-29', new_name] = indus_close.loc[:'2019-11-29', old_name] / \
#             indus_close.loc['2019-11-29', old_name] * indus_close.loc['2019-11-29', new_name] 
           
#     ## 提取三级行业收盘价
#     indus3_close = indus_close.reindex(columns=target3_indus['Wind代码'])

#     # 行业Wind代码替换成名称
#     indus3_close.columns = list(map(lambda x: name_windcode_to_name[x], indus3_close.columns))
#     indus3_close.to_pickle('data/daily/indus/indus3_close')
    
    
# # =============================================================================
# # 读取融资融券数据
# # =============================================================================
    
#     print("融资融券数据")
    
#     AShareMarginTrade = model.get_all_data(database=model.money_database, 
#                               collection='AShareMarginTrade', 
#                               start_date = start_date,
#                               end_date = end_date,
#                               date_name='TRADE_DT')  # 按照发布日期读取
#     # 数据存储
#     AShareMarginTrade.to_pickle('data/money/AShareMarginTrade')

#     # 数据预处理
#     AShareMarginTrade = pd.read_pickle('data/money/AShareMarginTrade')    
#     stock_info = pd.read_pickle("data/basic/stock_info")

#     # 交易日期替换
#     AShareMarginTrade['TRADE_DT'] = [str(int(i)) for i in AShareMarginTrade['TRADE_DT']]
#     AShareMarginTrade['TRADE_DT'] = pd.to_datetime(AShareMarginTrade['TRADE_DT'])

#     # 待下载数据列表
#     data_dict = {'S_MARGIN_PURCHWITHBORROWMONEY':'融资买入额',
#                 'S_MARGIN_REPAYMENTTOBROKER':'融资偿还额',
#                 'S_MARGIN_REPAYMENTOFBORROWSEC':'融券偿还量',
#                 'S_MARGIN_SALESOFBORROWEDSEC':'融券卖出量',
#                 'S_MARGIN_MARGINTRADEBALANCE':'融资融券余额',
#                 'S_MARGIN_TRADINGBALANCE':'融资余额',
#                 'S_MARGIN_SECLENDINGBALANCE':'融券余额'}
    
#     for data_name in data_dict.keys():
        
#         # 读取当前数据
#         data = AShareMarginTrade.loc[:, ['TRADE_DT', 'S_INFO_WINDCODE',data_name]]  
        
#         # 展开成dataframe形式
#         data_pivot = data.pivot(index='TRADE_DT', columns='S_INFO_WINDCODE', values=data_name)
            
#         # 数据列表改写
#         data_pivot = data_pivot.reindex(columns=stock_info.index)
        
#         # 数据存储
#         data_pivot.to_pickle('data/daily/stock/{}'.format(data_name))
        
     
# # =============================================================================
# # 读取沪深港通持股数据
# # =============================================================================
    
#     print("北向资金数据")
    
#     # 数据下载
#     SHSCChannelholdings = model.get_all_data(database=model.money_database, 
#                               collection='SHSCChannelholdings', 
#                               start_date = start_date,
#                               end_date = end_date,
#                               date_name='TRADE_DT')  # 按照发布日期读取
    
#     # 取上交所和深交所数据
#     cur_data = SHSCChannelholdings[(SHSCChannelholdings['S_INFO_EXCHMARKETNAME'] == 'SZN') | \
#                                     (SHSCChannelholdings['S_INFO_EXCHMARKETNAME'] =='SHN')]
    
#     # 按照股票和交易日期求和
#     data = cur_data.groupby(['S_INFO_WINDCODE', 'TRADE_DT']).sum()['S_QUANTITY']
    
#     # 计算沪深港通持仓数据
#     pivot_data = data.reset_index()
#     pivot_data = pivot_data.pivot(index='TRADE_DT', columns='S_INFO_WINDCODE')['S_QUANTITY']
    
#     # 数据存储
#     pivot_data.index = pd.to_datetime(pivot_data.index)
#     pivot_data = pivot_data.reindex(columns=stock_info.index) 
#     pivot_data.to_pickle('data/daily/stock/SHSCChannelholdings')
    

# # =============================================================================
# # 基金数据下载
# # =============================================================================
    
#     print("基金数据下载")
    
#     # -------------------------------------------------------------------------
#     # 数据下载 - 读取基金基本信息
#     # -------------------------------------------------------------------------
#     # 要读取的列信息
#     target = {'F_INFO_WINDCODE'                : '基金代码',                                    
#               'F_INFO_FULLNAME'                : '基金全称',                 
#               'F_INFO_NAME'                    : '基金简称',                    
#               'F_INFO_CORP_FUNDMANAGEMENTCOMP' : '基金公司',  
#               'F_INFO_SETUPDATE'               : '成立日期',               
#               'F_INFO_BENCHMARK'               : '业绩基准',          
#               'F_ISSUE_TOTALUNIT'              : '发行份额', 
#               # 'F_INFO_INVESTSCOPE'             : '投资范围',
#               'F_INFO_FIRSTINVESTTYPE'         : '一级投资类型'}
    
#     # 设置查询标签
#     search = {'_id':0}
#     search.update(dict.fromkeys(target.keys(),1))
     
#     # 读取基金基本信息
#     fund_info = pd.DataFrame(list(
#         model.fund_database["ChinaMutualFundDescription"].find({},search)))   

#     # 对于重复的部分，保留新成立的基金
#     fund_info = fund_info.sort_values(by = ['F_INFO_SETUPDATE'])
#     fund_info = fund_info.drop_duplicates(subset='F_INFO_WINDCODE', keep='last') # 保留新成立的基金
    
#     # 修改列名
#     fund_info.rename(columns=target,inplace=True)
    
#     # 将基金成立日数据转换成时间戳
#     fund_info['成立日期'] = pd.to_datetime(fund_info['成立日期']) 
         
#     # 基金代码都替换为'.OF'形式的
#     fund_info['基金代码'] = fund_info['基金代码'].map(
#             lambda x: x.replace('.SH', '.OF').replace('.SZ', '.OF'))
    
#     # 最后一次去重，主要针对新成立的名字不同的基金
#     fund_info = fund_info.drop_duplicates(subset='基金代码', keep='last') 
#     fund_info = fund_info.set_index('基金代码')     
            
#     # 数据存储
#     fund_info.to_pickle('data/basic/fund_info')
    
    
#     # -------------------------------------------------------------------------
#     # ETF基金列表
#     # -------------------------------------------------------------------------  
            
#     # 基金信息
#     fund_info = pd.read_pickle('data/basic/fund_info')
    
#     # 读取基金类型数据
#     fund_rating = pd.DataFrame(list(model.fund_database["ChinaMutualFundSector"].find({},{'_id':0})))
    
#     # 基金名称替换
#     fund_rating['F_INFO_WINDCODE'] = fund_rating['F_INFO_WINDCODE'].map(
#               lambda x: x.replace('.SH', '.OF').replace('.SZ', '.OF'))
    
#     # 提取ETF基金列表
#     fund_rating1 = fund_rating.loc[fund_rating['S_INFO_SECTOR'].str.contains('20010202'),:].copy()
#     fund_rating2 = fund_rating.loc[fund_rating['S_INFO_SECTOR'].str.contains('2001010102'),:].copy()
    
#     # 读取有交集的基金数据
#     fund_list = set(fund_info.index.tolist()) & set(fund_rating1['F_INFO_WINDCODE'].tolist()) & set(fund_rating2['F_INFO_WINDCODE'].tolist())

#     # 提取ETF数据
#     ETF_fund_info = fund_info.loc[fund_list,:]

#     # 基金基准数据
#     fund_bench = pd.DataFrame(list(model.fund_database["ChinaMutualFundBenchMark"].find({},{'_id':0})))
#     fund_bench = fund_bench.loc[fund_bench['S_INFO_WINDCODE'].notnull(),:]
    
#     # 替换名称
#     fund_bench['S_INFO_WINDCODE'] = fund_bench['S_INFO_WINDCODE'].map(
#               lambda x: x.replace('.SH', '.OF').replace('.SZ', '.OF'))
    
#     # 提取主信息
#     fund_bench = fund_bench[fund_bench['S_INFO_INDEXWEG']>50]
    
#     # 剔除变更了跟踪基准基金的旧版基准    
#     ETF_fund_bench = fund_bench[fund_bench['S_INFO_WINDCODE'].isin(ETF_fund_info.index)]
    
#     # 对于重复的部分，保留新成立的基金
#     ETF_fund_bench = ETF_fund_bench.sort_values(by=['S_INFO_BGNDT'])
#     ETF_fund_bench = ETF_fund_bench.drop_duplicates(subset='S_INFO_WINDCODE', keep='last') # 保留新成立的基金
    
#     # 记录数据
#     ETF_fund_bench = ETF_fund_bench.set_index('S_INFO_WINDCODE')
#     ETF_fund_info['跟踪指数'] = ETF_fund_bench['S_INFO_INDEXWINDCODE']
#     ETF_fund_info.to_pickle('data/basic/ETF_fund_info')


#     # -------------------------------------------------------------------------
#     # 读取基金复权净值数据
#     # -------------------------------------------------------------------------  
    
#     # 读取基金基本信息
#     fund_info = pd.read_pickle('data/basic/fund_info')    
    
#     F_NAV_ADJUSTED = model.get_specific_data(database=model.fund_database,  
#                                 collection = 'ChinaMutualFundNAV',  # 中国共同基金净值
#                                 start_date = start_date,
#                                 end_date = end_date,
#                                 date_name = 'PRICE_DATE',           # 按照截止日期（PRICE_DATE）读取数据
#                                 stock_name='F_INFO_WINDCODE',       # 基金代码
#                                 target='F_NAV_ADJUSTED')            # 复权净值      
        
#     # 基金代码都替换为'.OF'形式的
#     F_NAV_ADJUSTED.columns = F_NAV_ADJUSTED.columns.map(
#             lambda x: x.replace('.SH', '.OF').replace('.SZ', '.OF'))

#     # 部分基金净值分两段记载，重复的基金净值进行拼接
#     same_funds = F_NAV_ADJUSTED.columns[F_NAV_ADJUSTED.columns.duplicated()]
#     other_funds = set(list(F_NAV_ADJUSTED.columns)) - set(list(same_funds))
    
#     # 汇总数据
#     fund_nav_change = []
    
#     for same_fund_name in same_funds:
        
#         # 读取相应的基金净值
#         same_fund_nav = F_NAV_ADJUSTED.loc[:,same_fund_name]
        
#         # 判断最近更新净值的日期
#         judge_notice = same_fund_nav.notnull()
#         judge_notice_sum = judge_notice.cumsum(axis=0)        
        
#         # 判断仍在更新的基金位置
#         newer_id = judge_notice_sum.idxmax().values.argmax()
#         older_id = judge_notice_sum.idxmax().values.argmin()       
        
#         # 数据拼接汇总
#         fund_nav_merge = same_fund_nav.iloc[:, newer_id]
#         fund_nav_merge.loc[fund_nav_merge.isnull()] = same_fund_nav.iloc[:, older_id][fund_nav_merge.isnull()]  
#         fund_nav_change.append(fund_nav_merge)
    
#     # 处理后的基金净值数据拼接
#     fund_nav_summary = pd.concat([pd.concat(fund_nav_change, axis=1), F_NAV_ADJUSTED.loc[:, other_funds]] , axis=1)
    
#     # 共同的基金名称
#     mutual_funds = set(list(fund_info.index)) & set(list(F_NAV_ADJUSTED.columns))
             
#     # 基金信息提取
#     fund_info = fund_info.loc[mutual_funds, :]
#     fund_nav = fund_nav_summary.loc[:, mutual_funds]
    
#     # 将成立日之前的基金净值置为空，规避掉一些冗余数据
#     for fund_name in fund_nav.columns:
        
#         # 当前基金信息    
#         cur_fund_info = fund_info.loc[fund_name, :]
        
#         # 当前基金净值
#         fund_nav.loc[:cur_fund_info['成立日期'], fund_name] = np.nan
        
#     # 空值填充
#     fund_nav_fill = fund_nav.ffill()

#     # 基金净值数据
#     fund_nav_fill.to_pickle("data/daily/fund/F_NAV_ADJUSTED")


#     # -------------------------------------------------------------------------
#     # 读取基金单位净值数据
#     # -------------------------------------------------------------------------  
        
#     # 读取基金基本信息
#     fund_info = pd.read_pickle('data/basic/fund_info')    

#     F_NAV_ADJUSTED = model.get_specific_data(database=model.fund_database,  
#                                 collection = 'ChinaMutualFundNAV',  # 中国共同基金净值
#                                 start_date = start_date,
#                                 end_date = end_date,
#                                 date_name = 'PRICE_DATE',           # 按照截止日期（PRICE_DATE）读取数据
#                                 stock_name='F_INFO_WINDCODE',       # 基金代码
#                                 target='F_NAV_UNIT')            # 复权净值      
        
#     # 基金代码都替换为'.OF'形式的
#     F_NAV_ADJUSTED.columns = F_NAV_ADJUSTED.columns.map(
#             lambda x: x.replace('.SH', '.OF').replace('.SZ', '.OF'))

#     # 部分基金净值分两段记载，重复的基金净值进行拼接
#     same_funds = F_NAV_ADJUSTED.columns[F_NAV_ADJUSTED.columns.duplicated()]
#     other_funds = set(list(F_NAV_ADJUSTED.columns)) - set(list(same_funds))
    
#     # 汇总数据
#     fund_nav_change = []
    
#     for same_fund_name in same_funds:
        
#         # 读取相应的基金净值
#         same_fund_nav = F_NAV_ADJUSTED.loc[:,same_fund_name]
        
#         # 判断最近更新净值的日期
#         judge_notice = same_fund_nav.notnull()
#         judge_notice_sum = judge_notice.cumsum(axis=0)        
        
#         # 判断仍在更新的基金位置
#         newer_id = judge_notice_sum.idxmax().values.argmax()
#         older_id = judge_notice_sum.idxmax().values.argmin()       
        
#         # 数据拼接汇总
#         fund_nav_merge = same_fund_nav.iloc[:, newer_id]
#         fund_nav_merge.loc[fund_nav_merge.isnull()] = same_fund_nav.iloc[:, older_id][fund_nav_merge.isnull()]  
#         fund_nav_change.append(fund_nav_merge)
    
#     # 处理后的基金净值数据拼接
#     fund_nav_summary = pd.concat([pd.concat(fund_nav_change, axis=1), F_NAV_ADJUSTED.loc[:, other_funds]] , axis=1)
    
#     # 共同的基金名称
#     mutual_funds = set(list(fund_info.index)) & set(list(F_NAV_ADJUSTED.columns))
    
#     # 基金信息提取
#     fund_info = fund_info.loc[mutual_funds, :]
#     fund_nav = fund_nav_summary.loc[:, mutual_funds]
    
#     # 将成立日之前的基金净值置为空，规避掉一些冗余数据
#     for fund_name in fund_nav.columns:
        
#         # 当前基金信息    
#         cur_fund_info = fund_info.loc[fund_name, :]
        
#         # 当前基金净值
#         fund_nav.loc[:cur_fund_info['成立日期'], fund_name] = np.nan
        
#     # 空值填充
#     fund_nav_fill = fund_nav.ffill()

#     # 基金净值数据
#     fund_nav_fill.to_pickle("data/daily/fund/F_NAV_UNIT")

    
#     # -------------------------------------------------------------------------
#     # 基金份额数据
#     # -------------------------------------------------------------------------  
            
#     # 基金份额
#     FUNDSHARE_TOTAL = model.get_specific_data(database=model.fund_database,  
#                                 collection = 'ChinaMutualFundShare',
#                                 start_date = start_date,
#                                 end_date = end_date,
#                                 date_name = 'CHANGE_DATE',           
#                                 stock_name='F_INFO_WINDCODE',        
#                                 target='FUNDSHARE_TOTAL')      
    
#     # 基金代码都替换为'.OF'形式的
#     FUNDSHARE_TOTAL.columns = FUNDSHARE_TOTAL.columns.map(
#             lambda x: x.replace('.SH', '.OF').replace('.SZ', '.OF'))
    
#     # 寻找重复基金
#     same_funds = FUNDSHARE_TOTAL.columns[FUNDSHARE_TOTAL.columns.duplicated()]
#     other_funds = set(list(FUNDSHARE_TOTAL.columns)) - set(list(same_funds))
    
#     # 汇总数据
#     fund_share_change = []
    
#     for same_fund_name in same_funds:
        
#         # 读取相应的基金份额
#         same_fund_nav = FUNDSHARE_TOTAL.loc[:,same_fund_name]
        
#         # 判断仍在更新的基金位置
#         newer_id = same_fund_nav.notnull().sum().argmax()    
        
#         # 数据拼接汇总
#         fund_share_change.append(same_fund_nav.iloc[:, newer_id])
                
#     # 处理后的基金份额数据拼接
#     fund_share_summary = pd.concat([pd.concat(fund_share_change, axis=1),
#                                     FUNDSHARE_TOTAL.loc[:, other_funds]] , axis=1)
    
#     # 基金份额数据
#     fund_share_summary.to_pickle("data/daily/fund/FUNDSHARE")

    
    # # -------------------------------------------------------------------------
    # # 基金份额数据
    # # ------------------------------------------------------------------------- 
    
    # # 读取基金份额数据
    # fund_share_summary = pd.read_pickle("data/daily/fund/FUNDSHARE")
      
    # # 基金份额
    # change_reason = model.get_specific_data(database=model.fund_database,  
    #                             collection = 'ChinaMutualFundShare',
    #                             start_date = start_date,
    #                             end_date = end_date,
    #                             date_name = 'CHANGE_DATE',           
    #                             stock_name='F_INFO_WINDCODE',        
    #                             target='CHANGEREASON')      
    

    # # 基金代码都替换为'.OF'形式的
    # change_reason.columns = change_reason.columns.map(
    #         lambda x: x.replace('.SH', '.OF').replace('.SZ', '.OF'))    
    # change_reason = change_reason.loc[:,~change_reason.columns.duplicated()]
    
    # # 保留ETF基金份额变动
    # ETF_fund_info = pd.read_pickle('data/basic/ETF_fund_info')
    
    # ETF_change_reason = change_reason.reindex(columns=ETF_fund_info.index)   
    # ETF_change_reason = ETF_change_reason.loc[:,~ETF_change_reason.columns.duplicated()]
    # ETF_share = fund_share_summary.reindex(columns=ETF_fund_info.index)   
    
    # # 数据存储
    # ETF_share[~(ETF_change_reason=='SGSH')] = np.nan
    # ETF_share.to_pickle("data/daily/fund/ETF_share")
    
    

# =============================================================================
# 基金发行、产业资本数据
# =============================================================================

    # 数据下载列表        
    map_list = {
                'ChinaMutualFundIssue':'OPDATE', # 中国共同基金发行
                'AshareStockRepo':'OPDATE', # 股票回购
                'AShareSEO':'OPDATE', # 定向增发
                'AShareRightIssue':'OPDATE', # 配股
                # 'AShareIPO':'OPDATE', # IPO
                'AShareDividend':'OPDATE', # 股票分红
                # 'AShareCompRestricted':'OPDATE',  # 限售股解禁
                'AShareMjrHolderTrade':'ANN_DT', # 主要股东增减持
                'AShareInsiderTrade':'ACTUAL_ANN_DT', # 内部增减持
                'AShareFreeFloatCalendar':'OPDATE', # 中国A股限售股流通日历  
                'ASarePlanTrade':'ANN_DT_NEW'  #中国A股股东拟增减持计划  
                } 
                
        
    for index in map_list.keys():
        
        print('下载：{}'.format(index))
        
        # 数据下载
        data = model.get_all_data(database=model.money_database,
                                collection=index,
                                start_date=start_date,
                                end_date=end_date,
                                date_name=map_list[index])
        
        # 数据存储
        data.to_pickle('data/money/{}'.format(index))
        

    
    