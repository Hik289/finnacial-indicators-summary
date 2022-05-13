# -*- coding: utf-8 -*-
"""
Created on Sat Jul  3 23:01:25 2021

"""


import os 
import pandas as pd
import numpy as np


# -----------------------------------------------------------------------------
# 产业资本指标计算
# -----------------------------------------------------------------------------
class indus_cap():

    # -------------------------------------------------------------------------
    # 实例化对象，主要用于加载全局数据，避免每次重复加载
    # -------------------------------------------------------------------------
    def __init__(self):
        
        # 获取上级文件路径
        file_path = os.path.abspath(os.path.dirname(os.getcwd()))
    
        # 读取行业信息
        self.indus1_info = pd.read_pickle(file_path + '/data/basic/indus1_info')    
        self.indus2_info = pd.read_pickle(file_path + '/data/basic/indus2_info')  
        
        # 读取行业归属数据
        self.indus1_belong = pd.read_pickle(file_path + '/data/daily/stock/indus1_belong')
        self.indus2_belong = pd.read_pickle(file_path + '/data/daily/stock/indus2_belong')
        
        # 获取行业收盘价
        self.indus1_close = pd.read_pickle(file_path + '/data/daily/indus/indus1_close').loc['2005-01-01':,:]
        self.indus2_close = pd.read_pickle(file_path + '/data/daily/indus/indus2_close').loc['2005-01-01':,:]
                
        # 读取A股均价
        self.avg_price = pd.read_pickle(file_path + '/data/daily/stock/S_DQ_AVGPRICE')
        
        # 读取A股收盘价
        self.stock_close = pd.read_pickle(file_path + '/data/daily/stock/S_DQ_ADJCLOSE') # 复权
        self.stock_ori_close = pd.read_pickle(file_path + '/data/daily/stock/S_DQ_CLOSE') # 不复权
        
        # 下载指数
        self.index_close = pd.read_pickle(file_path + '/data/daily/index/index_close')        
        
        # 读取A股收盘价
        self.stock_close = pd.read_pickle(file_path + '/data/daily/stock/S_DQ_ADJCLOSE') 
        
        # # 读取A股流通市值
        # self.float_size = pd.read_pickle(file_path + '/data/daily/stock/S_DQ_MV').loc['2005-01-01':,:]

        # 所有日频交易日期序列
        self.daily_dates = pd.Series(self.stock_close.index, index=self.stock_close.index)
    
        # 是否出现过借壳上市
        self.judge = (pd.read_csv(file_path + '/data/借壳上市.csv', index_col=0, encoding='gbk') == '否').iloc[0,:]
    
        # 数据文件和简称对应关系
        self.name_dict = {"定向增发":"AShareSEO",
                          "限售股解禁":"AShareFreeFloatCalendar",
                          "股票回购":"AshareStockRepo",
                          "内部增减持":"AShareInsiderTrade",
                          "主要股东增减持":"AShareMjrHolderTrade",
                          "拟增减持":"ASarePlanTrade"}
            
        # 读取数据
        self.AShareSEO = pd.read_pickle(file_path + "/data/money/AShareSEO") # 定向增发
        self.AShareFreeFloatCalendar = pd.read_pickle(file_path + "/data/money/AShareFreeFloatCalendar") # 限售股解禁
        self.AshareStockRepo = pd.read_pickle(file_path + "/data/money/AshareStockRepo") # 股票回购
        self.AShareInsiderTrade = pd.read_pickle(file_path + "/data/money/AShareInsiderTrade") # 内部增减持  
        self.AShareMjrHolderTrade = pd.read_pickle(file_path + "/data/money/AShareMjrHolderTrade") # 主要股东增减持
        self.ASarePlanTrade = pd.read_pickle(file_path + "/data/money/ASarePlanTrade") # 拟增减持
              
        
    # ================================================================================
    # 数据读取
    # ================================================================================
    def data_preprocess(self, data_name, paras):
                
        # 定向增发
        if data_name == "AShareSEO":
                            
            # 相同时间数据去重
            cur_data = self.AShareSEO.sort_values('OPDATE')
            cur_data = cur_data.drop_duplicates(subset=['EVENT_ID'], keep='last') 
            
            # 预案公告日	S_FELLOW_PREPLANDATE
            # 股东大会公告日	S_FELLOW_SMTGANNCEDATE
            # 发审委/上市委通过公告日	S_FELLOW_PASSDATE
            # 证监会通过公告日	S_FELLOW_APPROVEDDATE
            # 增发公告日	S_FELLOW_OFFERINGDATE		
            
            para_dict = {'preplan':'S_FELLOW_PREPLANDATE',    # 预案公告日
                         'pass':'S_FELLOW_PASSDATE',          # 审核通过日
                         'offering':'S_FELLOW_OFFERINGDATE'}  # 增发公告日
            
            # 提取数据
            data = cur_data.loc[:,['S_INFO_WINDCODE', para_dict[paras], 
                             'S_FELLOW_PRICE','S_FELLOW_AMOUNT','EXP_COLLECTION']]
            
            # 募股数调整，计算实际募集金额
            data["S_FELLOW_AMOUNT"] = data['S_FELLOW_AMOUNT'] * 10000
            data["AMOUNT"] = data['S_FELLOW_PRICE'] * data['S_FELLOW_AMOUNT']
            
            # 列名替换
            data.columns = ['股票代码', '统计日期', '募集价格', '募集股数', '预期募集资金', '实际募集资金']
            data = data[~data.loc[:,'统计日期'].isnull()]
            data = data[~(data.loc[:,'股票代码'] == 'T00018.SH')]
            data = data.reset_index(drop=True)
            
            # 日期形式转换，只保留2005年以后的数据
            data['统计日期'] = [pd.to_datetime(str(int(i))) for i in data['统计日期']]
            data = data[data['统计日期'] < self.stock_ori_close.index[-1]]
            
            # 股票和行业价格数据进行填充
            stock_close = self.stock_ori_close.resample('D').ffill()
            
            # 有实际募集资金数据时，优先用实际募集资金（发行价格*发行数量）
            # 没有实际募集资金数据采用预期募集资金数据（EXP_COLLECTION）
            # 如果两者都没有，采用目标募集股份数*当日价格的80%进行估算    
            for index in data.index:
            
                # 记录预估发行价格
                data.loc[index, '预估发行价格'] = \
                stock_close.loc[data.loc[index, '统计日期'], data.loc[index, '股票代码']] * 0.8
                                               
            # 预估募集金额
            data['预估募集金额'] = data['预估发行价格'] * data['募集股数']
            
            # 只有在增发公告日会公布详细的募集金额，其他几个时间都采用预估的募集数据
            if para_dict[paras] != 'S_FELLOW_OFFERINGDATE':
                data.loc[:, '实际募集资金'] == 0
            
            # 预估募集资金汇总
            data['汇总金额'] = data['实际募集资金']
            data.loc[data['汇总金额'].isnull(),'汇总金额'] = data.loc[data['汇总金额'].isnull(),'预期募集资金'] 
            data.loc[data['汇总金额'].isnull(),'汇总金额'] = data.loc[data['汇总金额'].isnull(),'预估募集金额'] 
            
            # 结果汇总
            output = data.loc[:, ['股票代码', '统计日期', '汇总金额']]
            output.columns = ['股票代码', '统计日期', '资金数据']
                
            
        # 限售股解禁    
        elif data_name == "AShareFreeFloatCalendar":
            
            # 上市股份数量（万股）	S_SHARE_LST
            # 未上市股份数量（万股）	S_SHARE_NONLST
            
            # 提取数据
            data = self.AShareFreeFloatCalendar.loc[:,['S_INFO_WINDCODE',
                   'S_INFO_LISTDATE', 'ANN_DT', 'S_SHARE_LST', 'S_SHARE_NONLST']]
            
            # 列名替换
            data.columns = ['股票代码', '限售股上市日期', '公告日期', '上市股份数量', '未上市股份数量']
            data = data[~(data.loc[:,'股票代码'] == 'T00018.SH')]
            data = data.reset_index(drop=True)
            
            # 从万股转换成股
            data.loc[:,['上市股份数量', '未上市股份数量']] = data.loc[:,['上市股份数量', '未上市股份数量']] * 10000
            
            # 日期形式转换
            data['公告日期'] = pd.to_datetime(data['公告日期'].astype(str))
            data['限售股上市日期'] = [np.nan if pd.isnull(i) else pd.to_datetime(str(int(i))) for i in data['限售股上市日期']]  
            data = data[data['限售股上市日期'] < self.stock_ori_close.index[-1]]
                        
            # 按照时间差距
            data['时间差距'] = data['限售股上市日期'] - data['公告日期']
            data['修正日期'] = data['限售股上市日期'] - pd.Timedelta(days=20)
            data.loc[data['时间差距'] > '20 days', '公告日期'] = data['修正日期'] 
            
            # 股票和行业价格数据进行填充
            stock_close = self.stock_ori_close.resample('D').ffill()
                    
            # 按照最近股价统计
            for index in data.index:
            
                # 记录解禁
                data.loc[index, '预估解禁价格'] = \
                stock_close.loc[data.loc[index, '公告日期'], data.loc[index, '股票代码']]
                           
            # 预估募集金额 
            data['预估解禁金额'] = data['预估解禁价格'] * data['上市股份数量']
            
            # 按照公告日期提取数据
            if paras == 'anndt':             
                
                # 结果汇总
                output = data.loc[:, ['股票代码', '公告日期', '预估解禁金额']]
                output.columns = ['股票代码', '统计日期', '资金数据']
                
            elif paras == 'listdt':   
                
                # 结果汇总
                data = data[~(data['时间差距'] < '0 days')]
                output = data.loc[:, ['股票代码', '公告日期',  '限售股上市日期', '上市股份数量']]  
                output.columns = ['股票代码', '统计日期',  '限售股上市日期', '上市股份数量']
           
        # 股票回购
        elif data_name == "AshareStockRepo":
             
            # 每个回购时间持续时间过长，不适合进行长时间统计
            # 着重统计三个时间点：董事会预案、股东大会通过、中间实施回购的日期（可能有多个）
        
            # 提取数据
            data = self.AshareStockRepo.loc[:,['S_INFO_WINDCODE', 'ANN_DT', 
                                      'AMT', 'TOTAL_SHARE_RATIO', 'STATUS']]
            
            # 列名替换
            data.columns = ['股票代码', '公告日期', '回购金额',
                            '回购股数占总股本比例', '进度类型代码']
            data = data.reset_index(drop=True)
            
            # # 提取类型
            # 324003002	董事会预案
            # 324003004	股东大会通过
            # 324004001	回购实施                    
            para_dict = {'preplan':324003002,   # 324003002	董事会预案
                         'pass':324003004,      # 324003004	股东大会通过
                         'conduct':324004001}   # 324004001	回购实施
            
            data = data[data['进度类型代码'] == para_dict[paras]]
        
            # 日期类型转换
            data['公告日期'] = pd.to_datetime(data['公告日期'].astype(str))
            data = data[data['公告日期'] < self.stock_ori_close.index[-1]]
                               
            # 结果汇总
            output = data.loc[:, ['股票代码', '公告日期', '回购金额']]
            output.columns = ['股票代码', '统计日期', '资金数据']
                        

        # 主要股东增减持
        elif data_name == "MjrHolderTrade":
                                
            # 提取数据
            data_mjr = self.AShareMjrHolderTrade.loc[:,
                          ['S_INFO_WINDCODE','ANN_DT', 'TRANSACT_QUANTITY',
                           'TRANSACT_TYPE', 'AVG_PRICE', 'IS_REANNOUNCED']]
            
            # 列名替换
            data_mjr.columns = ['股票代码', '公告日期', '变动数量', 
                                '买卖方向', '均价', '是否重复']
            
            # 剔除重复数据
            data_mjr_diff = data_mjr[~(data_mjr['是否重复']==1)] 
            
            # 替换形式
            data_mjr_diff = data_mjr_diff.reset_index(drop=True)    
            data_mjr_diff['公告日期'] = pd.to_datetime(data_mjr_diff['公告日期'])
            
            # 股票价格数据进行填充
            stock_close = self.stock_ori_close.resample('D').ffill()
                                           
            # 记录预估发行价格          
            for index in data_mjr_diff.index:
                
                if np.isnan(data_mjr_diff.loc[index, '均价']):
                    
                    data_mjr_diff.loc[index, '均价'] = stock_close.loc[
                            data_mjr_diff.loc[index, '公告日期'],
                            data_mjr_diff.loc[index, '股票代码']]    
        
            # 提取数据
            data_inside = self.AShareInsiderTrade.loc[:,['S_INFO_WINDCODE', 
                      'ACTUAL_ANN_DT', 'CHANGE_VOLUME',  'TRADE_AVG_PRICE']]
            data_inside = data_inside.reset_index(drop=True)  
            
            # 列名替换
            data_inside.columns = ['股票代码', '公告日期', '变动数量', '均价']
                        
            # 日期类型转换
            data_inside['公告日期'] = pd.to_datetime(data_inside['公告日期'])
            
            # 计算变动金额
            data_inside.loc[data_inside['变动数量']>0, '买卖方向'] = '增持'
            data_inside.loc[data_inside['变动数量']<0, '买卖方向'] = '减持'
            data_inside['变动数量'] = data_inside['变动数量'].abs()
            
            # 数据合并
            merge_data = pd.concat([data_mjr_diff, data_inside], axis=0)
            merge_data = merge_data.reset_index(drop=True)    
            
            # 预估募集金额
            merge_data['变动金额'] = merge_data['变动数量'] * merge_data['均价']
            
            # 相同数据合并
            map_dict = {'over':'增持', 'under':'减持'}
            merge_data = merge_data[merge_data['买卖方向'] == map_dict[paras]]
            merge_data_group = merge_data.groupby(['股票代码', '公告日期']).sum()['变动金额'].reset_index()    
               
            # 结果汇总
            output = merge_data_group.loc[:, ['股票代码', '公告日期', '变动金额']]
            output.columns = ['股票代码', '统计日期', '资金数据']
        
            
        # 拟增减持
        elif data_name == "ASarePlanTrade":
                                
            # 首次披露公告日	ANN_DT
            # 最新公告日	ANN_DT_NEW
            # 变动起始日期	CHANGE_START_DATE
            # 变动截止日期	CHANGE_END_DATE
            # 拟变动数量上限(股/张)	PLAN_TRANSACT_MAX_NUM
           
            # 提取日期
            para_dict = {'最新公告日':'ANN_DT_NEW',
                          '变动起始日期':'CHANGE_START_DATE'}
            
            # 提取数据
            data = self.ASarePlanTrade.loc[:,['S_INFO_WINDCODE', 'ANN_DT_NEW', 
                        'TRANSACT_TYPE','PLAN_TRANSACT_MAX_NUM', 'PLAN_TRANSACT_MAX']]
            
            # 列名替换
            data.columns = ['股票代码', '统计日期', '变动方向','拟变动数量上限', '拟变动金额上限']
            data = data.reset_index(drop=True)
            
            # 提取类型
            data = data[data['变动方向'] == paras]
            
            # 日期类型转换
            data['统计日期'] = [np.nan if pd.isnull(i) else pd.to_datetime(str(int(i))) for i in data['统计日期']]          
            data = data[data['统计日期'] < self.stock_ori_close.index[-1]]
            # data = data.dropna(axis=0)
                                    
            data = data.loc[data['股票代码'].isin(self.stock_ori_close.columns),:]
            
            # 股票和行业价格数据进行填充
            stock_close = self.stock_ori_close.resample('D').ffill()
                    
            # 按照最近股价统计姐
            for index in data.index:
                
                # 记录解禁
                data.loc[index, '预估价格'] = \
                stock_close.loc[data.loc[index, '统计日期'], data.loc[index, '股票代码']]
                
            # 预估募集金额
            data['拟变动金额上限-估计'] = data['预估价格'] * data['拟变动数量上限']
            data.loc[data['拟变动金额上限'].isnull(),'拟变动金额上限'] = \
                data.loc[data['拟变动金额上限'].isnull(),'拟变动金额上限-估计'] 
           
            # 结果汇总
            output = data.loc[:, ['股票代码', '统计日期', '拟变动金额上限']]
            output.columns = ['股票代码', '统计日期', '资金数据']  
    
        return output
    
                
    # ================================================================================
    # 添加行业信息
    # ================================================================================
    def add_indus_data(self, origin_data):                        
            
        # 获取股票数据
        stock_money = origin_data.copy()
        
        # 读取行业归属数据
        indus1_belong = self.indus1_belong.reset_index(drop=False).melt(
                id_vars=['date'], var_name='股票代码', value_name='一级行业归属')
        indus1_belong.rename(columns={'date':'统计日期'}, inplace=True)
    
        indus2_belong = self.indus2_belong.reset_index(drop=False).melt(
                id_vars=['date'], var_name='股票代码', value_name='二级行业归属')
        indus2_belong.rename(columns={'date':'统计日期'}, inplace=True)
        
        # 添加行业归属数据
        stock_money_indus = pd.merge(stock_money, indus1_belong, how='left', on=['统计日期', '股票代码'])
        stock_money_indus = pd.merge(stock_money_indus, indus2_belong, how='left', on=['统计日期', '股票代码'])
        
        # 剔除空值
        stock_money_indus = stock_money_indus.dropna().reset_index(drop=True)
        
        return stock_money_indus
        
    
    # ================================================================================
    # 收益统计
    # ================================================================================
    def retn_process(self, origin_data):
                    
        # 数据复制
        data = origin_data.copy()
                            
        # 剔除所属行业为空数据
        data = data.loc[~(data.loc[:, '一级行业归属']=='nan'),:]
        data = data.loc[~(data.loc[:, '二级行业归属']=='nan'),:]
                
        # 记录行业和指数收益率
        data_indus1 = data.copy()
        data_indus2 = data.copy()
        data_index = data.copy()
        
        # 日期列表
        days_list = np.arange(0, 55, 5)
    
        for days in days_list:
            
            print(days)
            
            # 按照交易日填数据计算收益率，按照日频填充计算
            stock_retn_ori = self.stock_close.shift(-days) / self.stock_close - 1
            stock_retn = stock_retn_ori.resample('D').ffill()
            
            # 行业收益率
            indus1_retn_ori = self.indus1_close.shift(-days) / self.indus1_close - 1
            indus1_retn = indus1_retn_ori.resample('D').ffill()

            # 行业收益率
            indus2_retn_ori = self.indus2_close.shift(-days) / self.indus2_close - 1
            indus2_retn = indus2_retn_ori.resample('D').ffill()
            
            # 指数收益率
            index_retn_ori = self.index_close.shift(-days) / self.index_close - 1
            index_retn = index_retn_ori.resample('D').ffill()
            
            # 计算每只股票在对应时间下的超额收益
            data.loc[:, days] = data.apply(lambda x: stock_retn.loc[x.loc['统计日期'], x.loc['股票代码']], axis=1)
            data_indus1.loc[:, days] = data.apply(lambda x: indus1_retn.loc[x.loc['统计日期'], x.loc['一级行业归属']], axis=1)
            data_indus2.loc[:, days] = data.apply(lambda x: indus2_retn.loc[x.loc['统计日期'], x.loc['二级行业归属']], axis=1)
            data_index.loc[:, days] = data.apply(lambda x: index_retn.loc[x.loc['统计日期'], '000985.CSI'], axis=1)
            
        return data, data_indus1, data_indus2, data_index
    
    
    # ================================================================================
    # 作图整理
    # ================================================================================
    def plot_process(self, plot_data, thres=None):
                        
        # 作图
        tmp_data = plot_data.copy()
        
        # 日期序列
        days_list = tmp_data.columns[5:]        
        
        # 按资金量筛选
        if thres is not None:
            tmp_data = tmp_data.loc[tmp_data['资金数据'] > thres]
             
        # # 剔除借壳上市股票
        # tmp_data = tmp_data[tmp_data['股票代码'].isin(self.judge[self.judge].index.tolist())]

        # 分不同行业统计
        plot_data_indus1 = tmp_data.groupby('一级行业归属').median().loc[:, days_list]
        plot_data_indus2 = tmp_data.groupby('二级行业归属').median().loc[:, days_list]
        
        # 分时间段统计
        plot_data_date = tmp_data.set_index('统计日期')
        plot_data_date = plot_data_date.resample('Y').median().loc[:, days_list]
            
        return tmp_data.loc[:, days_list], plot_data_indus1, plot_data_indus2, plot_data_date
        
    
if __name__ == "__main__":
    
    # 模型初始化
    model = indus_cap()

    # 数据列表
    # 定向增发 AShareSEO （预案公告有正收益、审核通过为中性、增发公告后会下跌）
    # 'preplan' # 预案公告日
    # 'pass':  # 审核通过日
    # 'offering' # 增发公告日
                                
    # 限售股解禁 AShareFreeFloatCalendar
        
    # 股票回购 AshareStockRepo (回购实施最好，但是策略跑的一般)
    # 'preplan'   # 董事会预案
    # 'pass'      # 股东大会通过
    # 'conduct'   # 回购实施
    
    # 增减持 MjrHolderTrade
    # AShareInsiderTrade 合并 AShareMjrHolderTrade   
    # 'over' 增持
    # 'under' 减持
    # 拟增减持 ASarePlanTrade
    #     参数: 增持/减持 
    #     参数: 最新公告日/变动起始日期
            
              
            
            
# # =============================================================================
# #  股东增减持
# # =============================================================================

#     data_name = 'MjrHolderTrade'
#     paras = 'under'  # over/under
                
#     # 定向增发
#     data_pos = model.data_preprocess(data_name, paras)
    
#     # 股票
#     data_pos = data_pos.groupby(['股票代码', '统计日期']).sum()['资金数据'].reset_index()
#     data_pos = data_pos.loc[data_pos['统计日期']>pd.to_datetime('2005-01-01'),:]    
#     data_pos = data_pos.loc[data_pos['资金数据']>0,:]    
    
#     # 添加行业归属数据    
#     data_pos_add = model.add_indus_data(data_pos)
    
#     # 收益数据添加个股
#     data, data_indus1, data_indus2, data_index = model.retn_process(data_pos_add)  
          
#     # 计算超额收益
#     ex_data = data.copy()
#     ex_data.iloc[:,5:] = data.iloc[:,5:] - data_index.iloc[:,5:]
    
#     # 绘图数据处理    
#     tmp_data, plot_data_indus1, plot_data_indus2, plot_data_date = \
#                 model.plot_process(ex_data, thres=None)

#     # 绘图                
#     tmp_data.median(axis=0, skipna=True).plot()
    
#     # 分段统计      
#     merge_data = []
#     for thres in [0, 1E6, 5E6, 1E7, 5E7, 1E8]:
        
#         tmp_data, plot_data_indus1, plot_data_indus2, plot_data_date = \
#                     model.plot_process(ex_data, thres)
                    
#         print(tmp_data.shape[0])
        
#         merge_data.append(tmp_data.median(skipna=True,axis=0))
        
#     # 结果统计     
#     final_data = pd.concat(merge_data,axis=1)
   


    
# # =============================================================================
# #  股票回购
# # =============================================================================

#     data_name = 'AshareStockRepo'
#     paras = 'conduct'
    
#     # 'preplan'   # 董事会预案
#     # 'pass'      # 股东大会通过
#     # 'conduct'   # 回购实施
                
#     # 定向增发
#     data_pos = model.data_preprocess(data_name, paras)
    
#     # 股票
#     data_pos = data_pos.groupby(['股票代码', '统计日期']).sum()['资金数据'].reset_index()
#     data_pos = data_pos.loc[data_pos['统计日期']>pd.to_datetime('2005-01-01'),:]    
#     data_pos = data_pos.loc[data_pos['资金数据']>0,:]    
    
#     # 添加行业归属数据    
#     data_pos_add = model.add_indus_data(data_pos)
    
#     # 收益数据添加个股
#     data, data_indus1, data_indus2, data_index = model.retn_process(data_pos_add)  
          
#     # 计算超额收益
#     ex_data = data.copy()
#     ex_data.iloc[:,5:] = data.iloc[:,5:] - data_index.iloc[:,5:]
    
#     # 绘图数据处理    
#     tmp_data, plot_data_indus1, plot_data_indus2, plot_data_date = \
#                 model.plot_process(ex_data, thres=None)

#     # 绘图                
#     tmp_data[tmp_data==0] = np.nan
#     tmp_data.median(axis=0, skipna=True).plot()
   
    
# # =============================================================================
# #  限售解禁
# # =============================================================================

#     data_name = 'AShareFreeFloatCalendar'
#     paras = 'anndt'
#     # paras = 'listdt'
                
#     # 定向增发
#     data_pos = model.data_preprocess(data_name, paras)
    
#     # 股票
#     data_pos = data_pos.groupby(['股票代码', '统计日期']).sum()['资金数据'].reset_index()
#     data_pos = data_pos.loc[data_pos['统计日期']>pd.to_datetime('2005-01-01'),:]    
#     data_pos = data_pos.loc[data_pos['资金数据']>0,:]    
    
#     # 添加行业归属数据    
#     data_pos_add = model.add_indus_data(data_pos)
    
#     # 收益数据添加个股
#     data, data_indus1, data_indus2, data_index = model.retn_process(data_pos_add)  
          
#     # 计算超额收益
#     ex_data = data.copy()
#     ex_data.iloc[:,5:] = data.iloc[:,5:] - data_index.iloc[:,5:]
    
#     # 绘图数据处理    
#     tmp_data, plot_data_indus1, plot_data_indus2, plot_data_date = \
#                 model.plot_process(ex_data, thres=None)

#     # 绘图                
#     tmp_data.median(axis=0, skipna=True).plot()
    
#     # 分段统计      
#     merge_data = []    
#     for thres in [0, 1E8, 5E8, 1E9, 5E9, 1E10]:
        
#         tmp_data, plot_data_indus1, plot_data_indus2, plot_data_date = \
#                     model.plot_process(ex_data, thres)
                    
#         print(tmp_data.shape[0])
        
#         merge_data.append(tmp_data.median(skipna=True,axis=0))
        
#     # 结果统计     
#     final_data = pd.concat(merge_data,axis=1)
   


# # =============================================================================
# #  定增统计
# # =============================================================================

#     data_name = 'AShareSEO'
#     paras = 'offering'
                
#     # 定向增发
#     data_pos = model.data_preprocess(data_name, paras)
    
#     # 股票
#     data_pos = data_pos.groupby(['股票代码', '统计日期']).sum()['资金数据'].reset_index()
#     data_pos = data_pos.loc[data_pos['统计日期']>pd.to_datetime('2005-01-01'),:]    
#     data_pos = data_pos.loc[data_pos['资金数据']>0,:]    
    
#     # 添加行业归属数据    
#     data_pos_add = model.add_indus_data(data_pos)
    
#     # 收益数据添加个股
#     data, data_indus1, data_indus2, data_index = model.retn_process(data_pos_add)  
          
#     # 计算超额收益
#     ex_data = data.copy()
#     ex_data.iloc[:,5:] = data.iloc[:,5:] - data_index.iloc[:,5:]
    
#     # 绘图数据处理    
#     tmp_data, plot_data_indus1, plot_data_indus2, plot_data_date = \
#                 model.plot_process(ex_data, thres=None)

#     # 绘图                
#     tmp_data.median(axis=0, skipna=True).plot()
    
#     # 分段统计      
#     merge_data = []    
#     for thres in [0, 1E8, 5E8, 1E9, 5E9, 1E10]:
        
#         tmp_data, plot_data_indus1, plot_data_indus2, plot_data_date = \
#                     model.plot_process(ex_data, thres)
                    
#         print(tmp_data.shape[0])
        
#         merge_data.append(tmp_data.median(skipna=True,axis=0))
        
#     # 结果统计     
#     final_data = pd.concat(merge_data,axis=1)
   
    
   
    # for thres in [1E7, 2E7, 3E7, 4E7, 5E7, 
    #               6E7, 7E7, 8E7, 9E7, 10E7, 20E7, 30E7]:
        
    # # 每月统计下个月或是下周解禁数量
    # # 本月解禁数量 / 下月解禁数量

     
    # tmp_data, plot_data_indus1, plot_data_indus2, plot_data_date = \
    #             model.plot_process(data, thres=1E7)
            



     
     
     
     
     
     
     
     
     
     
     
     
     
     
     
     
     
     
     
     
     
     
    