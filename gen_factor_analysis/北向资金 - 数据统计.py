# -*- coding: utf-8 -*-
"""
Created on Sat Jul  3 23:01:25 2021

"""

# import sys
# sys.path.append("..")
import os 
import pandas as pd
import numpy as np


# -----------------------------------------------------------------------------
# 北向资金指标计算
# -----------------------------------------------------------------------------
class north_money():

    # -------------------------------------------------------------------------
    # 实例化对象，主要用于加载全局数据，避免每次重复加载
    # -------------------------------------------------------------------------
    def __init__(self):
        
        # 获取上级文件路径
        file_path = os.path.abspath(os.path.dirname(os.getcwd()))        
            
        # 读取A股均价
        self.avg_price = pd.read_pickle(file_path + '/data/daily/stock/S_DQ_AVGPRICE')
            
        # 读取A股收盘价
        self.stock_close = pd.read_pickle(file_path + '/data/daily/stock/S_DQ_ADJCLOSE')
        
        # 读取A股成交额
        self.stock_amount = pd.read_pickle(file_path + '/data/daily/stock/S_DQ_AMOUNT').loc['2005-01-01':,:] * 1000
        
        # 读取A股流通市值
        self.float_size = pd.read_pickle(file_path + '/data/daily/stock/S_DQ_MV').loc['2005-01-01':,:] * 10000
        
        # 读取北向资金持股数据
        self.north_holdings = pd.read_pickle(file_path + '/data/daily/stock/SHSCChannelholdings')

        # 所有日频交易日期序列
        self.daily_dates = pd.Series(self.avg_price.index, index=self.avg_price.index)
        
        # 计算北向资金净流入情况
        self.north_money = self.get_north_money()

        # 读取行业信息
        self.indus1_info = pd.read_pickle(file_path + '/data/basic/indus1_info')    
        self.indus2_info = pd.read_pickle(file_path + '/data/basic/indus2_info')  
        
        # 读取行业归属数据
        self.indus1_belong = pd.read_pickle(file_path + '/data/daily/stock/indus1_belong')
        self.indus2_belong = pd.read_pickle(file_path + '/data/daily/stock/indus2_belong')
        
        # 获取行业收盘价
        self.indus1_close = pd.read_pickle(file_path + '/data/daily/indus/indus1_close').loc['2015-01-01':,:]
        self.indus2_close = pd.read_pickle(file_path + '/data/daily/indus/indus2_close').loc['2015-01-01':,:]
                
        # 下载指数
        self.index_close = pd.read_pickle(file_path + '/data/daily/index/index_close')
        
        
    # -------------------------------------------------------------------------
    # 依据设定时间间隔获取设定区间内的交易日序列
    # [输入]
    # start_date     开始时间
    # end_date       终止时间
    # frequency      升采样频率，默认为一个月'M'，n个月为'nM'
    # -------------------------------------------------------------------------
    def gen_panel_dates(self, start_date, end_date, frequency='M'):
        
        # 指标时间区间
        month_end_dates = pd.date_range(start=start_date, end=end_date, freq=frequency)
        
        # 将交易日序列按月频重采样，取最后一个值
        monthly_dates = self.daily_dates.resample('M').last()
        
        # 将月末自然日替换成月末交易日
        panel_dates = pd.to_datetime(monthly_dates[month_end_dates].values)
        
        return panel_dates
    
    
    # -------------------------------------------------------------------------
    # 北向资金计算代码
    # -------------------------------------------------------------------------
    def get_north_money(self):
                
        # 北向资金流入额
        north_money = (self.north_holdings - self.north_holdings.shift(1)) *  self.avg_price

        # 北向资金空值替换成0
        north_money.fillna(0, inplace=True)
        
        # 按日历日提取数据
        north_money = north_money.resample('D').asfreq() 
        
        # 返回计算结果
        return north_money.loc['2014-01-01':,:]
        
    
    # -------------------------------------------------------------------------
    # 数据按设定频率进行提取
    # freq: 'W' 或是 'M'
    # -------------------------------------------------------------------------
    def data_resample(self, origin_data, freq, method):
                
        if method == 'sum':
                
            # 取不同频率数据
            data_freq = origin_data.resample(freq).sum()
            
        elif method == 'last':
                                
            # 取不同频率数据
            origin_data_change = origin_data.loc[origin_data.sum(axis=1)!=0, :]        
            data_freq = origin_data_change.resample(freq).last()
                
        # 日期调整，汇总到最近的交易日上
        change_dates = self.daily_dates.resample(freq).last()
        
        # 替换名称
        data_freq.index = pd.to_datetime(change_dates[data_freq.index].values)
        
        # 剔除空值数据
        data_freq = data_freq.loc[data_freq.index.notnull(), :]
        
        return data_freq
    
    
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
        days_list = np.arange(10, 110, 10)
    
        for days in days_list:
            
            print(days)
            
            # 按照交易日填数据计算收益率，按照日频填充计算
            stock_retn_ori = self.stock_close.shift(-days) / self.stock_close - 1
            stock_retn = stock_retn_ori.resample('D').ffill()
            
            # 行业收益率
            indus1_retn_ori  = self.indus1_close.shift(-days) / self.indus1_close - 1
            indus1_retn = indus1_retn_ori.resample('D').ffill()

            # 行业收益率
            indus2_retn_ori  = self.indus2_close.shift(-days) / self.indus2_close - 1
            indus2_retn = indus2_retn_ori.resample('D').ffill()
            
            # 指数收益率
            index_retn_ori  = self.index_close.shift(-days) / self.index_close - 1
            index_retn = index_retn_ori.resample('D').ffill()
            
            # 计算每只股票在对应时间下的超额收益
            data.loc[:, days] = data.apply(lambda x: stock_retn.loc[x.loc['统计日期'], x.loc['股票代码']], axis=1)
            data_indus1.loc[:, days] = data.apply(lambda x: indus1_retn.loc[x.loc['统计日期'], x.loc['一级行业归属']], axis=1)
            data_indus2.loc[:, days] = data.apply(lambda x: indus2_retn.loc[x.loc['统计日期'], x.loc['二级行业归属']], axis=1)
            data_index.loc[:, days] = data.apply(lambda x: index_retn.loc[x.loc['统计日期'], '000985.CSI'], axis=1)
            
        return data, data_indus1, data_indus2, data_index

        
    # ================================================================================
    # 添加行业信息
    # ================================================================================
    def add_indus_data(self, origin_data):                        
            
        # 获取股票数据
        stock_money = origin_data.reset_index(drop=False).melt(
                id_vars=['index'], var_name='股票代码', value_name='资金数据')
        stock_money.rename(columns={'index':'统计日期'}, inplace=True)
        
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
    # 作图整理
    # ================================================================================
    def plot_process(self, plot_data, thres=None):
                        
        # 作图
        tmp_data = plot_data.copy()
        
        # 日期序列
        days_list = tmp_data.columns[4:]        
        
        # 按资金量筛选
        if thres is not None:
            tmp_data = tmp_data.loc[tmp_data['资金数据'] > thres]

        # 分不同行业统计
        plot_data_indus = tmp_data.groupby('所属行业').median().loc[:, days_list]
        
        # 分时间段统计
        plot_data_date = tmp_data.set_index('统计日期')
        plot_data_date = plot_data_date.resample('Y').median().loc[:, days_list]
            
        return tmp_data.loc[:, days_list], plot_data_indus, plot_data_date
        
    
if __name__ == "__main__":
    
    # 模型初始化
    model = north_money()


    # 股票数据
    north_holdings_money = model.north_holdings * model.avg_price.loc[model.north_holdings.index[0]:,:]                        
    north_holdings_money = model.data_resample(north_holdings_money, 'M', 'sum')
    north_holdings_money[north_holdings_money==0] = np.nan
    
    # # 股票数据
    # north_money = model.north_money
    stock_money = model.add_indus_data(north_holdings_money)                        
            
    # 收益统计
    data, data_indus1, data_indus2, data_index = model.retn_process(stock_money)  
            
    # 绘图数据处理
    tmp_data, plot_data_indus, plot_data_date = \
                model.plot_process(data, thres=None)

    # 绘图                
    tmp_data.median(axis=0).plot()
    data_indus1.median(axis=0).plot()
    plot_data_indus.median(axis=0).plot()

