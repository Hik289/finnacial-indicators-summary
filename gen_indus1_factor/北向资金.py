# -*- coding: utf-8 -*-
"""
Created on Sat Jul  3 23:01:25 2021


"""


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
        
        # 读取行业归属
        self.indus_info = pd.read_pickle(file_path + '/data/basic/indus1_info')    
        
        # 行业归属数据存储
        self.indus_belong = pd.read_pickle(file_path + '/data/daily/stock/indus1_belong')
            
        # 读取A股均价
        self.avg_price = pd.read_pickle(file_path + '/data/daily/stock/S_DQ_AVGPRICE')
            
        # 读取A股收盘价
        self.close = pd.read_pickle(file_path + '/data/daily/stock/S_DQ_CLOSE')
        
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
    # 将个股数据合并到行业层面
    # -------------------------------------------------------------------------
    def calc_indus_data(self, stock_data):
                            
        # 行业数据列表
        indus_data = pd.DataFrame(index=stock_data.index,
                    columns=self.indus_info['行业名称'].tolist()) 
        
        # 计算北向资金持股市值
        for index in stock_data.index:
                              
            # 获取当前截面股票数据
            df_cur = pd.DataFrame(
                {'汇总数据': stock_data.loc[index, :],
                 '行业归属': self.indus_belong.loc[index, :]},
                index=stock_data.columns)
                        
            # 行业持股数据和流通市值
            indus_data.loc[index, :] = df_cur.groupby('行业归属').sum()['汇总数据']
                    
        return indus_data

    
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
    
    
    # -------------------------------------------------------------------------
    # 存量指标：计算累计资金流入状况
    # -------------------------------------------------------------------------
    def gen_money_holdings(self, data_type='normal', freq='W', method='qoq'):

        # 股票数据
        origin_data = self.north_holdings * self.close.loc[self.north_holdings.index[0]:,:]

        # 计算行业数据
        origin_indus_data = self.calc_indus_data(origin_data)
              
        # 归一化
        if data_type == 'float':
            
            # 读取流通市值，计算资金流数据和行业流通市值之比
            indus_float = self.calc_indus_data(self.float_size)
            indus_data = self.data_resample(origin_indus_data/indus_float, freq, 'sum')
            
        if data_type == 'amount':
            
            # 读取成交额，计算资金流数据和行业成交额之比
            indus_amount = self.calc_indus_data(self.stock_amount)
            indus_data = self.data_resample(origin_indus_data/indus_amount, freq, 'sum')
            

        elif data_type == 'orig':
                             
            # 取样后数据
            data_resample = self.data_resample(origin_indus_data, freq, 'sum')
            
            # 数据在全市场占比
            indus_data = data_resample.multiply(1 / data_resample.sum(axis=1), axis=0)
            
        
        # 根据同比/环比设定进行数据调整
        if method == 'orig':
            output = indus_data
        
        elif method == 'qoq':
            shift_sign = 1    
            output = indus_data - indus_data.shift(shift_sign)
            
        elif method == 'yoy':
            if freq == 'W':
                shift_sign = 52
            else:
                shift_sign = 12
            output = indus_data - indus_data.shift(shift_sign)
            
        return output

    
    # -------------------------------------------------------------------------
    # 增量指标：计算资金流入边际变化情况
    # -------------------------------------------------------------------------
    def gen_money_change(self, data_type='normal', freq='W', method='qoq'):
                            
        # 股票数据
        origin_data = self.north_money

        # 计算行业数据
        origin_indus_data = self.calc_indus_data(origin_data)                   
            
        # 行业北向资金进行归一化
        data_resample = self.data_resample(origin_indus_data, freq, 'sum')
        
        # 归一化
        if data_type == 'float':
                
            # 取流通市值数据，并按照不同频率汇总
            indus_float = self.calc_indus_data(self.float_size)        
            float_resample = self.data_resample(indus_float, freq, 'sum')
            
            # 将资金数据归一化
            indus_data = data_resample / float_resample
                        
        # 归一化
        if data_type == 'amount':
                
            # 取成交额数据，并按照不同频率汇总
            indus_amount = self.calc_indus_data(self.stock_amount)        
            float_resample = self.data_resample(indus_amount, freq, 'sum')
            
            # 将资金数据归一化
            indus_data = data_resample / float_resample
            
            
        elif data_type == 'orig':
            indus_data = data_resample
            pass
        
        # 根据同比/环比设定进行数据调整
        if method == 'orig':
            output = indus_data
        
        elif method == 'qoq':
            shift_sign = 1    
            output = indus_data - indus_data.shift(shift_sign)
            
        elif method == 'yoy':
            if freq == 'W':
                shift_sign = 52
            else:
                shift_sign = 12
            output = indus_data - indus_data.shift(shift_sign)
            
        return output

    
if __name__ == "__main__":
    
    # 模型初始化
    model = north_money()
    
    # 生成调仓信号
    for data_type in ['orig', 'float', 'amount']:     # 是否除以流通市值 
        for freq in ['W','M']:   # 信号发出频率
            for method in ['orig', 'qoq', 'yoy']:  # 信号调整
                
                print(data_type, freq, method)
                
                # 北向流入资金统计
                data_monneyflow = model.gen_money_change(data_type, freq, method)
                data_monneyflow.to_pickle('north_results/north_change_{}_{}_{}'.format(data_type, freq, method))
                
                # 北向资金持股市值
                data_holdings = model.gen_money_holdings(data_type, freq, method)                
                data_holdings.to_pickle('north_results/north_holdings_{}_{}_{}'.format(data_type, freq, method))
    

    
    
    
    
    