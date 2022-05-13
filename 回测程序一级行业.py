# -*- coding: utf-8 -*-
"""
Created on Tue Sep 17 13:56:20 2019


"""


from utils import BacktestUtils, PerfUtils
import pandas as pd
import os
import matplotlib.pyplot as plt
import numpy as np
plt.rcParams['font.sans-serif'] = ['KaiTi']  # 用来正常显示中文标签
plt.rcParams['axes.unicode_minus'] = False    # 用来正常显示负号


class model_backtest():
    
    # -------------------------------------------------------------------------
    # 实例化，加载基本信息
    # -------------------------------------------------------------------------
    def __init__(self):
            
        # 获取一级行业收盘价
        self.indus_close = pd.read_pickle('data/daily/indus/indus1_close')
        # self.indus_close = self.indus_close.bfill()
                                       
        # 生成日频交易序列
        self.daily_dates = pd.Series(self.indus_close.index, index=self.indus_close.index)


    # -------------------------------------------------------------------------
    # 获取设定时间区间内的月末交易日 - 设定时间间隔
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
    # 回测过程计算
    # [输入]
    # df_factor      行业景气度指标，一般只有1，-1，0三个数值，分别代表多头/空头/空仓
    # start_date     计算回测净值曲线开始时间
    # end_date       计算回测净值曲线终止时间
    # -------------------------------------------------------------------------
    def backtest(self, df_factor_input, start_date, end_date, base, fee=0):
        
        df_factor = df_factor_input.copy()
        
        # 当行业景气度指标给定的最后一个日期大于最后一个交易日时（月末容易出现）
        # 最后一个交易信号无法调仓
        if df_factor.index[-1] >= self.daily_dates[-1]:
            df_factor = df_factor.drop(index=df_factor.index[-1])
            
        # 调仓日期为生成信号的下一天，即月初第一个交易日        
        df_factor.index = [self.daily_dates[self.daily_dates > i].index[0] for i in df_factor.index]        
        
        # # 根据输入的行业指标，计算多头和空头持仓
        # long_portion_all = df_factor.copy() * 0
        # short_portion_all = df_factor.copy() * 0
        
        # # 遍历所有日期
        # for date in df_factor.index:    
            
        #     # # 一般情况，标为1的行业为多头持仓，这里简化为指标值大于零的行业为多头持仓
        #     # if sum(df_factor.loc[date,:] > 0) != 0:
        #     #     long_portion_all.loc[date,df_factor.loc[date,:]>0] = 1/sum(df_factor.loc[date,:] > 0)
        #     # else:
        #     #     long_portion_all.loc[date,:] = 1/df_factor.shape[1]
                
        #     # # 一般情况，标为-1的行业为空头持仓，这里简化为指标值小于零的行业为空头持仓
        #     # if sum(df_factor.loc[date,:] < 0) != 0:
        #     #     short_portion_all.loc[date,df_factor.loc[date,:]<0] = 1/sum(df_factor.loc[date,:] < 0)
        #     # else:
        #     #     short_portion_all.loc[date,:] = 1/df_factor.shape[1]
            

        # # 对持仓进行时间截断 
        # long_portion = long_portion_all[start_date:end_date]
        # short_portion = short_portion_all[start_date:end_date]

        # 根据输入的行业指标，计算多头和空头持仓
        long_portion_all = df_factor.copy()
        long_portion_all[long_portion_all<=0] = 0
        short_portion_all = df_factor.copy()
        short_portion_all[short_portion_all>=0] = 0
        
        # 遍历所有日期
        for date in df_factor.index:    
                        
            # 计算多头持仓
            if sum(df_factor.loc[date,:] > 0) != 0:
                long_portion_all.loc[date,:] = long_portion_all.loc[date,:] / sum(long_portion_all.loc[date,:])
                
            # 计算空头持仓
            if sum(df_factor.loc[date,:] < 0) != 0:
                short_portion_all.loc[date,:] = short_portion_all.loc[date,:] / sum(short_portion_all.loc[date,:])
                
        # 对持仓进行时间截断
        long_portion = long_portion_all[start_date:end_date]
        short_portion = short_portion_all[start_date:end_date]

        
        # 参照基准 - 行业收益率等权
        if base == 'mean' :   
            indus_return = self.indus_close.pct_change()
            base_return = indus_return.mean(axis=1) + 1
            base_close = base_return.cumprod()
            base_nav = base_close.loc[start_date:end_date]
            
        # 参照基准 - 沪深300
        elif base == 'hs300':
            base_nav = self.hs300_close.loc[start_date:end_date,'close']
            
        # 计算绝对净值        
        nav = pd.DataFrame(columns=['多头策略','空头策略','基准'])
        
        # 回测,计算策略净值
        nav['多头策略'], turn_long = BacktestUtils.cal_nav(long_portion, 
           self.indus_close[start_date:end_date], base_nav, fee)
            
        nav['空头策略'], turn_short = BacktestUtils.cal_nav(short_portion,
           self.indus_close[start_date:end_date], base_nav, fee)
        
        # 基准净值归一化
        nav['基准'] = base_nav / base_nav.values[0]
          
        # 计算相对净值
        nav_relative = pd.DataFrame(columns=['多头/基准','空头/基准'])
        nav_relative['多头/基准'] = nav['多头策略'] / nav['基准'] 
        nav_relative['空头/基准'] = nav['空头策略'] / nav['基准']
        
        # 返回绝对净值曲线，相对净值曲线，多头持仓和空头持仓
        return nav, nav_relative, long_portion, short_portion, turn_long, turn_short


    # -------------------------------------------------------------------------
    # 回测过程计算
    # [输入]
    # df_factor      行业景气度指标，一般只有1，-1，0三个数值，分别代表多头/空头/空仓
    # start_date     计算回测净值曲线开始时间
    # end_date       计算回测净值曲线终止时间
    # -------------------------------------------------------------------------
    def clsfy_backtest(self, origin_factor, start_date, end_date, base):
        
        merge_factor = origin_factor.copy()
        
        # 当行业景气度指标给定的最后一个日期大于最后一个交易日时（月末容易出现）
        # 最后一个交易信号无法调仓
        if merge_factor.index[-1] >= self.daily_dates[-1]:
            merge_factor = merge_factor.drop(index=merge_factor.index[-1])
            
        # 调仓日期为生成信号的下一天，即月初第一个交易日        
        merge_factor.index = [self.daily_dates[self.daily_dates > i].index[0] for i in merge_factor.index]        
                
        # 对复合行业景气度指标进行指数加权
        merge_factor_ewm = merge_factor.ewm(alpha=0.99).mean()
        
        # 加权后的景气度指标进行排序
        merge_factor_rank = merge_factor_ewm.rank(method='average', ascending=False, axis=1)

        # 分层数
        indus_number = 5
        layer_ind_num = np.int(np.floor(merge_factor_rank.shape[1] / indus_number))
        inter_part = merge_factor_rank.shape[1] % indus_number / indus_number
        thres = layer_ind_num + inter_part
        
        # 初始化返回值
        factor_1 = pd.DataFrame(np.zeros_like(merge_factor.values), index=merge_factor.index, columns=merge_factor.columns)
        factor_2 = pd.DataFrame(np.zeros_like(merge_factor.values), index=merge_factor.index, columns=merge_factor.columns)
        factor_3 = pd.DataFrame(np.zeros_like(merge_factor.values), index=merge_factor.index, columns=merge_factor.columns)
        factor_4 = pd.DataFrame(np.zeros_like(merge_factor.values), index=merge_factor.index, columns=merge_factor.columns)
        factor_5 = pd.DataFrame(np.zeros_like(merge_factor.values), index=merge_factor.index, columns=merge_factor.columns)
               
        # 多头持仓行业
        factor_1[merge_factor_rank <= thres] = 1
        factor_2[(merge_factor_rank > thres) & (merge_factor_rank <= 2 * thres)] = 1
        factor_3[(merge_factor_rank > 2 * thres) & (merge_factor_rank <= 3 * thres)] = 1
        factor_4[(merge_factor_rank > 3 * thres) & (merge_factor_rank <= 4 * thres)] = 1
        factor_5[merge_factor_rank > 4 * thres] = 1
        
        if np.ceil(thres) != np.floor(thres):
            factor_1[merge_factor_rank == np.ceil(thres)] = thres - np.floor(thres)
            factor_2[merge_factor_rank == np.ceil(thres)] = np.ceil(thres) - thres

        if np.ceil(2 * thres) != np.floor(2 * thres):
            factor_2[merge_factor_rank == np.ceil(2 *thres)] = 2 * thres - np.floor(2 * thres)
            factor_3[merge_factor_rank == np.ceil(2 *thres)] = np.ceil(2 * thres) - 2 * thres
            
        if np.ceil(3 * thres) != np.floor(3 * thres):
            factor_3[merge_factor_rank == np.ceil(3 *thres)] = 3 * thres - np.floor(3 * thres)
            factor_4[merge_factor_rank == np.ceil(3 *thres)] = np.ceil(3 * thres) - 3 * thres
            
        if np.ceil(4 * thres) != np.floor(4 * thres):
            factor_4[merge_factor_rank == np.ceil(4 *thres)] = 4 * thres - np.floor(4 * thres)
            factor_5[merge_factor_rank == np.ceil(4 *thres)] = np.ceil(4 * thres) - 4 * thres
            
        # 计算多头持仓
        factor_1 = (factor_1.multiply(1/factor_1.sum(axis=1), axis=0)).loc[start_date:end_date,:]
        factor_2 = (factor_2.multiply(1/factor_2.sum(axis=1), axis=0)).loc[start_date:end_date,:]
        factor_3 = (factor_3.multiply(1/factor_3.sum(axis=1), axis=0)).loc[start_date:end_date,:]
        factor_4 = (factor_4.multiply(1/factor_4.sum(axis=1), axis=0)).loc[start_date:end_date,:]
        factor_5 = (factor_5.multiply(1/factor_5.sum(axis=1), axis=0)).loc[start_date:end_date,:]

        # 全为零行替换为全仓
        factor_1[(factor_1==0).sum(axis=1) == factor_1.shape[1]] = 1/factor_1.shape[1]
        factor_2[(factor_2==0).sum(axis=1) == factor_2.shape[1]] = 1/factor_2.shape[1]
        factor_3[(factor_3==0).sum(axis=1) == factor_3.shape[1]] = 1/factor_3.shape[1]
        factor_4[(factor_4==0).sum(axis=1) == factor_4.shape[1]] = 1/factor_4.shape[1]
        factor_5[(factor_5==0).sum(axis=1) == factor_5.shape[1]] = 1/factor_5.shape[1]
        
        # 空值替换为全仓
        factor_1[factor_1.isnull().sum(axis=1) == factor_1.shape[1]] = 1/factor_1.shape[1]
        factor_2[factor_2.isnull().sum(axis=1) == factor_2.shape[1]] = 1/factor_2.shape[1]
        factor_3[factor_3.isnull().sum(axis=1) == factor_3.shape[1]] = 1/factor_3.shape[1]
        factor_4[factor_4.isnull().sum(axis=1) == factor_4.shape[1]] = 1/factor_4.shape[1]
        factor_5[factor_5.isnull().sum(axis=1) == factor_5.shape[1]] = 1/factor_5.shape[1]
            
        # 参照基准 - 行业收益率等权
        if base == 'mean' :   
            indus_return = self.indus_close.pct_change()
            base_return = indus_return.mean(axis=1) + 1
            base_close = base_return.cumprod()
            base_nav = base_close.loc[start_date:end_date]
                        
        # 计算绝对净值        
        nav = pd.DataFrame(columns=['分层1','分层2','分层3','分层4','分层5','基准'])
        
        # 回测,计算策略净值
        nav['分层1'], _ = BacktestUtils.cal_nav(factor_1, self.indus_close[start_date:end_date], base_nav, fee=0)
        nav['分层2'], _ = BacktestUtils.cal_nav(factor_2, self.indus_close[start_date:end_date], base_nav, fee=0)
        nav['分层3'], _ = BacktestUtils.cal_nav(factor_3, self.indus_close[start_date:end_date], base_nav, fee=0)
        nav['分层4'], _ = BacktestUtils.cal_nav(factor_4, self.indus_close[start_date:end_date], base_nav, fee=0)
        nav['分层5'], _ = BacktestUtils.cal_nav(factor_5, self.indus_close[start_date:end_date], base_nav, fee=0)
        
        # 基准净值归一化
        nav['基准'] = base_nav / base_nav.values[0]
          
        # 计算相对净值
        nav_relative = pd.DataFrame(columns=['分层1','分层2','分层3','分层4','分层5'])
        nav_relative['分层1'] = nav['分层1'] / nav['基准'] 
        nav_relative['分层2'] = nav['分层2'] / nav['基准'] 
        nav_relative['分层3'] = nav['分层3'] / nav['基准'] 
        nav_relative['分层4'] = nav['分层4'] / nav['基准'] 
        nav_relative['分层5'] = nav['分层5'] / nav['基准'] 
        
        # 返回绝对净值曲线，相对净值曲线，多头持仓
        return nav, nav_relative, factor_1
    

    # -------------------------------------------------------------------------
    # 回测过程计算
    # [输入]
    # df_factor      行业景气度指标，一般只有1，-1，0三个数值，分别代表多头/空头/空仓
    # start_date     计算回测净值曲线开始时间
    # end_date       计算回测净值曲线终止时间
    # -------------------------------------------------------------------------
    def thres_backtest(self, merge_factor, start_date, end_date, base):
               
        # 当行业景气度指标给定的最后一个日期大于最后一个交易日时（月末容易出现）
        # 最后一个交易信号无法调仓
        if merge_factor.index[-1] >= self.daily_dates[-1]:
            merge_factor = merge_factor.drop(index=merge_factor.index[-1])
            
        # 调仓日期为生成信号的下一天，即月初第一个交易日        
        merge_factor.index = [self.daily_dates[self.daily_dates > i].index[0] for i in merge_factor.index]        
        
        # 数据汇总
        thres = pd.DataFrame(0, index=merge_factor.index, 
                             columns=['多头阈值1','多头阈值2','多头阈值3',
                                      '空头阈值1','空头阈值2','空头阈值3'])

        # 遍历所有日期
        for date in merge_factor.index:    
            
            # 当前指标
            cur_factor = merge_factor.loc[:date, :].copy()
            cur_factor[cur_factor==0] = np.nan
            
            # 剔除空值
            cur_data = cur_factor.dropna(how='all')
            if len(cur_data) > 0:
                thres.loc[date,'多头阈值1'] = np.nanquantile(cur_factor, 0.9)
                thres.loc[date,'多头阈值2'] = np.nanquantile(cur_factor, 0.7)
                thres.loc[date,'多头阈值3'] = np.nanquantile(cur_factor, 0.5)
                thres.loc[date,'空头阈值1'] = np.nanquantile(cur_factor, 0.1)
                thres.loc[date,'空头阈值2'] = np.nanquantile(cur_factor, 0.3)
                thres.loc[date,'空头阈值3'] = np.nanquantile(cur_factor, 0.5)

                
        # 参照基准 - 行业收益率等权
        if base == 'mean' :   
            indus_return = self.indus_close.pct_change()
            base_return = indus_return.mean(axis=1) + 1
            base_nav = base_return.cumprod().loc[start_date:end_date]
                        
        # 计算绝对净值        
        nav = pd.DataFrame(columns=['多头阈值1','多头阈值2','多头阈值3',
                                    '空头阈值1','空头阈值2','空头阈值3', '基准'])
        
        for column_index in thres.columns:
            
            # 初始化返回值
            df_factor = pd.DataFrame(np.zeros_like(merge_factor.values), 
                                     index=merge_factor.index, columns=merge_factor.columns)
            
            # 计算多空头持仓
            if column_index.find('空头') > -1:
                df_factor[merge_factor.sub(thres.loc[:, column_index] , axis=0) < 0] = 1
                
            elif column_index.find('多头') > -1:
                df_factor[merge_factor.sub(thres.loc[:, column_index] , axis=0) > 0] = 1
                
            # 计算多头持仓
            df_factor = (df_factor.multiply(1 / df_factor.sum(axis=1), axis=0)).loc[start_date:end_date,:]
            
            # 空值部分采用整体持仓计算
            df_factor[(df_factor==0).sum(axis=1) == df_factor.shape[1]] = 1/df_factor.shape[1]
            df_factor[df_factor.isnull().sum(axis=1) == df_factor.shape[1]] = 1/df_factor.shape[1]
            
            # 计算净值
            nav[column_index], _ = BacktestUtils.cal_nav(
                df_factor, self.indus_close[start_date:end_date], base_nav, fee=0)
            
            
        # 基准净值归一化
        nav['基准'] = base_nav / base_nav.values[0]
          
        # 计算相对净值
        nav_relative = pd.DataFrame(columns=nav.columns[0:-1])
        for column_name in nav_relative:
            nav_relative[column_name] = nav[column_name] / nav['基准'] 

        # 返回绝对净值曲线，相对净值曲线，多头持仓
        return nav, nav_relative, df_factor
    
    
    # -------------------------------------------------------------------------
    # 计算回测指标
    # [输入]
    # nav             净值
    # refresh_dates   调仓日期
    # -------------------------------------------------------------------------
    def performance(self, nav, refresh_dates):

        # 初始化结果矩阵
        perf = pd.DataFrame(index=['多头', '空头', '基准'], 
                          columns=['年化收益率','年化波动率','夏普比率','最大回撤',
                                   '年化超额收益率','超额收益年化波动率','信息比率',
                                   '超额收益最大回撤', '调仓胜率', '相对基准盈亏比'])
                
        # 计算多头收益
        long_perf = PerfUtils.excess_statis(nav['多头策略'], nav['基准'], refresh_dates)
        perf.loc['多头',:] = long_perf.loc['策略',:]
        perf.loc['基准',:] = long_perf.loc['基准',:]
        
        # 计算空头收益
        short_perf = PerfUtils.excess_statis(nav['空头策略'], nav['基准'], refresh_dates)
        perf.loc['空头',:] = short_perf.loc['策略',:]
        
        # 计算多头相对空头胜率
        ls_perf = PerfUtils.excess_statis(nav['多头策略'], nav['空头策略'], refresh_dates)
        perf.loc['多头','多空胜率'] = ls_perf.loc['策略','调仓胜率']
        
        return perf
    
    
    # -------------------------------------------------------------------------
    # 计算回测指标
    # [输入]
    # nav             净值
    # refresh_dates   调仓日期
    # -------------------------------------------------------------------------
    def multi_perf(self, nav, freq='M'):
        
        # 提取月末日期
        date_series = pd.Series(nav.index, index=nav.index)
        date_list = date_series.resample(freq).last().dropna().values        
        
        # 计算回测业绩指标        
        perf_all = []
        for target in nav.columns[0:-1]:            
            perf_all.append(PerfUtils.excess_statis(nav[target], nav['基准'], date_list).loc['策略',:])
            
        # 结果汇总
        merge_perf_all = pd.concat(perf_all, axis=1)
        merge_perf_all.columns = nav.columns[0:-1]
        
        return merge_perf_all.T


    # -------------------------------------------------------------------------
    # 根据复合指标计算做多或是做空行业
    # [输入]
    # merge_factor    复合行业景气度指标
    # indus_num       通过复合行业指标计算持仓时，需要规定选择的行业个数，默认5个
    # drop_list       需要剔除的行业数目
    # -------------------------------------------------------------------------
    def cal_ls_factor(self, merge_factor_input):
                        
        # 复制
        merge_factor = merge_factor_input.copy()

        # 初始化返回值
        merge_ls_factor = merge_factor.copy() * 0
               
        # 空头
        merge_ls_factor[merge_factor>0] = 1
        merge_ls_factor[merge_factor<0] = -1 
        
                
        return merge_ls_factor
        
    
    # -------------------------------------------------------------------------
    # 根据复合行业景气度指标计算做多或是做空行业
    # [输入]
    # merge_factor    复合行业景气度指标
    # indus_num       通过复合行业指标计算持仓时，需要规定选择的行业个数，默认5个
    # drop_list       需要剔除的行业数目
    # -------------------------------------------------------------------------
    def cal_merge_ls_factor(self, merge_factor_input, indus_num=5, drop_list=[]):
                        
        # 舍弃部分行业
        merge_factor = merge_factor_input.copy()
        merge_factor.drop(columns=drop_list,inplace=True)
        self.indus_close.drop(columns=drop_list,inplace=True)
        
        # 对复合行业景气度指标进行指数加权
        merge_factor_ewm = merge_factor.ewm(alpha=0.99).mean()
        merge_factor_ewm.loc[:, '综合金融'] = np.nan
        
        # 加权后的景气度指标进行排序
        merge_factor_rank = merge_factor_ewm.rank(method='average', ascending=False, axis=1)

        # 初始化返回值
        merge_ls_factor = merge_factor.copy() * 0
               
        # 多头持仓行业
        long_judge = (merge_factor_rank <= indus_num)
        long_judge[long_judge.sum(axis=1) > indus_num] = \
            (merge_factor_rank <= indus_num - 1)[long_judge.sum(axis=1) > indus_num] 
        merge_ls_factor[long_judge] = 1

        # 加权后的景气度指标进行排序 - 由小到大
        merge_factor_rank = merge_factor_ewm.rank(method='average', ascending=True, axis=1)

        # 空头持仓行业
        short_judge = (merge_factor_rank <= indus_num)
        short_judge[short_judge.sum(axis=1) > indus_num] = \
            (merge_factor_rank <= indus_num - 1)[short_judge.sum(axis=1) > indus_num] 
        merge_ls_factor[short_judge] = - 1
        
        merge_ls_factor[merge_ls_factor.isnull()] = 0
                
        return merge_ls_factor
    
    
    # -------------------------------------------------------------------------
    # 将给定路径下的景气度指标进行叠加
    # [输入]
    # path        需要进行复合的单项行业景气度指标存储路径
    # factors     进行复合的行业指标名称
    # cut_index   输入对于指标进行截断的时间点
    # -------------------------------------------------------------------------
    def cal_merge_factor(self, path, factors, cut_index):
        
        # 复合指标记录变量初始化
        m_factor = None
        
        # 遍历给定的单项行业景气度指标   
        for factor_name in factors:
            
            # 读取单项行业景气度指标
            df = pd.read_pickle(path + factor_name)
            
            # 将不同单项景气度指标进行叠加
            if m_factor is None:
                m_factor = df
                
            else:
                # 不同单项景气度指标时间起点有区别
                m_index = m_factor.index if len(df.index) > len(m_factor.index) else df.index
                m_factor.loc[m_index,:] = m_factor.loc[m_index,:] + df.loc[m_index,:]    
        
        return m_factor.loc[cut_index:,:]
    
    # -------------------------------------------------------------------------
    # 回测过程计算
    # [输入]
    # df_factor      行业景气度指标，一般只有1，-1，0三个数值，分别代表多头/空头/空仓
    # start_date     计算回测净值曲线开始时间
    # end_date       计算回测净值曲线终止时间
    # -------------------------------------------------------------------------
    def score(self, factor):
               

        # 数据汇总
        thres = pd.DataFrame(0, index=factor.index, 
                             columns=['多头阈值1','多头阈值2','多头阈值3',
                                      '空头阈值1','空头阈值2','空头阈值3'])

        # 遍历所有日期
        for date in factor.index:    
            
            # 当前指标
            cur_factor = factor.loc[:date, :].copy()
            cur_factor[cur_factor==0] = np.nan
            
            # 剔除空值
            cur_data = cur_factor.dropna(how='all')
            if len(cur_data) > 0:
                thres.loc[date,'多头阈值1'] = np.nanquantile(cur_factor, 0.9)
                thres.loc[date,'多头阈值2'] = np.nanquantile(cur_factor, 0.7)
                thres.loc[date,'多头阈值3'] = np.nanquantile(cur_factor, 0.5)
                thres.loc[date,'空头阈值1'] = np.nanquantile(cur_factor, 0.1)
                thres.loc[date,'空头阈值2'] = np.nanquantile(cur_factor, 0.3)
                thres.loc[date,'空头阈值3'] = np.nanquantile(cur_factor, 0.5)

                
        # 初始化返回值
        df_factor = pd.DataFrame(np.zeros_like(factor.values), 
                                 index=factor.index, columns=factor.columns)
       
        # 计算指标得分
        df_factor[factor.sub(thres.loc[:, '多头阈值3'] , axis=0) > 0] = 1
        df_factor[factor.sub(thres.loc[:, '多头阈值2'] , axis=0) > 0] = 2    
        df_factor[factor.sub(thres.loc[:, '空头阈值3'] , axis=0) < 0] = -1
        df_factor[factor.sub(thres.loc[:, '空头阈值2'] , axis=0) < 0] = -2

        # 返回因子值
        return df_factor
    
    
    
if __name__ == '__main__':        

        
# # =============================================================================
# #   单项指标测试
# # =============================================================================

#     # 模型初始化
#     model = model_backtest()
    
#     start_time = '2017-01-01'
#     end_time = '2021-09-30'
    
#     # 指标复合
#     # df_factor = pd.read_pickle('gen_indus1_factor/north_results/north_change_amount_W_orig')
#     # df_factor = pd.read_pickle('gen_indus1_factor/north_results/north_holdings_float_W_yoy')
    
#     # df_factor = pd.read_pickle('gen_indus1_factor/margin_results/margin_slend_balance_float_W_qoq')
#     df_factor = pd.read_pickle('gen_indus1_factor/margin_results/margin_tr_balance_orig_W_yoy')
   
    
#     # df_factor = pd.read_pickle('gen_indus1_factor/ETF_results/allETFselect_amount_M_yoy')    
#     # df_factor = pd.read_pickle('gen_indus1_factor/industry_results/AShareSEO_preplan_recent_orig_W')
    
#     # df_factor = pd.read_pickle('gen_indus1_factor/industry_results/AShareFreeFloatCalendar_listdt_next_orig_M')
#     # df_factor = pd.read_pickle('gen_indus1_factor/industry_results/MjrHolderTrade_under_recent_amount_W')

#     # df_factor = pd.read_pickle('gen_indus1_factor/industry_results/ASarePlanTrade_under_next_amount_M')
    
#     # 回测过程
#     [nav, nav_relative, portion_1] = model.clsfy_backtest(df_factor, start_time, end_time, base='mean')
#     # [nav, nav_relative, portion_1] = model.thres_backtest(df_factor, start_time, end_time, base='mean')
                           
#     # 计算回测业绩指标
#     clsfy_perf = model.multi_perf(nav)
    
#     # 作图
#     nav.plot()
#     nav_relative.plot()
    
    
#     # # 多空方向备份
#     # # 计算多空行业
#     # ls_factor = model.cal_ls_factor(df_factor)
    
#     # # 回测过程
#     # [nav1, _, long_portion1, _, _] = model.backtest(ls_factor, start_time, end_time, base='mean', fee=0.000)

#     # # 计算回测业绩指标
#     # ls_perf = model.performance(nav1, long_portion1.index)      
    
#     # # 作图
#     # nav1.plot()
    

# # =============================================================================
# #   遍历测试（分层测试+阈值）
# # =============================================================================
    
#     # 模型初始化
#     model = model_backtest()
    
#     # 读取路径下所有行业指标
#     # path='gen_indus1_factor/north_results/'
#     # path='gen_indus1_factor/margin_results/'
#     # path='gen_indus1_factor/ETF_results/'
#     path='gen_indus1_factor/industry_results/'
#     for root, dirs, factors in os.walk(path):
#         pass
    
#     start_time = '2017-01-01'
#     end_time = '2021-09-30'
          
#     for factor_name in factors:
        
#         print(factor_name)

#         # 记录指标数据
#         df_factor = pd.read_pickle(path + factor_name)
                
#         # 回测过程
#         [nav, nav_relative, portion_1] = model.clsfy_backtest(df_factor, start_time, end_time,  base='mean')
                      
#         # 计算回测业绩指标
#         clsfy_perf = model.multi_perf(nav)
        
#         # 信号覆盖度
#         indus_portion = (portion_1.iloc[:,:-1]>0).sum(axis=0) / portion_1.shape[0]
#         clsfy_perf.loc[clsfy_perf.index[0], '行业覆盖率'] = (indus_portion > 0.1).sum() / portion_1.shape[1]
        

#         # 回测过程
#         [nav_long, _, _] = model.thres_backtest(df_factor, start_time, end_time, base='mean')
                                     
#         # 计算回测业绩指标
#         thres_perf = model.multi_perf(nav_long)
                

#         # 数据汇总
#         perf = pd.concat([clsfy_perf, thres_perf], axis=0)
                
#         # 存储回测结果
#         perf.to_excel('results/一级行业_{}.xlsx'.format(factor_name))

    

# # =============================================================================
# #   分析结果
# # =============================================================================
    
#     # 读取路径下所有行业指标
#     # path='gen_indus1_factor/north_results/' 
#     # path='gen_indus1_factor/margin_results/'
#     # path='gen_indus1_factor/ETF_results/'   
#     path='gen_indus1_factor/industry_results/'   
#     for root, dirs, factors in os.walk(path):
#         pass
                
#     # 指标业绩汇总（分层1数据更大，即资金流入更明显）
#     merge_perf = pd.DataFrame(index = factors,
#                 columns = ['分层1', '分层2', '分层3', '分层4', '分层5', '分层1-分层5',
#                             '多头阈值1','多头阈值2','多头阈值3',
#                             '空头阈值1','空头阈值2','空头阈值3','行业覆盖率'])
    
#     for factor_name in factors:

#         # 读取回测结果
#         perf = pd.read_excel('results/一级行业_{}.xlsx'.format(factor_name), index_col=0)
        
#         # 指标信号
#         merge_perf.loc[factor_name,:].iloc[:-1] = perf.loc[:,'年化超额收益率']

#         # 行业覆盖率
#         merge_perf.loc[factor_name,'行业覆盖率'] = perf.loc['分层1','行业覆盖率']
    
#     # 分层超额
#     merge_perf['分层1-分层5'] = merge_perf['分层1'] - merge_perf['分层5']
    
#     # 名称拼接    
#     name_list = pd.read_excel('结果整理/增减持指标名称.xlsx', index_col=0)    
#     merge_data = pd.concat([name_list, merge_perf], axis=1)
    
#     # 结果存储
#     merge_data.to_excel('结果整理/一级行业-增减持-2017.xlsx')
    
    

# #     # ETF_money_amount_M_qoq
# #     # ETF_money_amount_W_qoq          
# #     # north_change_orig_W_orig
# #     # north_change_normal_M_yoy
# #     # margin_tr_balance_orig_M_yoy
# #     # margin_balance_orig_M_yoy
# #     # AShareMjrHolderTrade_减持_recent_amount_W
# #     # AShareInsiderTrade_增持_recent_amount_W
# #     # AShareInsiderTrade_增持_recent_amount_W


# =============================================================================
#   单项指标测试
# =============================================================================

    # 模型初始化
    model = model_backtest()
    
    start_time = '2017-01-01'
    end_time = '2021-11-05'

    
    money_list = ['north_holdings_float_W_yoy',
                  'north_change_amount_W_orig',
                  'margin_tr_balance_orig_W_yoy',
                  'AShareSEO_preplan_recent_orig_W',
                  'MjrHolderTrade_under_recent_amount_W']
    
    # money_list = ['north_holdings_float_M_yoy',
    #               'margin_tr_balance_orig_M_yoy',
    #               'allETFselect_amount_M_yoy',
    #               'AShareFreeFloatCalendar_listdt_next_orig_M']

    cur_score = 0
    
    for index in money_list:
        
        df_factor = pd.read_pickle('gen_indus1_factor/results/{}'.format(index))
        df_score = model.score(df_factor)
        
        if index in ['MjrHolderTrade_under_recent_amount_W', 
                      'AShareFreeFloatCalendar_listdt_next_orig_M']:            
            df_score = - df_score
            df_score[df_score>0] = 0
            
        if index in ['AShareSEO_preplan_recent_orig_W']:            
            df_score[df_score<0] = 0
            
        # print(index, df_score.iloc[-1,:])
        cur_score = cur_score + df_score
    
    cur_score = cur_score.loc['2016-01-01':,:]
    
    # # 资金面指标
    # cur_score = (cur_score + 6) / 7 - 1
    # cur_score.loc[:'2016-01-01',:] = 0
    
    # # 景气度指标
    # groom_factor = pd.read_excel('景气度指标.xlsx', index_col=0)  /  17  
    
    # groom_factor = groom_factor.resample('D').ffill()
    # groom_factor = groom_factor.reindex(index=cur_score.index)

    # # [nav, nav_relative, portion_1] = model.clsfy_backtest(
    # #         cur_score + groom_factor, start_time, end_time, base='mean')
        
    # # # 计算回测业绩指标
    # # clsfy_perf = model.multi_perf(nav, 'M')
    
    # # # 作图
    # # nav.plot()
    # # nav_relative.plot()
                           
    
    # ls_factor = model.cal_merge_ls_factor(groom_factor + cur_score)
    # # groom_factor = model.cal_merge_ls_factor(groom_factor)
    
    # # groom_factor[ls_factor>0] = groom_factor*2
    # # groom_factor[ls_factor<0] = groom_factor/2
        
    
    # # 回测过程
    # [nav1, nav_relative1, long_portion1, short_portion1, turn_long1, turn_short1] = \
    #     model.backtest(ls_factor, start_time, end_time, base='mean', fee=0.000)

    # # 计算回测业绩指标
    # ls_perf = model.multi_perf(nav1, 'M')
    # nav_relative1.plot()








