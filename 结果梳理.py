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



if __name__ == '__main__':        
    
# # =============================================================================
# # 北向资金名称
# # =============================================================================

#     # 读取路径下所有行业指标
#     path='gen_indus1_factor/north_results/'
#     for root, dirs, factors in os.walk(path):
#         pass
        
#     data_sum = pd.DataFrame(index=factors, 
#             columns=['数据类型','归一化方法','信号频率','信号生成方式'])


#     for factor_name in factors:
        

#         if factor_name.split('_')[1] == 'change':
#             data_sum.loc[factor_name, '数据类型'] = '北向资金流入额' 
             
#         elif factor_name.split('_')[1] == 'holdings':
#             data_sum.loc[factor_name, '数据类型'] = '北向资金持股市值' 


#         if factor_name.split('_')[2] == 'orig':
#             data_sum.loc[factor_name, '归一化方法'] = '原始资金数据' 
             
#         elif factor_name.split('_')[2] == 'amount':
#             data_sum.loc[factor_name, '归一化方法'] = '除以成交额' 
            
#         elif factor_name.split('_')[2] == 'float':
#             data_sum.loc[factor_name, '归一化方法'] = '除以流通市值' 
            
            
#         if factor_name.split('_')[3] == 'W':
#             data_sum.loc[factor_name, '信号频率'] = '周度' 
             
#         elif factor_name.split('_')[3] == 'M':
#             data_sum.loc[factor_name, '信号频率'] = '月度' 
            

            
#         if factor_name.split('_')[4] == 'orig':
#             data_sum.loc[factor_name, '信号生成方式'] = '原始值' 
             
#         elif factor_name.split('_')[4] == 'yoy':
#             data_sum.loc[factor_name, '信号生成方式'] = '同比' 
            
#         elif factor_name.split('_')[4] == 'qoq':
#             data_sum.loc[factor_name, '信号生成方式'] = '环比' 
            
#     data_sum.to_excel('结果整理/北向资金名称.xlsx')
        


# # =============================================================================
# # 两融资金名称
# # =============================================================================

#     # 读取路径下所有行业指标
#     path='gen_indus1_factor/margin_results/'
#     for root, dirs, factors in os.walk(path):
#         pass
        
#     data_sum = pd.DataFrame(index=factors, 
#             columns=['数据类型','归一化方法','信号频率','信号生成方式'])
            
#     # 字典映射关系
#     dict_map = {'slend_balance':"融券余额", 'tr_balance':"融资余额",
#                 'borrow':"融资买入额", 'repay':"融资偿还额",  'net_buy':"净融资"}            
    
#     # 归一化方法
#     normal_method = {'orig':'原始资金数据', 'float':'除以流通市值', 'amount':'除以成交额'}
    
#     # 频率信号
#     freq_change = {'W':'周度',  'M':'月度'}
    
#     # 信号变换
#     data_change = {'orig':'原始值' , 'qoq':'环比' , 'yoy':'同比'}
    

#     for factor_name in factors:
        
#         for index in dict_map.keys():
            
#             if factor_name.find(index) > -1:
#                 data_sum.loc[factor_name, '数据类型'] = dict_map[index]
#                 continue
        
#         # data_sum.loc[factor_name, '数据类型'] = dict_map[factor_name.split('_')[1]]
#         data_sum.loc[factor_name, '归一化方法'] = normal_method[factor_name.split('_')[-3]]
#         data_sum.loc[factor_name, '信号频率'] = freq_change[factor_name.split('_')[-2]]
#         data_sum.loc[factor_name, '信号生成方式'] = data_change[factor_name.split('_')[-1]]
        
#     data_sum.to_excel('结果整理/两融资金名称.xlsx')
        




# # =============================================================================
# # 两融资金名称
# # =============================================================================

#     # 读取路径下所有行业指标
#     path='gen_indus1_factor/ETF_results/'
#     for root, dirs, factors in os.walk(path):
#         pass
        
#     data_sum = pd.DataFrame(index=factors, 
#             columns=['数据类型','归一化方法','信号频率','信号生成方式'])
            
#     # 字典映射关系
#     dict_map = {'allETF':"全市场ETF", 'indusETF':"行业主题ETF",
#                 'allETFselect':"全市场ETF筛选", 'indusETFselect':"行业主题ETF筛选"}            
    
#     # 归一化方法
#     normal_method = {'orig':'原始资金数据', 'float':'除以流通市值', 'amount':'除以成交额'}
    
#     # 频率信号
#     freq_change = {'W':'周度',  'M':'月度'}
    
#     # 信号变换
#     data_change = {'orig':'原始值' , 'qoq':'环比' , 'yoy':'同比'}
    

#     for factor_name in factors:
        
#         for index in dict_map.keys():
            
#             if factor_name.find(index) > -1:
#                 data_sum.loc[factor_name, '数据类型'] = dict_map[index]
#                 continue
        
#         # data_sum.loc[factor_name, '数据类型'] = dict_map[factor_name.split('_')[1]]
#         data_sum.loc[factor_name, '归一化方法'] = normal_method[factor_name.split('_')[-3]]
#         data_sum.loc[factor_name, '信号频率'] = freq_change[factor_name.split('_')[-2]]
#         data_sum.loc[factor_name, '信号生成方式'] = data_change[factor_name.split('_')[-1]]
        
#     data_sum.to_excel('结果整理/ETF资金名称.xlsx')
        




# # =============================================================================
# # 产业资本名称 - 定向增发
# # =============================================================================

#     # 读取路径下所有行业指标
#     path='gen_indus1_factor/industry_results/'
#     for root, dirs, factors in os.walk(path):
#         pass
        
#     data_sum = pd.DataFrame(index=factors, 
#             columns=['数据类型','日期参数', '归一化方法','信号频率'])
            
#     # 字典映射关系
#     dict_map = {'AShareSEO':"定向增发"}            
    
#     # 信号变换
#     factor_date = {'preplan':'预案公告日' , 'pass':'审核通过日' , 'offering':'增发公告日'}                    
    
#     # 归一化方法
#     normal_method = {'orig':'原始资金数据', 'float':'除以流通市值', 'amount':'除以成交额'}
    
#     # 频率信号
#     freq_change = {'W':'周度',  'M':'月度'}
    

#     for factor_name in factors:
        
#         for index in dict_map.keys():
            
#             if factor_name.find(index) > -1:
#                 data_sum.loc[factor_name, '数据类型'] = dict_map[index]
#                 continue
        
#         # data_sum.loc[factor_name, '数据类型'] = dict_map[factor_name.split('_')[1]]
#         data_sum.loc[factor_name, '日期参数'] = factor_date[factor_name.split('_')[-4]]
#         data_sum.loc[factor_name, '归一化方法'] = normal_method[factor_name.split('_')[-2]]
#         data_sum.loc[factor_name, '信号频率'] = freq_change[factor_name.split('_')[-1]]
        
#     data_sum.to_excel('结果整理/定向增发指标名称.xlsx')
    


# # =============================================================================
# # 产业资本名称 - 限售解禁
# # =============================================================================

#     # 读取路径下所有行业指标
#     path='gen_indus1_factor/industry_results/'
#     for root, dirs, factors in os.walk(path):
#         pass
        
#     data_sum = pd.DataFrame(index=factors, 
#             columns=['数据类型', '日期参数', '归一化方法','信号频率'])
            
#     # 字典映射关系
#     dict_map = {'AShareFreeFloatCalendar':"限售解禁"}            
    
#     # 信号变换
#     factor_date = {'anndt':'公告日期' , 'listdt':'限售股上市日期' }                    
    
#     # 归一化方法
#     normal_method = {'orig':'原始资金数据', 'float':'除以流通市值', 'amount':'除以成交额'}
    
#     # 频率信号
#     freq_change = {'W':'周度',  'M':'月度'}
    

#     for factor_name in factors:
        
#         for index in dict_map.keys():
            
#             if factor_name.find(index) > -1:
#                 data_sum.loc[factor_name, '数据类型'] = dict_map[index]
#                 continue
        
#         # data_sum.loc[factor_name, '数据类型'] = dict_map[factor_name.split('_')[1]]
#         data_sum.loc[factor_name, '日期参数'] = factor_date[factor_name.split('_')[-4]]
#         data_sum.loc[factor_name, '归一化方法'] = normal_method[factor_name.split('_')[-2]]
#         data_sum.loc[factor_name, '信号频率'] = freq_change[factor_name.split('_')[-1]]
        
#     data_sum.to_excel('结果整理/限售解禁指标名称.xlsx')
    


# # =============================================================================
# # 产业资本名称 - 回购
# # =============================================================================

#     # 读取路径下所有行业指标
#     path='gen_indus1_factor/industry_results/'
#     for root, dirs, factors in os.walk(path):
#         pass
        
#     data_sum = pd.DataFrame(index=factors, 
#             columns=['数据类型', '日期参数', '归一化方法','信号频率'])
            
#     # 字典映射关系
#     dict_map = {'AshareStockRepo':"回购"}  
          
#     # 信号变换
#     factor_date = {'preplan':'董事会预案' , 'pass':'股东大会通过', 'conduct':'回购实施' }                    
    
#     # 归一化方法
#     normal_method = {'orig':'原始资金数据', 'float':'除以流通市值', 'amount':'除以成交额'}
    
#     # 频率信号
#     freq_change = {'W':'周度',  'M':'月度'}
    

#     for factor_name in factors:
        
#         for index in dict_map.keys():
            
#             if factor_name.find(index) > -1:
#                 data_sum.loc[factor_name, '数据类型'] = dict_map[index]
#                 continue
        
#         # data_sum.loc[factor_name, '数据类型'] = dict_map[factor_name.split('_')[1]]
#         data_sum.loc[factor_name, '日期参数'] = factor_date[factor_name.split('_')[-4]]
#         data_sum.loc[factor_name, '归一化方法'] = normal_method[factor_name.split('_')[-2]]
#         data_sum.loc[factor_name, '信号频率'] = freq_change[factor_name.split('_')[-1]]
        
#     data_sum.to_excel('结果整理/回购指标名称.xlsx')





# # =============================================================================
# # 产业资本名称 - 大股东减持
# # =============================================================================

#     # 读取路径下所有行业指标
#     path='gen_indus1_factor/industry_results/'
#     for root, dirs, factors in os.walk(path):
#         pass
        
#     data_sum = pd.DataFrame(index=factors, 
#             columns=['数据类型', '日期参数', '归一化方法','信号频率'])
            
#     # 字典映射关系
#     dict_map = {'MjrHolderTrade':"股东增减持"}  
          
#     # 信号变换
#     factor_date = {'over':'增持' , 'under':'减持'}                    
    
#     # 归一化方法
#     normal_method = {'orig':'原始资金数据', 'float':'除以流通市值', 'amount':'除以成交额'}
    
#     # 频率信号
#     freq_change = {'W':'周度',  'M':'月度'}
    
#     for factor_name in factors:
        
#         for index in dict_map.keys():
            
#             if factor_name.find(index) > -1:
#                 data_sum.loc[factor_name, '数据类型'] = dict_map[index]
#                 continue
        
#         # data_sum.loc[factor_name, '数据类型'] = dict_map[factor_name.split('_')[1]]
#         data_sum.loc[factor_name, '日期参数'] = factor_date[factor_name.split('_')[-4]]
#         data_sum.loc[factor_name, '归一化方法'] = normal_method[factor_name.split('_')[-2]]
#         data_sum.loc[factor_name, '信号频率'] = freq_change[factor_name.split('_')[-1]]
        
#     data_sum.to_excel('结果整理/增减持指标名称.xlsx')




# # =============================================================================
# # 产业资本名称 - 增减持计划
# # =============================================================================

#     # 读取路径下所有行业指标
#     path='gen_indus1_factor/industry_results/'
#     for root, dirs, factors in os.walk(path):
#         pass
        
#     data_sum = pd.DataFrame(index=factors, 
#             columns=['数据类型', '日期参数', '归一化方法','信号频率'])
            
#     # 字典映射关系
#     dict_map = {'ASarePlanTrade':"股东增减持"}  
          
#     # 信号变换
#     factor_date = {'over':'增持' , 'under':'减持'}                    
    
#     # 归一化方法
#     normal_method = {'orig':'原始资金数据', 'float':'除以流通市值', 'amount':'除以成交额'}
    
#     # 频率信号
#     freq_change = {'W':'周度',  'M':'月度'}
    
#     for factor_name in factors:
        
#         for index in dict_map.keys():
            
#             if factor_name.find(index) > -1:
#                 data_sum.loc[factor_name, '数据类型'] = dict_map[index]
#                 continue
        
#         # data_sum.loc[factor_name, '数据类型'] = dict_map[factor_name.split('_')[1]]
#         data_sum.loc[factor_name, '日期参数'] = factor_date[factor_name.split('_')[-4]]
#         data_sum.loc[factor_name, '归一化方法'] = normal_method[factor_name.split('_')[-2]]
#         data_sum.loc[factor_name, '信号频率'] = freq_change[factor_name.split('_')[-1]]
        
#     data_sum.to_excel('结果整理/拟增减持指标名称.xlsx')






# index_close = pd.read_pickle('data/daily/index/index_close')
# fund_share = pd.read_pickle("data/daily/fund/FUNDSHARE")

# SZ_50 = pd.concat([index_close.loc[:, '000016.SH'], fund_share.loc[:, '510050.OF']], axis=1).resample('M').last()

# # SZ_50 = pd.concat([index_close.loc[:, '000016.SH'], model.float_share.loc[:, '510050.OF']], axis=1)
    
    

# # =============================================================================
# #  两融资金指标筛选
# # =============================================================================
    
#     import pandas as pd
    
#     # 结果存储
#     data_1 = pd.read_excel('结果整理/两融/一级行业-两融-2017.xlsx', index_col=0)
#     data_1_backup = data_1.copy()
#     data_2 = pd.read_excel('结果整理/两融/二级行业-两融-2017.xlsx', index_col=0)
#     data_2_backup = data_2.copy()
    
#     # # 修正反向指标
#     # index_a = data_1[data_1['数据类型'] == "融券余额"].index.tolist()
#     # index_b = data_1[data_1['数据类型'] == "融资偿还额"].index.tolist()
    
#     # merge_index = index_a + index_b
    
    
#     # data_1.loc[merge_index, '分层1-分层5'] = - data_1.loc[merge_index, '分层1-分层5']
#     # data_2.loc[merge_index, '分层1-分层5'] = - data_2.loc[merge_index, '分层1-分层5']
#     # data_1.loc[merge_index, ['多头阈值1', '多头阈值2', '多头阈值3']] = - data_1.loc[merge_index, ['多头阈值1', '多头阈值2', '多头阈值3']]
#     # data_2.loc[merge_index, ['多头阈值1', '多头阈值2', '多头阈值3']] = - data_2.loc[merge_index, ['多头阈值1', '多头阈值2', '多头阈值3']]
#     # data_1.loc[merge_index, ['空头阈值1', '空头阈值2', '空头阈值3']] = - data_1.loc[merge_index, ['空头阈值1', '空头阈值2', '空头阈值3']]
#     # data_2.loc[merge_index, ['空头阈值1', '空头阈值2', '空头阈值3']] = - data_2.loc[merge_index, ['空头阈值1', '空头阈值2', '空头阈值3']]
    
    
#     data_1 = data_1[data_1['行业覆盖率'] > 0.5]
#     data_1 = data_1[(data_1['多头阈值1'] > 0) | (data_1['多头阈值2'] > 0)]
#     data_1 = data_1[(data_1['空头阈值1'] < 0) | (data_1['空头阈值2'] < 0)]
    
#     data_1_screeen = data_1[data_1['分层1-分层5'] > 0.02]
    
    
#     data_2 = data_2[data_2['行业覆盖率'] > 0.5]
#     data_2 = data_2[(data_2['多头阈值1'] > 0) | (data_2['多头阈值2'] > 0)]
#     data_2 = data_2[(data_2['空头阈值1'] < 0) | (data_2['空头阈值2'] < 0)]
    
#     data_2_screeen = data_2[data_2['分层1-分层5'] > 0.02]
    
    
#     final_list = (set(data_1_screeen.index) & set(data_2_screeen.index))
#     data_1_screeen = data_1_backup.loc[final_list, :]
#     data_2_screeen = data_2_backup.loc[final_list, :]
    




# # =============================================================================
# #  ETF资金指标筛选
# # =============================================================================
    
#     import pandas as pd
    
#     # 结果存储
#     data_1 = pd.read_excel('结果整理/一级行业-ETF-2017.xlsx', index_col=0)
#     data_1_backup = data_1.copy()
#     data_2 = pd.read_excel('结果整理/二级行业-ETF-2017.xlsx', index_col=0)
#     data_2_backup = data_2.copy()
    
    
#     data_1 = data_1[data_1['行业覆盖率'] > 0.5]
#     data_1 = data_1[(data_1['多头阈值1'] > 0) | (data_1['多头阈值2'] > 0)]
#     data_1 = data_1[(data_1['空头阈值1'] < 0) | (data_1['空头阈值2'] < 0)]
    
#     data_1_screeen = data_1[data_1['分层1-分层5'] > 0.05]
    
    
#     data_2 = data_2[data_2['行业覆盖率'] > 0.5]
#     data_2 = data_2[(data_2['多头阈值1'] > 0) | (data_2['多头阈值2'] > 0)]
#     data_2 = data_2[(data_2['空头阈值1'] < 0) | (data_2['空头阈值2'] < 0)]
    
#     data_2_screeen = data_2[data_2['分层1-分层5'] > 0.05]
    
    
#     final_list = (set(data_1_screeen.index) & set(data_2_screeen.index))
#     data_1_screeen = data_1_backup.loc[final_list, :]
#     data_2_screeen = data_2_backup.loc[final_list, :]
    

                



