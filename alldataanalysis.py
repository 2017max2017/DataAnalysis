import pandas as pd
import pyodbc
from openpyxl import load_workbook
from datetime import datetime
import os

# 数据库连接信息
server = 'www.hzblwz.com,1533'
database = 'rxerpmygljtb'
username = 'hzblwz'
password = 'Hzblwz@240607'

# 创建数据库连接字符串
connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'

# 定义批次大小
batch_size = 1000

# 读取公司名称列表
file_path = '/home/user/data/公司名称0727.xlsx'  # 文件路径


data = pd.read_excel(file_path, sheet_name='Sheet1', dtype={'Company Name': str})

# 假设 data 是你的 DataFrame
selected_data = data['Company Name']  # 选择部分数据

# 将所有批次结果存储在列表中
all_results = []
all_comb_results = []
all_avg_sales = []

# SQL 查询函数
def 应收款明细(ysk_date_s, ysk_date_e, company_name, connection_string):
    sql_query = f"""
    SELECT FGS,KHBH,KHMC,KHBH1,KHMC1,RQ,DJLX,
    YSK,SKZJE,YE1,DH,PZH,PZLX,KHLX,HBLB,SHRQ,BZ,ZY,HSSM,WFHJE,YWY1,DEPT1,DWDM FROM (
    SELECT FGS,KHBH,KHMC,KHBH AS KHBH1,KHMC AS KHMC1,RQ,DJLX,
    YSK,SKZJE,0.00 YE1,DH,PZH,PZLX,DBO.F_GET_KHLX(FGS,KHBH) KHLX,HBLB,SHRQ,BZ,
    CASE WHEN DJLX='2' THEN CONVERT(VARCHAR(255),STUFF((SELECT ','+ISNULL(BEIZHU,'') FROM WP_DJMX T WHERE T.DJXH = V_YSK.DH GROUP BY ISNULL(BEIZHU,'') FOR XML PATH('')), 1, 1, '')) ELSE '' END ZY,
    CASE WHEN DJLX='2' THEN (SELECT CASE WHEN SUM(WSJE) = 0 AND SUM(HSJE) = 0 THEN ''
    WHEN SUM(WSJE) = 0 THEN '含税'
    WHEN SUM(HSJE) = 0 THEN '不含税'
    ELSE '含税:' + CONVERT(VARCHAR(20),SUM(HSJE)) + '/不含税:' + CONVERT(VARCHAR(20),SUM(WSJE)) END FROM (SELECT DH,CASE WHEN ISNULL(T.SHUILU,0) =0 THEN SUM(T.ZJE) ELSE 0 END WSJE,
    CASE WHEN ISNULL(T.SHUILU,0) = 0 THEN 0 ELSE SUM(T.ZJE) END HSJE FROM WP_DJMX T WHERE T.DJXH = V_YSK.DH
    GROUP BY T.DJXH,T.SHUILU) A WHERE A.DH = V_YSK.DH) WHEN DJLX='7' THEN
    (SELECT MC FROM WP_ZDGL WHERE V_YSK.YHZH = WP_ZDGL.DM) ELSE '' END HSSM,WFHJE,DEPT DEPT1,YWY YWY1,DWDM DWDM
    FROM V_YSK WITH (NOLOCK) WHERE (FGS = '|^|04080001|^|04080002|^|04080003|^|04080004|^|04080005|^|04080006|^|04080007|^|04080008|^|04080009|^|04080010|^|' OR CHARINDEX(FGS,'|^|04080001|^|04080002|^|04080003|^|04080004|^|04080005|^|04080006|^|04080007|^|04080008|^|04080009|^|04080010|^|')>0) AND ((YWFLAG = 1 AND 1 =1 ) OR (YWFLAG = 2 AND 1 =2 ) OR (YWFLAG = 1 AND 1 =3 AND SHBZ = 1) OR (YWFLAG = 2 AND 1 =4 AND SHBZ = 1))
    AND ((SUBSTRING(CONVERT(CHAR,RQ,112),1,8) BETWEEN '{ysk_date_s}' AND '{ysk_date_e}' AND 1 = 1)
    OR (SUBSTRING(CONVERT(CHAR,RQ,112),1,6) BETWEEN '{ysk_date_s}' AND '{ysk_date_e}' AND 1 = 2) OR (SUBSTRING(CONVERT(CHAR,RQ,112),1,4) BETWEEN '{ysk_date_s}' AND '{ysk_date_e}' AND 1 = 3))) UAP where khbh='{company_name}' and hblb ='人民币'
    """
    
    try:
        with pyodbc.connect(connection_string) as conn:
            # 执行 SQL 查询并加载数据到 DataFrame
            df = pd.read_sql_query(sql_query, conn)

            # 显示 DataFrame 的前 10 行
            print(df['KHMC'].value_counts())
            df = df.sort_values(by='RQ', ascending=True)
            df = df[['FGS', 'KHBH', 'KHMC', 'RQ', 'DJLX', 'YSK','SKZJE']]
            df['YE'] = 0

            print(df.head(10))
            return df

    except Exception as e:
        print(f"执行查询或文件操作时发生错误: {e}")
        return None

def 应收款余额(ysk_date, company_name, connection_string):
    ysk_date2 = f"{ysk_date} 23:59:59.00"
    ysk_date2 = datetime.strptime(ysk_date2, "%Y-%m-%d %H:%M:%S.%f")
    ysk_date = f"{ysk_date}T23:59:59.000"
    
    sql_query = f"""
    SELECT FGS,GHDW,GHDWMC,YFK,YSK,SJYSK,DEPT,YFKLX,YSKLX,DWLX,HBLB,INFO,BBJE,DWYWY,SBYWYWY FROM (
    SELECT A.FGS, A.GHDW,A.GHDWMC,SUM(A.YFK) YFK,SUM(A.YSK) YSK,SUM(A.SJYSK) SJYSK,A.DEPT,SUM(A.YFKLX) YFKLX,SUM(A.YSKLX) YSKLX,
    B.DWLX,DBO.F_GET_YSKINFO(A.FGS,A.GHDW,1) INFO,HBLB,SUM(A.BBJE) BBJE,B.YWY DWYWY,B.SBYWYWY FROM (
    SELECT A.FGS, A.GHDW,A.GHDWMC,SUM(A.YFK-A.FKZJE) YFK,0 YSK,0 SJYSK,A.DEPT,
    CASE WHEN 0 = 1 THEN DBO.F_GET_YFKLX(FGS,GHDW,DEPT,'','') ELSE 0 END YFKLX,0 YSKLX,HBLB,SUM(BBJE*-1) BBJE FROM (
    SELECT B.RQ,B.FGS,B.GHDW,B.GHDWMC,B.YFK,B.FKZJE,
    CASE WHEN (SELECT SFQY FROM GY_JTQXKZLB WHERE DM = 'DEPT') = 1 AND ISNULL((SELECT CSZ2 FROM WP_XTCS WHERE CSDM = 'YSYFQYBMQF'),0) <> 1 THEN B.DEPT ELSE '' END DEPT,B.HBLB,B.BBJE
    FROM V_YFK B WITH (NOLOCK) ) A
    WHERE A.RQ <='{ysk_date}' GROUP BY A.FGS,A.GHDW,A.GHDWMC,A.DEPT,A.HBLB
    UNION ALL
    SELECT A.FGS, A.KHBH,A.KHMC, 0 YFK,SUM(A.YSK-A.SKZJE) YSK,
    SUM(CASE WHEN A.DZFLAG=1 THEN A.YSK-A.SKZJE ELSE 0 END) SJYSK,A.DEPT,0,
    CASE WHEN 0 = 1 THEN DBO.F_GET_YSKLX(FGS,KHBH,DEPT,'','') ELSE 0 END,HBLB,SUM(BBJE) BBJE FROM (
    SELECT B.RQ,B.FGS,B.KHBH,B.KHMC,B.YSK,B.SKZJE,B.DZFLAG, CASE WHEN (SELECT SFQY FROM GY_JTQXKZLB WHERE DM = 'DEPT') = 1 AND ISNULL((SELECT CSZ2 FROM WP_XTCS WHERE CSDM = 'YSYFQYBMQF'),0) <> 1 THEN B.DEPT ELSE '' END DEPT,B.HBLB,B.BBJE
    FROM V_YSK B WITH (NOLOCK)) A
    WHERE A.RQ <='{ysk_date}'
    GROUP BY A.FGS,A.KHBH,A.KHMC,A.DEPT,A.HBLB
    ) A LEFT JOIN WP_DWGL B ON A.GHDW =B.DWDM
    GROUP BY A.FGS,A.GHDW,A.GHDWMC,A.DEPT,A.HBLB,B.DWLX,B.YWY,B.SBYWYWY
    ) UAP where ghdw='{company_name}' And ((exists (select lxdm from v_jtqxkz_sjqx where czy = '010' and lxdm=ghdw and qxlx = 'dwxx' and cxqx = 1) and ghdw >'') or isnull(ghdw,'') = '') and (yfk <>0 or ysk <>0 or sjysk <>0) And (CharIndex(Isnull(fgs,''),'|^|04080001|^|04080002|^|04080003|^|04080004|^|04080005|^|04080006|^|04080007|^|04080008|^|04080009|^|04080010|^|')>0)
    """
    
    
    try:
        with pyodbc.connect(connection_string) as conn:
            # 执行 SQL 查询并加载数据到 DataFrame
            df2 = pd.read_sql_query(sql_query, conn)
            df2 = df2[['FGS', 'GHDW', 'GHDWMC', 'SJYSK']]
            df2.rename(columns={'GHDW': 'KHBH', 'GHDWMC': 'KHMC', 'SJYSK':'YE'}, inplace=True)
            df2['RQ'] = ysk_date2
            df2['DJLX'] = 99

            # 显示 DataFrame 的前 10 行
            print(df2.head(10))
            return df2

    except Exception as e:
        print(f"执行查询或文件操作时发生错误: {e}")
        return None


# 使用示例
ysk_date = '2023-01-01'
ysk_date_s = '20230101'
ysk_date_e = '20240804'

fgs_numbers = [
    '04080001', '04080002', '04080003', '04080004', 
    '04080005', '04080006', '04080007', '04080008',
    '04080009', '04080010'
]

# 创建空白DataFrame
columns_order = ['Date'] + fgs_numbers + ['KHMC']

results_df = pd.DataFrame()
comb_result = pd.DataFrame(columns=columns_order)
avg_sale = pd.DataFrame(columns=columns_order)

# 批次处理 selected_data
for i in range(0, len(selected_data), batch_size):
    batch_data = selected_data[i:i + batch_size]
    print(f"Processing batch {i // batch_size + 1} with {len(batch_data)} companies.")

    for company_name in batch_data:
        print(company_name)
        df = 应收款明细(ysk_date_s, ysk_date_e, company_name, connection_string)
        df2 = 应收款余额(ysk_date, company_name, connection_string)
        
        # 重命名 df2 的列以匹配其他数据框
        merged_df = pd.concat([df, df2], ignore_index=True)
        merged_df['RQ_day'] = pd.to_datetime(merged_df['RQ'])  # 确保 RQ_day 是 datetime 格式
        merged_df['RQ'] = merged_df['RQ_day'].dt.strftime('%Y-%m-%d')
        print(merged_df)

        # 按照 FGS, KHMC 和 RQ 分组，计算 YSK 和 SKZJE 的总和
        data_grouped = merged_df.groupby(['FGS', 'KHMC', 'RQ', 'DJLX']).agg({
            'YSK': 'sum',
            'SKZJE': 'sum',
            'YE': 'first',
            'RQ_day': 'last'
        }).reset_index()

        # 按 FGS 和 RQ 排序
        data_grouped = data_grouped.sort_values(by=['FGS', 'RQ_day'])
        print(data_grouped)

        # 计算每个 FGS 组的 YE
        def compute_ye(group):
            previous_ye = 0  # 默认初始 YE 为 0
            for index, row in group.iterrows():
                if index == group.index[0] and row['DJLX'] == 99:
                    # 如果是第一行且 DJLX 为 99，则不计算
                    group.at[index, 'Computed_YE'] = row['YE']
                else:
                    # 对后续行进行计算
                    group.at[index, 'Computed_YE'] = row['YSK'] - row['SKZJE'] + previous_ye
                previous_ye = group.at[index, 'Computed_YE']
            return group

        # 应用 compute_ye 函数到每个 FGS 组
        data_grouped = data_grouped.groupby(['FGS', 'KHMC']).apply(compute_ye)
        results_df = pd.concat([results_df, data_grouped], ignore_index=True)
        print(results_df)

        data_grouped.set_index('RQ_day', inplace=True)

        # 初始化一个空的 DataFrame 来存储结果
        monthly_avg_ysk = pd.DataFrame()

        # 按 FGS 和 KHMC 分组，然后对每个组按月重新采样并计算 YSK 的平均值
        for (fgs, khmc), group in data_grouped.groupby(['FGS', 'KHMC']):
            monthly_avg = group['YSK'].resample('M').sum()  # 使用'M'或'ME'取决于 Pandas 版本
            monthly_avg_ysk = pd.concat([monthly_avg_ysk, monthly_avg.rename(fgs)], axis=1)
        monthly_avg_ysk['KHMC'] = company_name

        # 重置索引后将结果打印出来
        monthly_avg_ysk.reset_index(inplace=True)
        monthly_avg_ysk.columns = ['Date'] + data_grouped['FGS'].unique().tolist() + ['KHMC']
        avg_sale = pd.concat([avg_sale, monthly_avg_ysk], ignore_index=True)
        print(avg_sale)

        # 初始化一个空的 DataFrame 来存储结果
        monthly_avg_ye = pd.DataFrame()

        # 按 FGS 和 KHMC 分组处理数据
        for (fgs, khmc), group in data_grouped.groupby(['FGS', 'KHMC']):
            # 按日重新采样，填充 Computed_YE
            daily_filled = group['Computed_YE'].resample('D').ffill()
            # 计算每月平均值，使用'ME'代替'M'
            monthly_avg = daily_filled.resample('M').mean()
            # 将结果加入到 monthly_avg_ye
            monthly_avg_ye = pd.concat([monthly_avg_ye, monthly_avg.rename(fgs)], axis=1)
        monthly_avg_ye['KHMC'] = company_name

        # 处理完毕后重置索引
        monthly_avg_ye.reset_index(inplace=True)
        monthly_avg_ye.columns = ['Date'] + data_grouped['FGS'].unique().tolist() + ['KHMC']  # 更新列名为 FGS 编号

        comb_result = pd.concat([comb_result, monthly_avg_ye], ignore_index=True)
        print(comb_result)

    # 保存每个批次的结果
    file_path_batch = f'/home/DataAnalysis/查询结果_批次{i // batch_size + 1}.xlsx'
    with pd.ExcelWriter(file_path_batch, engine='openpyxl') as writer:
        results_df.to_excel(writer, sheet_name='应收款明细', index=False)
        comb_result.to_excel(writer, sheet_name='月平均应收款', index=False)
        avg_sale.to_excel(writer, sheet_name='月合计销售额', index=False)

    print(f"Excel 文件已成功保存到 {file_path_batch}")

    # 收集所有批次的结果
    all_results.append(results_df)
    all_comb_results.append(comb_result)
    all_avg_sales.append(avg_sale)

# 合并所有批次的结果
final_results = pd.concat(all_results, ignore_index=True)
final_comb_results = pd.concat(all_comb_results, ignore_index=True)
final_avg_sales = pd.concat(all_avg_sales, ignore_index=True)

# 最后将合并的结果导出到一个单独的文件
final_file_path =/home/user/data/s/查询结果_最终合并.xlsx'
with pd.ExcelWriter(final_file_path, engine='openpyxl') as writer:
    final_results.to_excel(writer, sheet_name='应收款明细', index=False)
    final_comb_results.to_excel(writer, sheet_name='月平均应收款', index=False)
    final_avg_sales.to_excel(writer, sheet_name='月合计销售额', index=False)

print(f"最终合并的 Excel 文件已成功保存到 {final_file_path}")
