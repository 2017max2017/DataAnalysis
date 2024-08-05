#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pandas as pd
import pyodbc
from openpyxl import load_workbook
import os


# In[3]:


#下载数据

# 数据库连接信息
server = 'www.hzblwz.com,1533'
database = 'rxerpmygljtb'
username = 'hzblwz'
password = 'Hzblwz@240607'

# 创建数据库连接字符串
connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'


# SQL 查询语句
sql_query = """


select a.rq as 日期,a.dh as 单据号,case when a.FHBZ ='1' then '已发货' else '未发货' end as 发货标志,
a.khbh as 客户编号,a.khmc as 客户名称,a.ywlb as 业务类别代码,ywlb.YWMC as 业务类别名称,a.ckdm as 仓库代码,a.ckmc as 仓库名称,
a.cpmc as 产品名称,a.cpcd as 产地,a.cpgg as 规格,a.cpxh as 型号,a.jh as 卷号,a.cpsl as 件数,a.cpzl as 重量,a.kdsl as 销售件数,a.kdzl as 销售重量,a.zje as 总金额,
a.num1 as 部门定价,a.cpdj as 单价,a.cbj as 成本价,a.czz as 操作员代码,czz.yhmc as 操作员名称,a.bz as 主表备注,a.beizhu as 明细备注,a.chepih as 车皮号,
case when isnull(mxskh,'') ='' then skbz when mxskh>'' and skbz='1'  then '1' else skbz end as 收款标志,
a.dwlx as 单位类型代码,dwlx.mc as 单位类型,a.ywy as 业务员代码,ywy.yhmc as 业务员名称,a.sysrq as 系统日期,a.dept as 部门代码,dept.mc as 部门名称,
a.ysfs as 运输方式代码,ysfs.MC as 运输方式,
a.fhr as 发货人代码,fhr.yhmc as 发货人名称,a.ydrq as 发货日期,a.ghdw as 供货单位代码,ghdw.dwmc as 供货单位名称, a.fgs as 公司代码,fgs.mc as 公司名称,
a.YSDH as 运输单号,a.xsysdj as 运输单价,a.FYDH as 费用单号,a.xsfydj as 费用单价 from (
select a.rq,a.dh,b.fhbz,a.khbh,a.khmc,a.ywlb,a.ckdm,a.ckmc,b.cpmc,b.cpcd,b.cpgg,b.cpxh,b.jh,b.cpsl,b.cpzl,b.kdsl,b.kdzl,b.zje,b.num1,b.cpdj,b.cbj,a.czz,a.bz,b.beizhu,b.chepih,
a.skbz,case when a.djlx ='2' and a.ywlb in ('XSD02','XSD05','XSD08') then
         (select max(dh) from (
          select max(a1.dh) dh from cw_skd a1 with (nolock),CW_sKDHXtzMX b1 with (nolock) where a1.dh = b1.dh and isnull(a1.zfpb,0) = 0 and b1.ywdjph = b.htph
          union all
          select max(a1.dh) from cw_skd a1 with (nolock),CW_sKDHXtzMX b1 with (nolock) where a1.dh = b1.dh and isnull(a1.zfpb,0) = 0 and b1.ywdjph = b.djph
          union all
          select max(a1.dh) from cw_skd a1 with (nolock),CW_sKDHXMX b1 with (nolock) where a1.dh = b1.dh and isnull(a1.zfpb,0) = 0 and b1.ywdjhm = b.djxh) A)
         else 
         (select max(dh) from (
          select max(a1.dh) dh from cw_skd a1 with (nolock),CW_sKDHXtzMX b1 with (nolock) where a1.dh = b1.dh and isnull(a1.zfpb,0) = 0 and b1.ywdjph = b.djph
          union all
          select max(a1.dh) from cw_skd a1 with (nolock),CW_sKDHXMX b1 with (nolock) where a1.dh = b1.dh and isnull(a1.zfpb,0) = 0 and b1.ywdjhm = b.djxh) A)
         end mxskh,c.dwlx,a.ywy,a.sysrq,a.dept,a.ysfs,b.fhr,b.ydrq,f.ghdw,a.fgs,b.djlx,a.YSDH,a.FYDH,b.xsysdj,b.xsfydj

FROM wp_dj a with (nolock) inner join WP_djmx b with (nolock) on a.dh = b.djxh and a.djlx = b.djlx 
	 inner join wp_dwgl c on a.khbh = c.dwdm left join wp_yhgl d on a.ywy = d.yhbh
	 left join(
	 SELECT max(D.GHDW) ghdw,MAX(E.DHRQ) dhrq,d.fgs,E.jh FROM WP_DJ D WITH (NOLOCK),WP_DJMX E WITH (NOLOCK) 
	 WHERE D.DH = E.DJXH AND D.DJLX = E.DJLX AND D.DJLX = '1' AND E.JH >'' group by d.fgs,e.jh /*,e.djph */
	 ) f on B.JH = f.JH and A.FGS = f.FGS
	where ISNULL(a.zfpb,0) =0
) A left join SYS_YWLB ywlb on a.YWLB = ywlb.YWDM
    left join WP_YHGL czz on a.CZZ = czz.yhbh
    left join WP_YHGL ywy on a.ywy = ywy.yhbh
    left join wp_zdgl dwlx on a.dwlx = dwlx.dm
    left join wp_zdgl dept on a.dept = dept.dm
    left join WP_YHGL fhr on a.fhr = fhr.yhbh
    left join wp_zdgl fgs on a.fgs = fgs.dm
    left join wp_dwgl ghdw on a.ghdw = ghdw.dwdm
    left join (
   SELECT WP_ZDGL.MC,   
         case when isnull(fylb,'') ='' then WP_ZDGL.DM else fylb end dm
    FROM WP_ZDGL where fldm = '0422' and isnull(jc,0) =1 and isnull(zfpb,0) =0
  union All
  select mc,dm from (
  select '自提' mc ,'1' dm
  union All
  select '代运','0'
  union All
  select '跟车','2'
  union All
  select '包运','3'
  union All
  select '汽运','01'
  Union All
  select '铁路','02'
  union All
  select '水运','03'
  union All
  select '自提(力费记账)','4'
  union All
  select '自提(力费自理)','5'
  ) a where (select count(*) from wp_zdgl where fldm ='0422') = 0) ysfs on a.ysfs = ysfs.dm
where A.djlx in('2','0','49') and 
/*以下日期条件根据自己要求调整*/
rq between '2022-01-01 00:00:00' and '2024-07-31 23:59:59'
"""

# 连接数据库并执行查询
file_path = '/home/user/data/sales_report0731.csv'
file_path2 = '/home/user/data/sales_report0731.xlsx'

try:
    with pyodbc.connect(connection_string) as conn:
        # 执行 SQL 查询并加载数据到 DataFrame
        df = pd.read_sql_query(sql_query, conn)

        # 显示 DataFrame 的前 10 行
        print(df.head(10))
    df = df.sort_values(by='日期', ascending=False)
    print('已按日期降序排列')
    

except Exception as e:
    print(f"执行查询或文件操作时发生错误: {e}")


# In[4]:


# 导出 DataFrame 到 CSV 文件
output_path = file_path  # 保存到你的指定地址
df.to_csv(output_path, index=False)
print(f"CSV 文件已成功保存到 {output_path}")

output_path2 = file_path2  # 保存到你的指定地址
df.to_excel(output_path2, index=False, engine='openpyxl')
print(f"EXCEL文件已成功保存到 {output_path2}")

