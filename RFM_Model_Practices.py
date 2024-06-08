#=================================== RFM Analysis ==================================
import pandas as pd
import pyodbc
from datetime import datetime   

#=================================== Lưu connection ===================================
connection = pyodbc.connect('Driver={SQL Server};'
               'Server=localhost\SQLEXPRESS01;'
               'Database=SuperStore;'
               'Trusted_Connection=yes;'
               )

#===========================================================================================
#========================== C1 Using Python Code ===========================================

Cus = pd.read_sql_query("Select * from customer",connection)
Sale= pd.read_sql_query("Select * from sales",connection)
Segment = pd.read_sql_query("Select * from segment_scores",connection)

# ============================= Recency ==============================================
Sale['Order_Date'] = pd.to_datetime(Sale['Order_Date']);
Sale.info();

RFM_data = Sale.groupby(by ='Customer_ID')['Order_Date'].max().reset_index()
RFM_data.columns = ['Customer_ID', 'Last_Order_Date']

""" dung thu vien datetime """
RFM_data['Recency_Value'] = (datetime.now() - RFM_data['Last_Order_Date']).dt.days # ep kieu ve day
datetime.today() # tuong tu datetime.now()

""" Join bang bang merge"""
RFM_data = pd.merge(Cus[['Customer_ID','Customer_Name']], RFM_data ,left_on = 'Customer_ID', right_on = 'Customer_ID')  # ko muon merge het bang thi dataframe[ ['..','...'] ] nhap ten cot muon merge


#=========================== Frequency ==============================================
Fre = Sale.groupby(by='Customer_ID')['Order_Date'].nunique().reset_index()
Fre.columns = ['Customer_ID','Frequency_Value']
Fre
RFM_data = pd.merge(RFM_data, Fre, on = 'Customer_ID')
RFM_data

#=============================== Monetary =================================================

Mon = Sale.groupby(by='Customer_ID')['Sales'].agg('sum').reset_index()
Mon.columns = ['Customer_ID','Monetary_Value']
Mon
RFM_data = pd.merge(RFM_data, Mon, on = 'Customer_ID')
RFM_data

#================================= Score ... thay the ntile = qcut trong pandas ====================================

# pandas.qcut(<cot muon chia>, <so can chia>, labels = [...]) # label la so danh dau

RFM_data['Recency_Score'] = pd.qcut(RFM_data['Recency_Value'], 5 ,labels=[5,4,3,2,1] )  
# Recency de vay de lay tu to den nho (thời gian mua hàng gần nhất càng lớn _lâu mua thì đánh dấu càng lớn _)

RFM_data['Frequency_Score'] = pd.qcut(RFM_data['Frequency_Value'], 5, labels = [1,2,3,4,5])
RFM_data['Monetary_Score'] = pd.qcut(RFM_data['Monetary_Value'], 5, labels = [1,2,3,4,5])

RFM_data.info()
RFM_data['RFM_Score'] = RFM_data['Recency_Score'].astype(str) + RFM_data['Frequency_Score'].astype(str) + RFM_data['Monetary_Score'].astype(str)

RFM_data

#================================== Merge ===============================

RFM_data.info()
Segment['Scores'] = Segment['Scores'].astype(str)
Segment

RFM_data = pd.merge(RFM_data[['Customer_Name','Recency_Value','Frequency_Value','Monetary_Value','RFM_Score']],Segment,left_on='RFM_Score',right_on='Scores')

#==================== Xóa bớt cột dư ===============================
RFM_result = RFM_data.drop('Scores',axis=1)


#======================================================================================
#===================== Cách 2 import script từ SQL ======================================

RFM2_result = pd.read_sql(
'''
with Base as
(
Select a.Customer_ID
    , Datediff(day,max(a.Order_Date),convert(date,getdate())) as Frequency_Values  -- Recency
    , Count(distinct Order_Date) as Recency_Value -- Frequency
    , round(Sum(a.Sales),2) as Monetary_Value
from sales a
join customer b
on a.Customer_ID = b.Customer_ID
group by a.Customer_ID
)
, Score_table as
(
    Select *
        , ntile(5) over (order by Frequency_Values DESC) Frequency_Score
        , ntile(5) over (order by Recency_Value ASC) Recency_Score
        , ntile(5) over (order by Monetary_Value ASC) Monetary_Score
    From Base
)
, RFM_table as
(
    Select a.Customer_ID
        ,b.Customer_Name
        ,a.Recency_Value
        ,a.Frequency_Values
        ,a.Monetary_Value
        , concat(Recency_Score,Frequency_Score,Monetary_Score) as RFM_Score
    From Score_table a
    join customer b
    on a.Customer_ID = b.Customer_ID
)
Select Customer_Name
        ,a.Recency_Value
        ,a.Frequency_Values
        ,a.Monetary_Value
        ,a.RFM_Score
        , b.Segment
from RFM_table a
join segment_scores b
on a. RFM_Score = b.Scores
    '''
    , connection)






