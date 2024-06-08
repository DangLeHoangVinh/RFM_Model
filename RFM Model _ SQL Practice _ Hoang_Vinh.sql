--Practice RFM
-- Connect Data sources

-- B1: Create new data base
Create Database SuperStore;

--B2: import files csv using import wizard  ...

Select * from customer;
Select * from sales;
Select * from segment_scores;

---------------- RFM Calculate------------------

-- Group customer by customer key or name
-- Recency_Value = Current day - max (last) date buying   (using Datediff: find distance between 2 date)
-- Frequency_Value = count distinct order date
-- Monetary_Value =  sum(value0), can also use Round(value, 2)..

-- use over .. + order by
-- Recency order by DESCC 
-- Frequency = monetary ASC ...

-- use concat to combine char in to final score
-- join Segment score to -> segment


---------------- Practice -------------------------

Select * from sales;

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