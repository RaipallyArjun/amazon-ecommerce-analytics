WITH CustomerRFM AS (
    SELECT
        Customer_Name,
        MAX(Date) AS LastPurchaseDate,
        COUNT(DISTINCT Order_ID) AS Frequency,
        SUM(Total_Sales) AS Monetary
    FROM sales_data
    GROUP BY Customer_Name
),

RFM_Base AS (
    SELECT
        Customer_Name,
        (JULIANDAY('2025-04-01') - JULIANDAY(LastPurchaseDate)) AS Recency,
        Frequency,
        Monetary
    FROM CustomerRFM
),

RFM_Score AS (
    SELECT *,
        NTILE(5) OVER (ORDER BY Recency DESC) AS R_Score,
        NTILE(5) OVER (ORDER BY Frequency ASC) AS F_Score,
        NTILE(5) OVER (ORDER BY Monetary ASC) AS M_Score
    FROM RFM_Base
),

Final AS (
    SELECT *,
        (R_Score + F_Score + M_Score) AS RFM_Total_Score,
        CASE
            WHEN (R_Score >= 4 AND F_Score >= 4 AND M_Score >= 4)
                THEN 'Champions'
            WHEN (R_Score >= 3 AND F_Score >= 3)
                THEN 'Loyal Customers'
            WHEN (R_Score <= 2 AND F_Score >= 3)
                THEN 'At Risk'
            WHEN (R_Score = 1)
                THEN 'Lost Customers'
            ELSE 'Potential'
        END AS Customer_Segment
    FROM RFM_Score
)

SELECT *
FROM Final
ORDER BY RFM_Total_Score DESC;