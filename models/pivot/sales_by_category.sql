WITH sales_data AS (
    SELECT
        product,
        category,
        num_sold
    FROM {{ ref('sales') }}
)

SELECT
    category,
    SUM(num_sold) AS total_sales
FROM
    sales_data
GROUP BY
    category