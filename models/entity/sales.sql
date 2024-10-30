WITH web_sales AS (
    SELECT
        product,
        price,
        num_sold,
        category,
        'web' AS source
    FROM {{ ref('web_sales') }}
),

retail_sales AS (
    SELECT
        product,
        price,
        num_sold,
        category,
        'retail' AS source
    FROM {{ ref('retail_sales') }}
),

combined_sales AS (
    SELECT * FROM web_sales
    UNION ALL
    SELECT * FROM retail_sales
)

SELECT
    product,
    category,
    SUM(num_sold) AS num_sold
FROM
    combined_sales
GROUP BY
    product,
    category