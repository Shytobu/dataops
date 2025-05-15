-- Calcule les revenus quotidiens

with sales as (
    select * from {{ ref('stg_sales') }}
),

aggregated as (
    select
        date_trunc('day', invoice_date) as date,
        sum(quantity * unit_price) as daily_revenue,
        count(*) as nb_transactions
    from sales
    group by 1
)

select * from aggregated
