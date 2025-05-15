-- Nettoie et prépare les données de vente

with source as (
    select * from raw_sales where "CustomerID"<> NULL AND "UnitPrice" <> NULL
),

renamed as (
    select
        
        cast("InvoiceNo" as text) as invoice_no,
        cast("StockCode" as text) as stock_code,
        "Description" as description,
        cast("Quantity" as integer) as quantity,
        cast("InvoiceDate" as timestamp) as invoice_date,
        cast("UnitPrice" as numeric) as unit_price,
        cast("CustomerID" as integer) as customer_id,
        "Country" as country
    from source
    where "CustomerID" is not null
      and "UnitPrice" is not null
)

select * from renamed
