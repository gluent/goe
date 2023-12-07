SELECT 
    t.calendar_year,
    ch.channel_desc,
    my_plsql_func(p.prod_subcategory) as prod_subcategory,
    SUM(s.amount_sold)                as net_spend_amt,
    SUM(s.quantity_sold)              as item_qty,
    COUNT(*)                          as num_sales,
    GROUPING_ID(ch.channel_desc,
                t.calendar_year,
                p.prod_subcategory)   as grps
FROM
    sales           s,
    times           t,
    products        p,
    channels        ch,
    promotions      pm
WHERE
-- join conditions
    t.time_id         = s.time_id
    AND p.prod_id     = s.prod_id
    AND ch.channel_id = s.channel_id
    AND pm.promo_id   = s.promo_id
-- filter conditions
    AND pm.promo_name    = 'blowout sale'
    AND ch.channel_class = 'Direct'
    AND p.prod_category  = 'Peripherals and Accessories'
GROUP BY
    t.calendar_year,
    ch.channel_desc,
    ROLLUP(p.prod_subcategory)
ORDER BY
    t.calendar_year,
    ch.channel_desc,
    p.prod_subcategory;
