#!/bin/bash
source "$(dirname "${BASH_SOURCE[0]}")/utils.sh"

run_master_orders_etl() {
  echo "🧩 Building Master Orders and Order Items Tables from all stores ..."

  run_mysql_query "$LOCAL_HOST" "$LOCAL_USER" "$LOCAL_PASS" "woo_master" "
    TRUNCATE TABLE orders;
    TRUNCATE TABLE order_items;
  "

  for COUNTRY in  TR DE FR NL BE BEFRLU AT DK ES IT SE FI PT CZ HU RO SK UK OPS ; do
    echo "🔗 Merging orders and items for $COUNTRY ..."
    
  # ⚙️ Check if the store database exists first (safe check)
  DB_EXISTS=$(mysql -h "$LOCAL_HOST" -u "$LOCAL_USER" -p"$LOCAL_PASS" -N -B -e "
    SELECT COUNT(*) FROM INFORMATION_SCHEMA.SCHEMATA
    WHERE SCHEMA_NAME = 'woo_${COUNTRY,,}';
  " 2>/dev/null || echo 0)

  if [ "$DB_EXISTS" -eq 0 ]; then
    echo "⚠️  Database woo_${COUNTRY,,} does not exist — skipping $COUNTRY."
    continue
  fi


  HAS_NAME=$(mysql -h "$LOCAL_HOST" -u "$LOCAL_USER" -p"$LOCAL_PASS" -N -B -e "
    SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'woo_${COUNTRY,,}'
      AND TABLE_NAME = 'order_items'
      AND COLUMN_NAME = 'order_item_name';
  ")

  NAME_SELECT=$([ "$HAS_NAME" -eq 1 ] && echo "oi.order_item_name" || echo "NULL")

run_mysql_query "$LOCAL_HOST" "$LOCAL_USER" "$LOCAL_PASS" "woo_master" "
  SET SESSION sql_mode = REPLACE(@@sql_mode, 'ONLY_FULL_GROUP_BY', '');

  INSERT IGNORE INTO woo_master.orders (
    order_number_formatted, source_store, order_id, order_date, order_status,
    customer_id, country_code, channel, site, billing_country, billing_city,
    units_total, ordered_items_count, ordered_items_skus, payment_method,
    currency_code, subtotal, gross_total, cogs, total_price,
    tax_amount, shipping_fee, fee_amount, discount_amount,
    refunded_amount, ads_spend, logistics_cost, other_costs,
    net_profit, net_revenue, net_margin
  )
  SELECT *
  FROM (
    SELECT
      COALESCE(NULLIF(TRIM(o.order_number_formatted), ''), CONCAT('ORD', o.order_id)) AS order_number_formatted,
      '$COUNTRY' AS source_store,
      MIN(o.order_id) AS order_id,
      MIN(
        CASE
          WHEN o.order_date IS NULL
              OR TRIM(o.order_date) = ''
              OR o.order_date IN ('0000-00-00', '0000-00-00 00:00:00')
          THEN NULL
          WHEN STR_TO_DATE(o.order_date, '%Y-%m-%d %H:%i:%s') IS NOT NULL
          THEN STR_TO_DATE(o.order_date, '%Y-%m-%d %H:%i:%s')
          ELSE NULL
        END
      ) AS order_date,
      MAX(o.order_status) AS order_status,
      MAX(o.customer_id) AS customer_id,
      MAX(o.country_code) AS country_code,
      MAX(o.channel) AS channel,
      MAX(o.site) AS site,
      MAX(o.billing_country) AS billing_country,
      MAX(o.billing_city) AS billing_city,
      MAX(o.units_total) AS units_total,
      MAX(o.ordered_items_count) AS ordered_items_count,
      MAX(o.ordered_items_skus) AS ordered_items_skus,
      MAX(o.payment_method) AS payment_method,
      MAX(o.currency_code) AS currency_code,
      MAX(o.subtotal) AS subtotal,
      MAX(o.gross_total) AS gross_total,
      MAX(o.cogs) AS cogs,
      MAX(o.total_price) AS total_price,
      MAX(o.tax_amount) AS tax_amount,
      MAX(o.shipping_fee) AS shipping_fee,
      MAX(o.fee_amount) AS fee_amount,
      MAX(o.discount_amount) AS discount_amount,
      MAX(o.refunded_amount) AS refunded_amount,
      MAX(o.ads_spend) AS ads_spend,
      MAX(o.logistics_cost) AS logistics_cost,
      MAX(o.other_costs) AS other_costs,
      MAX(o.net_profit) AS net_profit,
      MAX(o.net_revenue) AS net_revenue,
      MAX(o.net_margin) AS net_margin
    FROM woo_${COUNTRY,,}.orders o
    WHERE o.order_number_formatted IS NOT NULL
      AND o.order_number_formatted <> ''
      AND LENGTH(TRIM(o.order_number_formatted)) > 0
      AND o.order_number_formatted <> 'NULL'
    GROUP BY o.order_number_formatted
  ) AS deduped;
"


  run_mysql_query "$LOCAL_HOST" "$LOCAL_USER" "$LOCAL_PASS" "woo_master" "
    INSERT INTO order_items (
      order_item_id, order_id, product_id, variation_id, sku,
      order_item_name, quantity, line_total, line_tax,
      refund_reference, currency_code, source_store
    )
    SELECT
      oi.order_item_id,
      oi.order_id,
      oi.product_id,
      oi.variation_id,
      oi.sku,
      oi.order_item_name,
      oi.quantity,
      oi.line_total,
      oi.line_tax,
      oi.refund_reference,
      oi.currency_code,
      '$COUNTRY'
    FROM woo_${COUNTRY,,}.order_items oi;
  "

  done

  echo "✅ Master Orders and Order Items tables merged successfully."
}
