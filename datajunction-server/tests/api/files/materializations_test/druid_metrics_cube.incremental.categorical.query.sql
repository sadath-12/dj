WITH
default_DOT_repair_orders_fact AS (SELECT  default_DOT_repair_orders_fact.order_date default_DOT_repair_orders_fact_DOT_order_date,
	default_DOT_hard_hat.state default_DOT_hard_hat_DOT_state,
	default_DOT_dispatcher.company_name default_DOT_dispatcher_DOT_company_name,
	default_DOT_municipality_dim.local_region default_DOT_municipality_dim_DOT_local_region,
	count(default_DOT_repair_orders_fact.repair_order_id) default_DOT_num_repair_orders,
	sum(default_DOT_repair_orders_fact.total_repair_cost) default_DOT_total_repair_cost
 FROM (SELECT  repair_orders.repair_order_id,
	repair_orders.municipality_id,
	repair_orders.hard_hat_id,
	repair_orders.dispatcher_id,
	repair_orders.order_date,
	repair_orders.dispatched_date,
	repair_orders.required_date,
	repair_order_details.discount,
	repair_order_details.price,
	repair_order_details.quantity,
	repair_order_details.repair_type_id,
	repair_order_details.price * repair_order_details.quantity AS total_repair_cost,
	repair_orders.dispatched_date - repair_orders.order_date AS time_to_dispatch,
	repair_orders.dispatched_date - repair_orders.required_date AS dispatch_delay
 FROM roads.repair_orders AS repair_orders JOIN roads.repair_order_details AS repair_order_details ON repair_orders.repair_order_id = repair_order_details.repair_order_id
 WHERE  repair_orders.order_date = ${dj_logical_timestamp}) AS default_DOT_repair_orders_fact LEFT JOIN (SELECT  default_DOT_dispatchers.dispatcher_id,
	default_DOT_dispatchers.company_name
 FROM roads.dispatchers AS default_DOT_dispatchers) AS default_DOT_dispatcher ON default_DOT_repair_orders_fact.dispatcher_id = default_DOT_dispatcher.dispatcher_id
LEFT JOIN (SELECT  default_DOT_hard_hats.hard_hat_id,
	default_DOT_hard_hats.state
 FROM roads.hard_hats AS default_DOT_hard_hats) AS default_DOT_hard_hat ON default_DOT_repair_orders_fact.hard_hat_id = default_DOT_hard_hat.hard_hat_id
LEFT JOIN (SELECT  m.municipality_id AS municipality_id,
	m.local_region
 FROM roads.municipality AS m LEFT JOIN roads.municipality_municipality_type AS mmt ON m.municipality_id = mmt.municipality_id
LEFT JOIN roads.municipality_type AS mt ON mmt.municipality_type_id = mt.municipality_type_desc) AS default_DOT_municipality_dim ON default_DOT_repair_orders_fact.municipality_id = default_DOT_municipality_dim.municipality_id
 GROUP BY  default_DOT_repair_orders_fact.order_date, default_DOT_hard_hat.state, default_DOT_dispatcher.company_name, default_DOT_municipality_dim.local_region),
combiner_query AS (SELECT  default_DOT_repair_orders_fact.default_DOT_num_repair_orders,
	default_DOT_repair_orders_fact.default_DOT_total_repair_cost,
	default_DOT_repair_orders_fact.default_DOT_repair_orders_fact_DOT_order_date,
	default_DOT_repair_orders_fact.default_DOT_hard_hat_DOT_state,
	default_DOT_repair_orders_fact.default_DOT_dispatcher_DOT_company_name,
	default_DOT_repair_orders_fact.default_DOT_municipality_dim_DOT_local_region
 FROM default_DOT_repair_orders_fact)

SELECT  default_DOT_num_repair_orders,
	default_DOT_total_repair_cost,
	default_DOT_repair_orders_fact_DOT_order_date,
	default_DOT_hard_hat_DOT_state,
	default_DOT_dispatcher_DOT_company_name,
	default_DOT_municipality_dim_DOT_local_region
 FROM combiner_query
 WHERE  default_DOT_repair_orders_fact_DOT_order_date = CAST(DATE_FORMAT(CAST(${dj_logical_timestamp} AS TIMESTAMP), 'yyyyMMdd') AS TIMESTAMP) AND default_DOT_hard_hat_DOT_state = CAST(${default_DOT_hard_hat_DOT_state} AS STRING)
