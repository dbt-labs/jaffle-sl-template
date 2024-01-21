use database dbt_jaffle_shop;
use schema data_dev;
use role snowpilot_team;

alter table stg_locations add constraint pk_1 primary key (location_id);
alter table stg_customers ADD constraint pk_1 primary key (customer_id);
alter table stg_products add constraint pk_1 primary key (product_id);
alter table stg_supplies add constraint pk_1 primary key (supply_uuid);

alter table stg_orders ADD constraint pk_1 primary key (order_id);
alter table stg_orders ADD constraint fk_1 foreign key (location_id) references stg_locations (location_id);
alter table stg_orders ADD constraint fk_2 foreign key (customer_id) references stg_customers (customer_id);
alter table stg_order_items add constraint pk_1 primary key (order_item_id);
alter table stg_order_items ADD constraint fk_1 foreign key (product_id) references stg_products (product_id);
alter table stg_supplies add constraint fk_1 foreign key (product_id) references stg_products (product_id);

