CREATE DATABASE IF NOT EXISTS perpinv;

DROP TABLE IF EXISTS perpinv.stg_stock;
DROP TABLE IF EXISTS perpinv.stg_pmix;
DROP TABLE IF EXISTS perpinv.stg_deli;
DROP TABLE IF EXISTS perpinv.stg_invadj;
DROP TABLE IF EXISTS perpinv.stg_reset;

CREATE TABLE IF NOT EXISTS perpinv.stg_stock(
	is_no LONG,
	siteid LONG,
	su STRING,
	transferday DATE,
	trx_date DATE,
	stock_qty_units DOUBLE
	)
    USING PARQUET  
    OPTIONS ( header='true',
              nullvalue='NA',
              dateFormat="MM/dd/yyyy",
              path='hdfs:///user/root/PerpInv/stg_stock.parquet');

select * from perpinv.stg_stock limit 1;

CREATE TABLE IF NOT EXISTS perpinv.stg_pmix(
	is_no LONG,
	siteid LONG,
	menuitem STRING,
	transferday DATE,
	trx_date DATE,
	sales_qty_units DOUBLE
	)
    USING PARQUET  
    OPTIONS ( header='true',
              nullvalue='NA',
              dateFormat="MM/dd/yyyy",
              path='hdfs:///user/root/PerpInv/stg_pmix.parquet');

select * from perpinv.stg_pmix limit 1;

CREATE TABLE IF NOT EXISTS perpinv.stg_deli(
	is_no LONG,
	siteid LONG,
	su STRING,
	transferday DATE,
	trx_date TIMESTAMP,
	delivery_qty_cases DOUBLE
	)
    USING PARQUET  
    OPTIONS ( header='true',
              nullvalue='NA',
              dateFormat="MM/dd/yyyy",
              timestampFormat="MM/dd/yyyy HH:mm:ss",
              path='hdfs:///user/root/PerpInv/stg_deli.parquet');

select * from perpinv.stg_deli limit 1;

CREATE TABLE IF NOT EXISTS perpinv.stg_invadj(
	is_no LONG,
	siteid LONG,
	su STRING,
	transferday DATE,
	trx_date DATE,
	adj_qty_cases DOUBLE
	)
    USING PARQUET  
    OPTIONS ( header='true',
              nullvalue='NA',
              dateFormat="MM/dd/yyyy",
              path='hdfs:///user/root/PerpInv/stg_invadj.parquet');

select * from perpinv.stg_invadj limit 1;

CREATE TABLE IF NOT EXISTS perpinv.stg_reset(
	is_no LONG,
	siteid LONG,
	pponumber STRING,
	transferday DATE,
	trx_date DATE,
	reset_qty_units DOUBLE
	)
    USING PARQUET  
    OPTIONS ( header='true',
              nullvalue='NA',
              dateFormat="MM/dd/yyyy",
              path='hdfs:///user/root/PerpInv/stg_reset.parquet');

select * from perpinv.stg_reset limit 1;

CREATE TABLE IF NOT EXISTS perpinv.stg_bmitoppo(
	siteid LONG,
	menuitem STRING,
	pponumber STRING,
	usage_qty DOUBLE
	)
    USING PARQUET  
    OPTIONS ( header='true',
              nullvalue='NA',
              path='hdfs:///user/root/PerpInv/stg_bmitoppo.parquet');

select * from perpinv.stg_bmitoppo limit 1;

CREATE TABLE IF NOT EXISTS perpinv.stg_ppotosu(
	siteid LONG,
	pponumber STRING,
	su STRING,
	unitpp DOUBLE
	)
    USING PARQUET  
    OPTIONS ( header='true',
              nullvalue='NA',
              path='hdfs:///user/root/PerpInv/stg_ppotosu.parquet');

select * from perpinv.stg_ppotosu limit 1;
