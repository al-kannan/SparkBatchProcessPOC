CREATE DATABASE IF NOT EXISTS perpinv;

DROP TABLE IF EXISTS perpinv.pmix_stk3_tx;
DROP TABLE IF EXISTS perpinv.pmix_stk3_bday_tx;
DROP TABLE IF EXISTS perpinv.reset_stk3_tx;
DROP TABLE IF EXISTS perpinv.reset_stk3_bday_tx;
DROP TABLE IF EXISTS perpinv.deli_stk3_tx;
DROP TABLE IF EXISTS perpinv.deli_stk3_bday_tx;
DROP TABLE IF EXISTS perpinv.invadj_stk3_tx;
DROP TABLE IF EXISTS perpinv.invadj_stk3_bday_tx;
DROP TABLE IF EXISTS perpinv.stock_stk3_tx;
DROP TABLE IF EXISTS perpinv.stock_stk3_bday_tx;

DROP TABLE IF EXISTS perpinv.stk3_bday_tx;
DROP TABLE IF EXISTS perpinv.stk3_tx;

CREATE TABLE IF NOT EXISTS perpinv.pmix_stk3_tx(
	is_no LONG,
	siteid LONG,
	pponumber STRING,
	bday DATE,
	qty DOUBLE
	)
    USING CSV  
    OPTIONS ( header='false',
              dateFormat="yyyy-MM-dd",
              path='hdfs:///user/root/PerpInv/pmix_stk3_tx.csv');

select * from perpinv.pmix_stk3_tx limit 1;

CREATE TABLE IF NOT EXISTS perpinv.pmix_stk3_bday_tx(
	is_no LONG,
	siteid LONG,
	bday DATE
	)
    USING CSV  
    OPTIONS ( header='false',
              dateFormat="yyyy-MM-dd",
              path='hdfs:///user/root/PerpInv/pmix_stk3_bday_tx.csv');

select * from perpinv.pmix_stk3_bday_tx limit 1;

CREATE TABLE IF NOT EXISTS perpinv.reset_stk3_tx(
	is_no LONG,
	siteid LONG,
	pponumber STRING,
	bday DATE,
	qty DOUBLE
	)
    USING CSV  
    OPTIONS ( header='false',
              dateFormat="yyyy-MM-dd",
              path='hdfs:///user/root/PerpInv/reset_stk3_tx.csv');

select * from perpinv.reset_stk3_tx limit 1;

CREATE TABLE IF NOT EXISTS perpinv.reset_stk3_bday_tx(
	is_no LONG,
	siteid LONG,
	bday DATE
	)
    USING CSV  
    OPTIONS ( header='false',
              dateFormat="yyyy-MM-dd",
              path='hdfs:///user/root/PerpInv/reset_stk3_bday_tx.csv');

select * from perpinv.reset_stk3_bday_tx limit 1;

CREATE TABLE IF NOT EXISTS perpinv.deli_stk3_tx(
	is_no LONG,
	siteid LONG,
	pponumber STRING,
	bday DATE,
	qty DOUBLE
	)
    USING CSV  
    OPTIONS ( header='false',
              dateFormat="yyyy-MM-dd",
              path='hdfs:///user/root/PerpInv/deli_stk3_tx.csv');

select * from perpinv.deli_stk3_tx limit 1;

CREATE TABLE IF NOT EXISTS perpinv.deli_stk3_bday_tx(
	is_no LONG,
	siteid LONG,
	bday DATE
	)
    USING CSV  
    OPTIONS ( header='false',
              dateFormat="yyyy-MM-dd",
              path='hdfs:///user/root/PerpInv/deli_stk3_bday_tx.csv');

select * from perpinv.deli_stk3_bday_tx limit 1;

CREATE TABLE IF NOT EXISTS perpinv.invadj_stk3_tx(
	is_no LONG,
	siteid LONG,
	pponumber STRING,
	bday DATE,
	qty DOUBLE
	)
    USING CSV  
    OPTIONS ( header='false',
              dateFormat="yyyy-MM-dd",
              path='hdfs:///user/root/PerpInv/invadj_stk3_tx.csv');

select * from perpinv.invadj_stk3_tx limit 1;

CREATE TABLE IF NOT EXISTS perpinv.invadj_stk3_bday_tx(
	is_no LONG,
	siteid LONG,
	bday DATE
	)
    USING CSV  
    OPTIONS ( header='false',
              dateFormat="yyyy-MM-dd",
              path='hdfs:///user/root/PerpInv/invadj_stk3_bday_tx.csv');

select * from perpinv.invadj_stk3_bday_tx limit 1;


CREATE TABLE IF NOT EXISTS perpinv.stock_stk3_tx(
	is_no LONG,
	siteid LONG,
	pponumber STRING,
	bday DATE,
	qty DOUBLE
	)
    USING CSV  
    OPTIONS ( header='false',
              dateFormat="yyyy-MM-dd",
              path='hdfs:///user/root/PerpInv/stock_stk3_tx.csv');

select * from perpinv.stock_stk3_tx limit 1;

CREATE TABLE IF NOT EXISTS perpinv.stock_stk3_bday_tx(
	is_no LONG,
	siteid LONG,
	bday DATE
	)
    USING CSV  
    OPTIONS ( header='false',
              dateFormat="yyyy-MM-dd",
              path='hdfs:///user/root/PerpInv/stock_stk3_bday_tx.csv');

select * from perpinv.stock_stk3_bday_tx limit 1;

CREATE TABLE IF NOT EXISTS perpinv.stk3_bday_tx(
        tx_type_cd STRING,
	siteid LONG,
	bday DATE,
	is_no LONG
	)
    USING CSV  
    OPTIONS ( header='false',
              dateFormat="yyyy-MM-dd",
              path='hdfs:///user/root/PerpInv/stk3_bday_tx.csv');

select * from perpinv.stk3_bday_tx limit 1;


CREATE TABLE IF NOT EXISTS perpinv.stk3_tx(
	is_no LONG,
	siteid LONG,
	bday DATE,
	pponumber STRING,
	qty DOUBLE,
        tx_type_cd STRING
	)
    USING CSV  
    OPTIONS ( header='false',
              dateFormat="yyyy-MM-dd",
              path='hdfs:///user/root/PerpInv/stk3_tx.csv');

select * from perpinv.stk3_tx limit 1;

CREATE TABLE IF NOT EXISTS perpinv.stk3_bday_status(
	siteid LONG,
	bday DATE,
	pmix_is_no long,
	deli_is_no long,
	invadj_is_no long,
	reset_is_no long,
	stock_is_no long
	)
    USING CSV  
    OPTIONS ( header='false',
              dateFormat="yyyy-MM-dd",
              path='hdfs:///user/root/PerpInv/stk3_bday_status.csv');

select * from perpinv.stk3_bday_status limit 1;

