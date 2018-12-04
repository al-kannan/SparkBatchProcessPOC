CREATE DATABASE IF NOT EXISTS perpinv;

DROP TABLE IF EXISTS perpinv.stg_stck_ppo;

CREATE TABLE IF NOT EXISTS perpinv.stg_stck_ppo(
	siteid LONG,
	bday DATE,
	pponumber STRING,
	stockqty DOUBLE,
	deliqty DOUBLE,
	invadjqty DOUBLE,
	pmixqty DOUBLE,
	onhand DOUBLE
	)
    USING parquet  
    OPTIONS ( header='false',
              dateFormat="yyyy-MM-dd",
              path='hdfs:///user/root/PerpInv/stg_stck_ppo.parquet');

select * from perpinv.stg_stck_ppo limit 1;

