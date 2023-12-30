#### Rust & jQuery Data Tables

> PostgreSQL Setup

Table-1 : `t_random`

Create Table `t_random`

```sql
CREATE TABLE t_random (
   random_num INT NOT NULL,
   random_float DOUBLE PRECISION NOT NULL,
   md5 TEXT NOT NULL
);
```

Insert Data Into Table `t_random`

```sql
INSERT INTO t_random (random_num, random_float, md5) 
    SELECT 
        floor(random()* (999-100 + 1) + 100), 
        random(), 
        md5(random()::text) 
    from generate_series(1,100);
```

Verify data on table `t_random`

```sql
# select * from t_random limit 10;
 random_num |    random_float     |               md5
------------+---------------------+----------------------------------
        349 |  0.7457606724537023 | 14569004fc1e3156ef5c2b95617863b6
        361 |  0.5383973494637289 | e06d3d8ca3b7256f7e861f6e007f0295
        870 | 0.31454480989041755 | 54160809bc82e4eda0f4cd69ba979cc4
        372 | 0.16725864580014616 | 76375027264b59b82beaede6ef929a26
        598 |  0.9532303436714109 | 11c50afd37d5df70dbff23618aa01d11
        927 |  0.6049381744761106 | cea32e3ecda6af3453095590ab488364
        140 |  0.9289394890510856 | 76b7a01223e2664acd44e2c3617c0033
        587 | 0.15882955249878705 | 5b04d0c44320512a568b43930aa1b659
        648 |  0.6760506693222972 | c8548624b4726e849ff76c52506f208a
        289 |  0.6554822669834195 | d2a2c2de2ae5d1eeaf4d636efa448f81
(10 rows)
```

<hr/>

Table-2 : `t_data`

How to generate random date in `YYYY-MM-DD` format

```sql
# SELECT to_char(date_trunc('year', current_date) - trunc(random() * 365) * interval '1 day', 'YYYY-MM-DD') AS random_date;
 random_date
-------------
 2022-09-08
```

How to generate random alpha-numeric character of fixed length

```sql
# SELECT substring(md5(random()::text), 1, 10) as fixed_length_string;
 fixed_length_string
---------------------
 d2aa99a4d5
```

Create Table `t_data`

```sql
CREATE TABLE t_data (
   my_date TEXT NOT NULL,
   my_data TEXT NOT NULL
);
```

Insert Data Into Table `t_data`

```sql
INSERT INTO t_data (my_date, my_data) 
    SELECT 
        to_char(date_trunc('year', current_date) - trunc(random() * 365) * interval '1 day', 'YYYY-MM-DD'), 
        substring(md5(random()::text), 1, 10) as fixed_length_string  
    from generate_series(1,100);
```

Verify data on table `t_data`

```sql
# select * from t_data limit 10;
  my_date   |  my_data
------------+------------
 2022-12-12 | 963e07c596
 2022-09-22 | 6e8b5918b0
 2022-10-07 | f3c7fc9982
 2022-10-11 | 52ef6e6e58
 2022-07-09 | f78a8b1c28
 2022-08-01 | fe78b9daef
 2022-12-30 | 86952b9cc5
 2022-03-16 | 793b8b8533
 2022-07-01 | a90f862917
 2022-11-01 | 3e39e6e552
(10 rows)
```

> Rust Setup

FYI : `Cargo.toml` has some (not-needed dependencies, please ignore that)

Create ENV Config `app.rust.env`

```ini
DATABASE_URL=postgres://PG_USERNAME:PG_PASS@HOSTNAME/DB_NAME
PG.USER=PG_USERNAME
PG.PASSWORD=PG_PASS
PG.HOST=HOSTNAME
PG.PORT=5432
PG.DBNAME=DB_NAME
PG.POOL.MAX_SIZE=16
```

Create CSV Data Directory (`*.csv` Files Gets Generated Here)

```bash
mkdir data_dir
```

Run The Program

```bash
cargo run
```

Access The Web UI (jQuery Data Table)

```bash
http://127.0.0.1:5050/tables
```