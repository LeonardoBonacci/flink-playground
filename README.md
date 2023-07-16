# flink-prep

```
select count(*) from transfers;
select * from transfers order by timestamp desc;

select * from transfers where id = '4d0ee0ee-682a-42b6-9d9a-ab046f1a5fc5'
drop table transfers;

CREATE TABLE IF NOT EXISTS transfers (
  id VARCHAR ( 50 ) PRIMARY KEY,
  _from VARCHAR ( 50 ) NOT NULL,
  _to VARCHAR ( 50 ) NOT NULL,
  pool_id VARCHAR ( 50 ) NOT NULL,
  amount decimal ( 6, 2 )  NOT NULL,
  timestamp bigint NOT NULL 
);

insert into transfers values ('foo', 'a', 'b', 'coro', 10.00, 1234567890);
insert into transfers values ('abcdef', 'a', 'b', 'coro', 3.00, 1234567890);
insert into transfers values ('ghijkl', 'b', 'c', 'coro', 5.00, 1234567890);
insert into transfers values ('mnopqr', 'c', 'a', 'coro', 4.00, 1234567890);

select sum(amount) as balance
from (
  select -1 * sum (amount) as amount from transfers where _from = 'aaaa'
  union
  select sum (amount) from transfers where _to = 'a'
) AS tmp; 

select sum(amount) as balance from (select -1 * sum (amount) as amount from transfers where _from = 'a' union select sum (amount) from transfers where _to = 'a') AS tmp

select sum(amount) as "balance" 
from (
  select sum("amount") from transfers where _from = 'c' 
  union 
  select sum("amount") from transfers where _to = 'c'
) as "alias_43890460"

```