use split_region;

-- Insert some data after the normal DDLs
insert into test1(id, val, c1, c2) values (6000, 600000, 600, 'test600');
insert into test1(id, val, c1, c2) values (7000, 700000, 700, 'test700');
insert into test1(id, val, c1, c2) values (8000, 800000, 800, 'test800');

insert into test2(id) values (6000), (7000), (8000);

-- Update some data
update test1 set val = val + 1000 where id > 5000;
update test2 set id = id + 10000 where id > 5000;

-- Delete some data
delete from test1 where id = 1;
delete from test2 where id = 1;
