create table ngc2000 (
  id int unsigned not null primary key,
  messier smallint unsigned default null,
  ra double not null,
  `dec` double not null,
  `type` char(3),
  constellation char(3),
  `size` float,
  mag float
) engine=InnoDB