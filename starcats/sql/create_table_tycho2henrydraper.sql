create table tycho2henrydraper (
       tyc1 smallint unsigned not null,
       tyc2 smallint unsigned not null,
       tyc3 smallint unsigned not null,
       hd_id int unsigned not null,
       index (tyc1, tyc2, tyc3),
       index (hd_id)
 ) engine=InnoDB;