create table concordance (
    hd_id int unsigned primary key,
    dm_id varchar(12),
    gc_id int unsigned,
    bs_id int unsigned,
    hip_id int unsigned,
    ra double,
    `dec` double,
    v_mag float,
    flamsteed smallint unsigned,
    bayer varchar(5),
    constellation char(3)
) engine=InnoDB