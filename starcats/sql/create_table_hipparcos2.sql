create table hipparcos2 (
    hip_id int unsigned not null primary key,
    ra double not null,
    `dec` double not null,
    parallax double,
    pm_ra double,
    pm_dec double,
    hp_mag float,
    bv_colour float,
    key (ra),
    key (hp_mag)
) engine=InnoDB