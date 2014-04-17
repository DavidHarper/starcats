create table henry_draper (
    hd_id int unsigned not null primary key,
    dm_id varchar(12),
    ra double not null,
    `dec` double not null,
    pv_mag float,
    pt_mag float,
    spectral_type varchar(3),
    key (ra),
    key (pv_mag)
) engine=InnoDB