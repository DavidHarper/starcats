create table sao2000 (
    sao_id int unsigned not null primary key,
    hd_id int unsigned,
    ra double not null,
    `dec` double not null,
    v_mag float,
    p_mag float,
    spectral_type varchar(3),
    key (ra),
    key (v_mag)
) engine=InnoDB