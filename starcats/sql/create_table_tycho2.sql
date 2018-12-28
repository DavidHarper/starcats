create table tycho2 (
       tyc1 smallint unsigned not null,
       tyc2 smallint unsigned not null,
       tyc3 smallint unsigned not null,
       ra_mean double,
       dec_mean double,
       pm_ra float,
       pm_dec float,
       se_ra_mean float,
       se_dec_mean float,
       bt_mag float,
       se_bt_mag float,
       vt_mag float,
       se_vt_mag float,
       hip_id int unsigned,
       ra double not null,
       `dec` double not null,
       epochRA float,
       epochDec float,
       se_ra float,
       se_dec float,
       vmag float generated always as (vt_mag - 0.090 * (bt_mag-vt_mag)) virtual,
       bv_colour float generated always as (0.850*(bt_mag-vt_mag)) virtual,
       unique key (tyc1, tyc2, tyc3),
       key (ra_mean),
       key (vt_mag)
) engine=innodb;