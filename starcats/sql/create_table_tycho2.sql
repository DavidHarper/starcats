-- starcats - a package for loading stars catalogues into a MySQL database.
--
-- Copyright (C) 2016-2019 David Harper at obliquity.com
-- 
-- This library is free software; you can redistribute it and/or
-- modify it under the terms of the GNU Library General Public
-- License as published by the Free Software Foundation; either
-- version 2 of the License, or (at your option) any later version.
--
-- This library is distributed in the hope that it will be useful,
-- but WITHOUT ANY WARRANTY; without even the implied warranty of
-- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
-- Library General Public License for more details.
-- 
-- You should have received a copy of the GNU Library General Public
-- License along with this library; if not, write to the
-- Free Software Foundation, Inc., 59 Temple Place - Suite 330,
-- Boston, MA  02111-1307, USA.
--
-- See the COPYING file located in the top-level-directory of
-- the archive of this library for complete text of license.

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