-- starcats - a package for loading stars catalogues into a MySQL database.
--
-- Copyright (C) 2016-2021 David Harper at obliquity.com
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

create table xz_zodiacal (
  xz_id int unsigned not null primary key,
  ra double not null,
  `dec` double not null,
  pm_ra double default 0.0,
  pm_dec double default 0.0,
  v_mag double default 99.9,
  parallax double default 0.0,
  rv double default 0.0,
  zc_id int unsigned,
  sao_id int unsigned,
  hd_id int unsigned,
  key (ra),
  key (v_mag, ra),
  key (zc_id),
  key (sao_id),
  key (hd_id)
) engine=InnoDB
