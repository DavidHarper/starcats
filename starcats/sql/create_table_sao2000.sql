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

create table sao2000 (
    sao_id int unsigned not null primary key,
    hd_id int unsigned,
    ra2000 double not null,
    dec2000 double not null,
    v_mag float,
    p_mag float,
    spectral_type varchar(3),
    pmRA2000 double,
    pmDec2000 double,
    key (ra2000),
    key (v_mag)
) engine=InnoDB