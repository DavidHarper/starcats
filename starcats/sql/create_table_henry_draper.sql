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