/*
 * starcats - a package for loading stars catalogues into a MySQL database.
 *
 * Copyright (C) 2016-2019 David Harper at obliquity.com
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Library General Public
 * License as published by the Free Software Foundation; either
 * version 2 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Library General Public License for more details.
 * 
 * You should have received a copy of the GNU Library General Public
 * License along with this library; if not, write to the
 * Free Software Foundation, Inc., 59 Temple Place - Suite 330,
 * Boston, MA  02111-1307, USA.
 *
 * See the COPYING file located in the top-level-directory of
 * the archive of this library for complete text of license.
 */

package com.obliquity.starcats.dataloading;

// Tycho-2 catalogue
// http://cdsarc.u-strasbg.fr/viz-bin/Cat?cat=I%2F259
// ftp://cdsarc.u-strasbg.fr/pub/cats/I/259/tyc2.dat.{00..19}.gz

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

public class Tycho2Loader extends AbstractCatalogueLoader {
	private static final String sources[] = { 
			"ftp://cdsarc.u-strasbg.fr/pub/cats/I/259/tyc2.dat.00.gz",
			"ftp://cdsarc.u-strasbg.fr/pub/cats/I/259/tyc2.dat.01.gz",
			"ftp://cdsarc.u-strasbg.fr/pub/cats/I/259/tyc2.dat.02.gz",
			"ftp://cdsarc.u-strasbg.fr/pub/cats/I/259/tyc2.dat.03.gz",
			"ftp://cdsarc.u-strasbg.fr/pub/cats/I/259/tyc2.dat.04.gz",
			"ftp://cdsarc.u-strasbg.fr/pub/cats/I/259/tyc2.dat.05.gz",
			"ftp://cdsarc.u-strasbg.fr/pub/cats/I/259/tyc2.dat.06.gz",
			"ftp://cdsarc.u-strasbg.fr/pub/cats/I/259/tyc2.dat.07.gz",
			"ftp://cdsarc.u-strasbg.fr/pub/cats/I/259/tyc2.dat.08.gz",
			"ftp://cdsarc.u-strasbg.fr/pub/cats/I/259/tyc2.dat.09.gz",
			"ftp://cdsarc.u-strasbg.fr/pub/cats/I/259/tyc2.dat.10.gz",
			"ftp://cdsarc.u-strasbg.fr/pub/cats/I/259/tyc2.dat.11.gz",
			"ftp://cdsarc.u-strasbg.fr/pub/cats/I/259/tyc2.dat.12.gz",
			"ftp://cdsarc.u-strasbg.fr/pub/cats/I/259/tyc2.dat.13.gz",
			"ftp://cdsarc.u-strasbg.fr/pub/cats/I/259/tyc2.dat.14.gz",
			"ftp://cdsarc.u-strasbg.fr/pub/cats/I/259/tyc2.dat.15.gz",
			"ftp://cdsarc.u-strasbg.fr/pub/cats/I/259/tyc2.dat.16.gz",
			"ftp://cdsarc.u-strasbg.fr/pub/cats/I/259/tyc2.dat.17.gz",
			"ftp://cdsarc.u-strasbg.fr/pub/cats/I/259/tyc2.dat.18.gz",
			"ftp://cdsarc.u-strasbg.fr/pub/cats/I/259/tyc2.dat.19.gz",
	};

	private PreparedStatement stmtInsertRow;
	
	public static void main(String[] args) {	
		try {
			Tycho2Loader loader = new Tycho2Loader();
			
			loader.run(args.length > 0 ? args : sources);
		}
		catch (Exception e) {
			e.printStackTrace();
			
			System.exit(1);
		}
	}
	
	protected void prepareSQLStatements(Connection conn) throws SQLException {
		String sql = "insert into tycho2 (tyc1,tyc2,tyc3,ra_mean,dec_mean,pm_ra,pm_dec,se_ra_mean,se_dec_mean," +
				"bt_mag,se_bt_mag,vt_mag,se_vt_mag,hip_id,ra,`dec`,epochRA,epochDec,se_ra,se_dec)" +
				"VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		
		stmtInsertRow = conn.prepareStatement(sql);
	}

	protected void processLine(String line) throws SQLException {
		setIntegerColumn(stmtInsertRow, 1, line, 1, 4);       // TYC1
		setIntegerColumn(stmtInsertRow, 2, line, 6, 10);      // TYC2
		setIntegerColumn(stmtInsertRow, 3, line, 12, 12);     // TYC3
		
		boolean hasMeanPosition = line.charAt(13) != 'X';
		
		if (hasMeanPosition)
			setDoubleColumn(stmtInsertRow, 4, line, 16, 27);      // RAmdeg
		else
			stmtInsertRow.setNull(4, Types.DOUBLE);
		
		if (hasMeanPosition)
			setDoubleColumn(stmtInsertRow, 5, line, 29, 40);      // DEmdeg
		else
			stmtInsertRow.setNull(5, Types.DOUBLE);
		
		setFloatColumn(stmtInsertRow, 6, line, 42, 48);       // pmRA
		setFloatColumn(stmtInsertRow, 7, line, 50, 56);       // pmDE
		
		setFloatColumn(stmtInsertRow, 8, line, 58, 60);       // e_RAmdeg
		setFloatColumn(stmtInsertRow, 9, line, 62, 64);       // e_DEmdeg

		setFloatColumn(stmtInsertRow, 10, line, 111, 116);    // BTmag
		setFloatColumn(stmtInsertRow, 11, line, 118, 122);    // e_BTmag

		setFloatColumn(stmtInsertRow, 12, line, 124, 129);    // VTmag
		setFloatColumn(stmtInsertRow, 13, line, 118, 122);    // e_VTmag
		
		setIntegerColumn(stmtInsertRow, 14, line, 143, 148);  // HIP
		
		setDoubleColumn(stmtInsertRow, 15, line, 153, 164);   // RAdeg
		setDoubleColumn(stmtInsertRow, 16, line, 166,177);    // DEdeg
		
		setFloatColumn(stmtInsertRow, 17, line, 179, 182);    // epRA
		setFloatColumn(stmtInsertRow, 18, line, 184, 187);    // epDec
		
		setFloatColumn(stmtInsertRow, 19, line, 189, 193);    // e_RAdeg
		setFloatColumn(stmtInsertRow, 20, line, 195, 199);    // e_DEdeg
	
		stmtInsertRow.execute();
	}	
}
