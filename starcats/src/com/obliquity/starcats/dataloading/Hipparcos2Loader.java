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

// Hipparcos 2 catalogue
// van Leeuwen, F. et al (2007) A&A 474, 653
// doi:10.1051/0004-6361:20078357
// http://cdsarc.u-strasbg.fr/viz-bin/cat/I/311
// http://cdsarc.u-strasbg.fr/ftp/I/311/hip2.dat.gz

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Hipparcos2Loader extends AbstractCatalogueLoader {
	private static final String SOURCE_URL = "http://cdsarc.u-strasbg.fr/ftp/I/311/hip2.dat.gz";
	
	private static final String sources[] = { SOURCE_URL };
	
	private PreparedStatement stmtInsertRow;
	
	public static void main(String[] args) {	
		try {
			Hipparcos2Loader loader = new Hipparcos2Loader();
			
			loader.run(args.length > 0 ? args : sources);
		}
		catch (Exception e) {
			e.printStackTrace();
			
			System.exit(1);
		}
	}

	protected void prepareSQLStatements(Connection conn) throws SQLException {
		String sql = "insert into hipparcos2 (hip_id,ra,`dec`,parallax,pm_ra,pm_dec,hp_mag,bv_colour)" +
				" values (?,?,?,?,?,?,?,?)";
		
		stmtInsertRow = conn.prepareStatement(sql);
	}
	
	protected void processLine(String line) throws SQLException {
		// Hipparcos catalogue number
		setIntegerColumn(stmtInsertRow, 1, line, 1, 6);

		// Right Ascension (J2000)
		setDoubleColumn(stmtInsertRow, 2, line, 16, 28);

		// Declination (J2000)
		setDoubleColumn(stmtInsertRow, 3, line, 30, 42);
		
		// Parallax (mas)
		setDoubleColumn(stmtInsertRow, 4, line, 44, 50);
		
		// Proper motion in RA (mas)
		setDoubleColumn(stmtInsertRow, 5, line, 52, 59);
		
		// Proper motion in Dec (mas)
		setDoubleColumn(stmtInsertRow, 6, line, 61, 68);

		// Hipparcos magnitude
		setFloatColumn(stmtInsertRow, 7, line, 130, 136);
		
		// B-V colour index
		setFloatColumn(stmtInsertRow, 8, line, 153, 158);
		
		stmtInsertRow.execute();
	}
}
