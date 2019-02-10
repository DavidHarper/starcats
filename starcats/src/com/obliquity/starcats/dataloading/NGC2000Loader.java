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

// NGC2000 catalogue of non-stellar objects
// http://cdsarc.u-strasbg.fr/viz-bin/Cat?VII/118
// ftp://cdsarc.u-strasbg.fr/pub/cats/VII/118/ngc2000.dat

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class NGC2000Loader extends AbstractCatalogueLoader {
	private static final String SOURCE_URL = "ftp://cdsarc.u-strasbg.fr/pub/cats/VII/118/ngc2000.dat";
	
	private static final String sources[] = { SOURCE_URL };
	
	private PreparedStatement stmtInsertRow;
	
	public static void main(String[] args) {	
		try {
			NGC2000Loader loader = new NGC2000Loader();
			
			loader.run(args.length > 0 ? args : sources);
		}
		catch (Exception e) {
			e.printStackTrace();
			
			System.exit(1);
		}
	}

	protected void prepareSQLStatements(Connection conn) throws SQLException {
		String sql = "insert into ngc2000 (id, ra, `dec`, `type`, constellation, size, mag)" +
				"VALUES(?,?,?,?,?,?,?)";
		
		stmtInsertRow = conn.prepareStatement(sql);		
	}

	protected void processLine(String line) throws SQLException {
		char prefix = line.charAt(0);
		
		String idStr = line.substring(1, 5).trim();
		
		int id = Integer.parseInt(idStr);
		
		if (prefix == 'I')
			id += 10000;
		
		stmtInsertRow.setInt(1, id);
		
		setDoubleColumnFromDMS(stmtInsertRow, 2, line, -1, 11, 12, 14, 17, -1, -1); // RA
		
		setDoubleColumnFromDMS(stmtInsertRow, 3, line, 20, 21, 22, 24, 25, -1, -1);  // Dec

		setStringColumn(stmtInsertRow, 4, line, 7, 9); // Object type

		setStringColumn(stmtInsertRow, 5, line, 30, 32); // Constellation
		
		setFloatColumn(stmtInsertRow, 6, line, 34, 38);  // size
		
		setFloatColumn(stmtInsertRow, 7, line, 41, 44);  // mag
		
		stmtInsertRow.execute();
	}

}
