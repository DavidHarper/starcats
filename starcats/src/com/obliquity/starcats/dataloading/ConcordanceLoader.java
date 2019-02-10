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

// HD-DM-GC-HR-HIP-Bayer-Flamsteed Cross Index
// http://cdsarc.u-strasbg.fr/viz-bin/Cat?IV/27A
// ftp://cdsarc.u-strasbg.fr/pub/cats/IV/27A/catalog.dat

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ConcordanceLoader extends AbstractCatalogueLoader {
	private static final String SOURCE_URL = "ftp://cdsarc.u-strasbg.fr/pub/cats/IV/27A/catalog.dat";
	
	private static final String sources[] = { SOURCE_URL };
	
	private PreparedStatement stmtInsertRow;
	
	public static void main(String[] args) {	
		try {
			ConcordanceLoader loader = new ConcordanceLoader();
			
			loader.run(args.length > 0 ? args : sources);
		}
		catch (Exception e) {
			e.printStackTrace();
			
			System.exit(1);
		}
	}
	
	protected void prepareSQLStatements(Connection conn) throws SQLException {
		String sql = "insert into concordance ( hd_id, dm_id, gc_id, bs_id, hip_id, ra, `dec`, v_mag, " +
				" flamsteed, bayer, constellation) values (?,?,?,?,?,?,?,?,?,?,?)";
		stmtInsertRow = conn.prepareStatement(sql);
	}

	protected void processLine(String line) throws SQLException {
		// Henry Draper catalogue number 
		setIntegerColumn(stmtInsertRow, 1, line, 1, 6);
		
		// Identifier in the Durchmusterung catalogues
		setStringColumn(stmtInsertRow, 2, line, 8, 19);
		
		// Boss General Catalogue identifier
		setIntegerColumn(stmtInsertRow, 3, line, 21, 25);
		
		// Bright Star Catalogue identifier
		setIntegerColumn(stmtInsertRow, 4, line, 27, 30);
		
		// Hipparcos identifier
		setIntegerColumn(stmtInsertRow, 5, line, 32, 37);
		
		// Right Ascension
		setDoubleColumnFromDMS(stmtInsertRow, 6, line, -1, 39, 40, 41, 42, 43,47);
		
		// Declination
		setDoubleColumnFromDMS(stmtInsertRow, 7, line, 49, 50, 51, 52, 53, 54, 57);
		
		// Visual magnitude
		setFloatColumn(stmtInsertRow, 8, line, 59, 63);
		
		// Flamsteed number
		setIntegerColumn(stmtInsertRow, 9, line, 65, 67);
		
		// Bayer designation
		setStringColumn(stmtInsertRow, 10, line, 69, 73);
		
		// Constellation abbreviation
		setStringColumn(stmtInsertRow, 11, line, 75, 77);

		stmtInsertRow.execute();
	}	

}
