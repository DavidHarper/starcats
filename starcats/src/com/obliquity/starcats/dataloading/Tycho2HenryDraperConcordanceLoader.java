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

// Tycho-2 to Henry Draper concordance
// Fabricius, C., et al (2002), A&A 386, 709
// https://www.aanda.org/articles/aa/pdf/2002/17/aah3397.pdf
// http://cdsarc.u-strasbg.fr/viz-bin/qcat?J/A+A/386/709
// ftp://cdsarc.u-strasbg.fr/pub/cats/IV/25/tyc2_hd.dat.gz

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Tycho2HenryDraperConcordanceLoader extends AbstractCatalogueLoader {
	private static final String sources[] = { 
			"ftp://cdsarc.u-strasbg.fr/pub/cats/IV/25/tyc2_hd.dat.gz"
	};

	private PreparedStatement stmtInsertRow;
	
	public static void main(String[] args) {	
		try {
			Tycho2HenryDraperConcordanceLoader loader = new Tycho2HenryDraperConcordanceLoader();
			
			loader.run(args.length > 0 ? args : sources);
		}
		catch (Exception e) {
			e.printStackTrace();
			
			System.exit(1);
		}
	}

	protected void prepareSQLStatements(Connection conn) throws SQLException {
		String sql = "insert into tycho2henrydraper (tyc1,tyc2,tyc3,hd_id)" +
				"VALUES(?,?,?,?)";
		
		stmtInsertRow = conn.prepareStatement(sql);		
	}

	protected void processLine(String line) throws SQLException {
		setIntegerColumn(stmtInsertRow, 1, line, 1, 4);       // TYC1
		setIntegerColumn(stmtInsertRow, 2, line, 6, 10);      // TYC2
		setIntegerColumn(stmtInsertRow, 3, line, 12, 12);     // TYC3
		setIntegerColumn(stmtInsertRow, 4, line, 15, 20);     // HD number
		
		stmtInsertRow.execute();		
	}

}
