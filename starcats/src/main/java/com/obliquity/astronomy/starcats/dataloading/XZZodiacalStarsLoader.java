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

package com.obliquity.astronomy.starcats.dataloading;

// XZ Catalogue of Zodiacal Stars
// https://cdsarc.cds.unistra.fr/viz-bin/Cat?I/291
// https://cdsarc.cds.unistra.fr/ftp/I/291/xz80q.dat

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class XZZodiacalStarsLoader extends AbstractCatalogueLoader {
	private static final String sources[] = { 
			"https://cdsarc.cds.unistra.fr/ftp/I/291/xz80q.dat"
	};
	
	private PreparedStatement stmtInsertRow;

	
	public static void main(String[] args) {	
		try {
			XZZodiacalStarsLoader loader = new XZZodiacalStarsLoader();
			
			loader.run(args.length > 0 ? args : sources);
		}
		catch (Exception e) {
			e.printStackTrace();
			
			System.exit(1);
		}
	}

	protected void prepareSQLStatements(Connection conn) throws SQLException {
		String sql = "insert into xz_zodiacal (xz_id,ra,`dec`,pm_ra,pm_dec,v_mag,zc_id,hd_id)" +
				"VALUES(?,?,?,?,?,?,?,?)";
		
		stmtInsertRow = conn.prepareStatement(sql);
	}

	protected void processLine(String line) throws SQLException {
		char code = line.charAt(6);
		
		if (code == 'L' || code == 'X' || code == 'E' || code == ' ')
			return;
		
		setIntegerColumn(stmtInsertRow, 1, line, 1, 6);
		
		setDoubleColumnFromDMS(stmtInsertRow, 2, line, 0, 26, 27, 28, 29, 30, 36);

		setDoubleColumnFromDMS(stmtInsertRow, 3, line, 45, 46, 47, 48, 49, 50, 55);
		
		setDoubleColumn(stmtInsertRow, 4, line, 37, 44);
		
		setDoubleColumn(stmtInsertRow, 5, line, 56, 63);
		
		setDoubleColumn(stmtInsertRow, 6, line, 20, 24);
		
		setIntegerColumn(stmtInsertRow, 7, line, 148,151);
		
		setIntegerColumn(stmtInsertRow, 8, line, 167, 172);
		
		stmtInsertRow.execute();
	}
}
