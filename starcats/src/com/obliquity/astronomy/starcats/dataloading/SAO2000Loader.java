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

// SAO2000 catalogue
// http://cdsarc.u-strasbg.fr/viz-bin/Cat?I/131A
// ftp://cdsarc.u-strasbg.fr/pub/cats/I/131A/sao.dat.gz

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class SAO2000Loader extends AbstractCatalogueLoader {
	private static final String SOURCE_URL = "ftp://cdsarc.u-strasbg.fr/pub/cats/I/131A/sao.dat.gz";
	
	private static final String sources[] = { SOURCE_URL };
	
	private PreparedStatement stmtInsertRow;
	
	public static void main(String[] args) {	
		try {
			SAO2000Loader loader = new SAO2000Loader();
			
			loader.run(args.length > 0 ? args : sources);
		}
		catch (Exception e) {
			e.printStackTrace();
			
			System.exit(1);
		}
	}
	
	protected void prepareSQLStatements(Connection conn) throws SQLException {
		String sql = "insert into sao2000 (sao_id, hd_id, ra, `dec`, v_mag, p_mag, spectral_type)" +
				"VALUES(?,?,?,?,?,?,?)";
		
		stmtInsertRow = conn.prepareStatement(sql);
	}

	protected void processLine(String line) throws SQLException {
		char deleted = line.charAt(6);
		
		if (deleted == 'D')
			return;
		
		// SAO catalogue number
		setIntegerColumn(stmtInsertRow, 1, line, 1, 6);
		
		// Henry Draper catalgue number 
		setIntegerColumn(stmtInsertRow, 2, line, 118, 123);
				
		// Right Ascension (J2000)
		double ra = getFieldAsDouble(line, 184, 193) * 12.0/Math.PI;
		stmtInsertRow.setDouble(3, ra);
		
		// Declination
		double dec = getFieldAsDouble(line, 194, 204) * 180.0/Math.PI;
		stmtInsertRow.setDouble(4, dec);
		
		// Visual magnitude
		double v_mag = getFieldAsDouble(line, 81, 84);
		
		if (Double.isNaN(v_mag))
			stmtInsertRow.setNull(5, java.sql.Types.DOUBLE);
		else
			stmtInsertRow.setDouble(5, v_mag);
		
		// Photographic magnitude
		double p_mag =  getFieldAsDouble(line, 77, 80);
				
		if (Double.isNaN(p_mag))
			stmtInsertRow.setNull(6, java.sql.Types.DOUBLE);
		else
			stmtInsertRow.setDouble(6, p_mag);
		
		// Spectral type
		setStringColumn(stmtInsertRow, 7, line, 85, 87);

		stmtInsertRow.execute();
	}	

}
