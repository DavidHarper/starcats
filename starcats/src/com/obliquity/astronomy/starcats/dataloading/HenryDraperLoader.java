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

// Henry Draper catalogue
// http://vizier.u-strasbg.fr/viz-bin/VizieR?-source=III/135
// ftp://cdsarc.u-strasbg.fr/pub/cats/III/135A/catalog.dat.gz

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class HenryDraperLoader extends AbstractCatalogueLoader {
	private static final String SOURCE_URL = "ftp://cdsarc.u-strasbg.fr/pub/cats/III/135A/catalog.dat.gz";
	
	private static final String sources[] = { SOURCE_URL };
	
	private PreparedStatement stmtInsertRow;
	
	public static void main(String[] args) {	
		try {
			HenryDraperLoader loader = new HenryDraperLoader();
			
			loader.run(args.length > 0 ? args : sources);
		}
		catch (Exception e) {
			e.printStackTrace();
			
			System.exit(1);
		}
	}
	
	protected void prepareSQLStatements(Connection conn) throws SQLException {
		String sql = "insert into henry_draper (hd_id, dm_id, ra, `dec`, pv_mag, pt_mag, spectral_type)" +
				"VALUES(?,?,?,?,?,?,?)";
		
		stmtInsertRow = conn.prepareStatement(sql);
	}

	protected void processLine(String line) throws SQLException {
		// Henry Draper catalgue number 
		setIntegerColumn(stmtInsertRow, 1, line, 1, 6);
		
		// Identifier in the Durchmusterung catalogues
		setStringColumn(stmtInsertRow, 2, line, 7, 18);
		
		// Right Ascension
		//
		// The Henry Draper catalogue stores RA as hours and deci-minutes
		double raHours = getFieldAsDouble(line, 19, 20);
		double raDeciMinutes = getFieldAsDouble(line, 21, 23);
		
		stmtInsertRow.setDouble(3, raHours+raDeciMinutes/600.0);
		
		// Declination
		setDoubleColumnFromDMS(stmtInsertRow, 4, line, 24, 25, 26, 27, 28, -1, -1);
		
		// Photovisual magnitude
		double pv_mag = getFieldAsDouble(line, 30, 34);
		
		if (Double.isNaN(pv_mag))
			stmtInsertRow.setNull(5, java.sql.Types.DOUBLE);
		else
			stmtInsertRow.setDouble(5, pv_mag);
		
		// Photographic magnitude
		double pt_mag =  getFieldAsDouble(line, 37, 41);
		
		// Some stars have pt_mag < 2.0 and pv_mag > 8.0, which is clearly impossible, so
		// we reset pt_mag to NaN for these stars to force the value to be NULL in the
		// database.
		
		if (pt_mag < 2.0 && (Double.isNaN(pv_mag) || pv_mag > 6.0))
			pt_mag = Double.NaN;
		
		if (Double.isNaN(pt_mag))
			stmtInsertRow.setNull(6, java.sql.Types.DOUBLE);
		else
			stmtInsertRow.setDouble(6, pt_mag);
		
		// Spectral type
		setStringColumn(stmtInsertRow, 7, line, 43, 45);

		stmtInsertRow.execute();
	}	
}
