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
