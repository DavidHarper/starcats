package com.obliquity.starcats.dataloading;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class HenryDraperLoader extends AbstractCatalogueLoader {
	private PreparedStatement stmtInsertRow;
	
	public static void main(String[] args) {	
		try {
			HenryDraperLoader loader = new HenryDraperLoader();
			
			loader.run();
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
		setIntegerColumn(stmtInsertRow, 1, line, 1, 6);       // HD number
		
		setStringColumn(stmtInsertRow, 2, line, 7, 18);       // Durchmusterung identifier
		
		// The Henry Draper catalogue stores RA as hours and deci-minutes
		double raHours = getFieldAsDouble(line, 19, 20);
		double raDeciMinutes = getFieldAsDouble(line, 21, 23);
		
		stmtInsertRow.setDouble(3, raHours+raDeciMinutes/600.0);
		
		setDoubleColumnFromDMS(stmtInsertRow, 4, line, 24, 25, 26, 27, 28, -1, -1);      // Dec

		setDoubleColumn(stmtInsertRow, 5, line, 30, 34);      // Photovisual magnitude

		try {
			setDoubleColumn(stmtInsertRow, 6, line, 37, 41);      // Photographic magnitude
		}
		catch (NumberFormatException e) {
			System.err.println("Bad photovisual magnitude");
		}
		
		setStringColumn(stmtInsertRow, 7, line, 43, 45);      // Spectral type

		stmtInsertRow.execute();
	}	
}
