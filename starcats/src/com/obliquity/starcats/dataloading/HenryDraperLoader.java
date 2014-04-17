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
		
		double pv_mag = getFieldAsDouble(line, 30, 34);
		
		if (Double.isNaN(pv_mag))
			stmtInsertRow.setNull(5, java.sql.Types.DOUBLE);
		else
			stmtInsertRow.setDouble(5, pv_mag);
		
		double pt_mag =  getFieldAsDouble(line, 37, 41);
		
		if (Double.isNaN(pt_mag) || (pt_mag == 0.0 && pv_mag > 6.0))
			stmtInsertRow.setNull(6, java.sql.Types.DOUBLE);
		else
			stmtInsertRow.setDouble(6, pt_mag);
		
		setStringColumn(stmtInsertRow, 7, line, 43, 45);      // Spectral type

		stmtInsertRow.execute();
	}	
}
