package com.obliquity.starcats.dataloading;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ConcordanceLoader extends AbstractCatalogueLoader {
	private PreparedStatement stmtInsertRow;
	
	public static void main(String[] args) {	
		try {
			ConcordanceLoader loader = new ConcordanceLoader();
			
			loader.run();
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
