package com.obliquity.starcats.dataloading;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Tycho2Loader extends AbstractCatalogueLoader {
	private PreparedStatement stmtInsertRow;
	
	public static void main(String[] args) {	
		try {
			Tycho2Loader loader = new Tycho2Loader();
			
			loader.run();
		}
		catch (Exception e) {
			e.printStackTrace();
			
			System.exit(1);
		}
	}
	
	protected void prepareSQLStatements(Connection conn) throws SQLException {
		String sql = "insert into tycho2 (tyc1,tyc2,tyc3,ra_mean,dec_mean,pm_ra,pm_dec,se_ra_mean,se_dec_mean," +
				"bt_mag,se_bt_mag,vt_mag,se_vt_mag,hip_id,ra,`dec`,se_ra,se_dec)" +
				"VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		
		stmtInsertRow = conn.prepareStatement(sql);
	}

	protected void processLine(String line) throws SQLException {
		setIntegerColumn(stmtInsertRow, 1, line, 1, 4);       // TYC1
		setIntegerColumn(stmtInsertRow, 2, line, 6, 10);      // TYC2
		setIntegerColumn(stmtInsertRow, 3, line, 12, 12);     // TYC3
		
		setDoubleColumn(stmtInsertRow, 4, line, 16, 27);      // RAmdeg
		setDoubleColumn(stmtInsertRow, 5, line, 29, 40);      // DEmdeg
		
		setFloatColumn(stmtInsertRow, 6, line, 42, 48);       // pmRA
		setFloatColumn(stmtInsertRow, 7, line, 50, 56);       // pmDE
		
		setFloatColumn(stmtInsertRow, 8, line, 58, 60);       // e_RAmdeg
		setFloatColumn(stmtInsertRow, 9, line, 62, 64);       // e_DEmdeg

		setFloatColumn(stmtInsertRow, 10, line, 111, 116);    // BTmag
		setFloatColumn(stmtInsertRow, 11, line, 118, 122);    // e_BTmag

		setFloatColumn(stmtInsertRow, 12, line, 124, 129);    // VTmag
		setFloatColumn(stmtInsertRow, 13, line, 118, 122);    // e_VTmag
		
		setIntegerColumn(stmtInsertRow, 14, line, 143, 148);  // HIP
		
		setDoubleColumn(stmtInsertRow, 15, line, 153, 164);   // RAdeg
		setDoubleColumn(stmtInsertRow, 16, line, 166,177);    // DEdeg
		
		setFloatColumn(stmtInsertRow, 17, line, 179, 182);    // e_RAdeg
		setFloatColumn(stmtInsertRow, 18, line, 184, 187);    // e_DEdeg
	
		stmtInsertRow.execute();
	}	
}
