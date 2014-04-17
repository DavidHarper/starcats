package com.obliquity.starcats.dataloading;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Tycho2SupplementLoader extends AbstractCatalogueLoader {
	private PreparedStatement stmtInsertRow;
	
	private static final int ER_DUP_ENTRY = 1062;

	public static void main(String[] args) {	
		try {
			Tycho2SupplementLoader loader = new Tycho2SupplementLoader();
			
			loader.run();
		}
		catch (Exception e) {
			e.printStackTrace();
			
			System.exit(1);
		}
	}
	
	protected void prepareSQLStatements(Connection conn) throws SQLException {
		String sql = "insert into tycho2 (tyc1,tyc2,tyc3,ra,`dec`,pm_ra,pm_dec,se_ra,se_dec," +
				"bt_mag,se_bt_mag,vt_mag,se_vt_mag,hip_id)" +
				"VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		
		stmtInsertRow = conn.prepareStatement(sql);
	}

	protected void processLine(String line) throws SQLException {
		setIntegerColumn(stmtInsertRow, 1, line, 1, 4);       // TYC1
		setIntegerColumn(stmtInsertRow, 2, line, 6, 10);      // TYC2
		setIntegerColumn(stmtInsertRow, 3, line, 12, 12);     // TYC3
		
		setDoubleColumn(stmtInsertRow, 4, line, 16, 27);      // RAdeg
		setDoubleColumn(stmtInsertRow, 5, line, 29, 40);      // DEdeg
		
		setFloatColumn(stmtInsertRow, 6, line, 42, 48);       // pmRA
		setFloatColumn(stmtInsertRow, 7, line, 50, 56);       // pmDE
		
		setFloatColumn(stmtInsertRow, 8, line, 58, 62);       // e_RAdeg
		setFloatColumn(stmtInsertRow, 9, line, 64, 68);       // e_DEdeg

		setFloatColumn(stmtInsertRow, 10, line, 84, 89);      // BTmag
		setFloatColumn(stmtInsertRow, 11, line, 91, 95);      // e_BTmag

		setFloatColumn(stmtInsertRow, 12, line, 97, 102);     // VTmag
		setFloatColumn(stmtInsertRow, 13, line, 104, 108);    // e_VTmag
		
		setIntegerColumn(stmtInsertRow, 14, line, 1416, 121); // HIP
	
		try {
			stmtInsertRow.execute();
		}
		catch (SQLException e) {
			if (e.getErrorCode() == ER_DUP_ENTRY)
				System.err.println(e.getMessage());
			else
				throw new SQLException(e);
		}
	}	

}
