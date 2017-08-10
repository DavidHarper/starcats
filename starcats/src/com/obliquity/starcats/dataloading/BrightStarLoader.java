package com.obliquity.starcats.dataloading;

// Yale Bright Star Catalogue
// http://vizier.u-strasbg.fr/viz-bin/VizieR?-source=V/50
// ftp://cdsarc.u-strasbg.fr/pub/cats/V/50/catalog.gz

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class BrightStarLoader extends AbstractCatalogueLoader {
	private static final String SOURCE_URL = "ftp://cdsarc.u-strasbg.fr/pub/cats/V/50/catalog.gz";
	
	private static final String sources[] = { SOURCE_URL };
	
	private PreparedStatement stmtInsertRow;
	
	private static final int ER_BAD_NULL_RECORD = 1048;
	
	public static void main(String[] args) {	
		try {
			BrightStarLoader loader = new BrightStarLoader();
			
			loader.run(args.length > 0 ? args : sources);
		}
		catch (Exception e) {
			e.printStackTrace();
			
			System.exit(1);
		}
	}
	
	protected void prepareSQLStatements(Connection conn) throws SQLException {
		String sql = "insert into bright_star (bs_id, name, dm_id, hd_id, sao_id, fk5_id, ra, `dec`, v_mag," + 
				"colour_BV, colour_UB, colour_RI, spectral_type, pm_ra, pm_dec, parallax, radial_velocity)" +
				"VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		
		stmtInsertRow = conn.prepareStatement(sql);
	}

	protected void processLine(String line) throws SQLException {
		setIntegerColumn(stmtInsertRow, 1, line, 1, 4);  // bs_id
		
		setStringColumn(stmtInsertRow, 2, line, 5, 14);  // name

		setStringColumn(stmtInsertRow, 3, line, 15, 25); // dm_id
		
		setIntegerColumn(stmtInsertRow, 4, line, 26, 31);  // hd_id
		
		setIntegerColumn(stmtInsertRow, 5, line, 32, 37);  // sao_id
		
		setIntegerColumn(stmtInsertRow, 6, line, 38, 41);  // fk5_id
	
		setDoubleColumnFromDMS(stmtInsertRow, 7, line, -1, 76, 77, 78, 79, 80, 83); // RA
		
		setDoubleColumnFromDMS(stmtInsertRow, 8, line, 84, 85, 86, 87, 88, 89, 90);  // Dec

		setFloatColumn(stmtInsertRow, 9, line, 103, 107);  // v_mag

		setFloatColumn(stmtInsertRow, 10, line, 110, 114);  // colour_BV

		setFloatColumn(stmtInsertRow, 11, line, 116, 120);  // colour_UB

		setFloatColumn(stmtInsertRow, 12, line, 122, 126);  // colour_RI
		
		setStringColumn(stmtInsertRow, 13, line, 128, 147);  // spectral_type

		setFloatColumn(stmtInsertRow, 14, line, 149, 154);  // pm_ra

		setFloatColumn(stmtInsertRow, 15, line, 155, 160);  // pm_dec

		setFloatColumn(stmtInsertRow, 16, line, 162, 166);  // parallax

		setFloatColumn(stmtInsertRow, 17, line, 167, 170);  // radial_velocity
		
		try {
			stmtInsertRow.execute();
		}
		catch (SQLException e) {
			if (e.getErrorCode() == ER_BAD_NULL_RECORD) {
				System.err.println(e.getMessage());
				System.err.println("# " + line);
			} else
				throw new SQLException(e);
		}
	}

}