package com.obliquity.starcats.dataloading;

// NGC2000 catalogue of non-stellar objects
// http://cdsarc.u-strasbg.fr/viz-bin/Cat?VII/118
// ftp://cdsarc.u-strasbg.fr/pub/cats/VII/118/ngc2000.dat

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class NGC2000Loader extends AbstractCatalogueLoader {
	private static final String SOURCE_URL = "ftp://cdsarc.u-strasbg.fr/pub/cats/VII/118/ngc2000.dat";
	
	private static final String sources[] = { SOURCE_URL };
	
	private PreparedStatement stmtInsertRow;
	
	public static void main(String[] args) {	
		try {
			NGC2000Loader loader = new NGC2000Loader();
			
			loader.run(args.length > 0 ? args : sources);
		}
		catch (Exception e) {
			e.printStackTrace();
			
			System.exit(1);
		}
	}

	protected void prepareSQLStatements(Connection conn) throws SQLException {
		String sql = "insert into ngc2000 (id, ra, `dec`, `type`, constellation, size, mag)" +
				"VALUES(?,?,?,?,?,?,?)";
		
		stmtInsertRow = conn.prepareStatement(sql);		
	}

	protected void processLine(String line) throws SQLException {
		char prefix = line.charAt(0);
		
		String idStr = line.substring(1, 5).trim();
		
		int id = Integer.parseInt(idStr);
		
		if (prefix == 'I')
			id += 10000;
		
		stmtInsertRow.setInt(1, id);
		
		setDoubleColumnFromDMS(stmtInsertRow, 2, line, -1, 11, 12, 14, 17, -1, -1); // RA
		
		setDoubleColumnFromDMS(stmtInsertRow, 3, line, 20, 21, 22, 24, 25, -1, -1);  // Dec

		setStringColumn(stmtInsertRow, 4, line, 7, 9); // Object type

		setStringColumn(stmtInsertRow, 5, line, 30, 32); // Constellation
		
		setFloatColumn(stmtInsertRow, 6, line, 34, 38);  // size
		
		setFloatColumn(stmtInsertRow, 7, line, 41, 44);  // mag
		
		stmtInsertRow.execute();
	}

}
