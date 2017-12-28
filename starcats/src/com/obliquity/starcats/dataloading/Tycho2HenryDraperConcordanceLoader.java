package com.obliquity.starcats.dataloading;

// Tycho-2 to Henry Draper concordance
// Fabricius, C., et al (2002), A&A 386, 709
// https://www.aanda.org/articles/aa/pdf/2002/17/aah3397.pdf
// http://cdsarc.u-strasbg.fr/viz-bin/qcat?J/A+A/386/709
// ftp://cdsarc.u-strasbg.fr/pub/cats/IV/25/tyc2_hd.dat.gz

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Tycho2HenryDraperConcordanceLoader extends AbstractCatalogueLoader {
	private static final String sources[] = { 
			"ftp://cdsarc.u-strasbg.fr/pub/cats/IV/25/tyc2_hd.dat.gz"
	};

	private PreparedStatement stmtInsertRow;
	
	public static void main(String[] args) {	
		try {
			Tycho2HenryDraperConcordanceLoader loader = new Tycho2HenryDraperConcordanceLoader();
			
			loader.run(args.length > 0 ? args : sources);
		}
		catch (Exception e) {
			e.printStackTrace();
			
			System.exit(1);
		}
	}

	protected void prepareSQLStatements(Connection conn) throws SQLException {
		String sql = "insert into tycho2henrydraper (tyc1,tyc2,tyc3,hd_id)" +
				"VALUES(?,?,?,?)";
		
		stmtInsertRow = conn.prepareStatement(sql);		
	}

	protected void processLine(String line) throws SQLException {
		setIntegerColumn(stmtInsertRow, 1, line, 1, 4);       // TYC1
		setIntegerColumn(stmtInsertRow, 2, line, 6, 10);      // TYC2
		setIntegerColumn(stmtInsertRow, 3, line, 12, 12);     // TYC3
		setIntegerColumn(stmtInsertRow, 4, line, 15, 20);     // HD number
		
		stmtInsertRow.execute();		
	}

}
