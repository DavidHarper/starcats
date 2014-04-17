package com.obliquity.starcats.dataloading;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.BufferedReader;
import java.io.FileReader;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import java.util.Properties;

import javax.swing.JFileChooser;

public abstract class AbstractCatalogueLoader {
	protected Connection conn;
	
	private static final int LINES_PER_COMMIT = 1000;
	
	protected void run() throws ClassNotFoundException, SQLException, IOException {
		conn = getConnection("starcats");
		
		prepareSQLStatements(conn);
		
		JFileChooser chooser = new JFileChooser();
		
		chooser.setMultiSelectionEnabled(true);
		
		File cwd = new File(System.getProperty("starcats.base"));
		chooser.setCurrentDirectory(cwd);

		int returnVal = chooser.showOpenDialog(null);

		if (returnVal == JFileChooser.APPROVE_OPTION) {
			File[] files = chooser.getSelectedFiles();
			
			try {
				load(files);
			}
			catch (Exception e) {
				e.printStackTrace();
			}
		} else
			System.exit(1);
		
		conn.close();
	}
	
	private Connection getConnection(String prefix) throws SQLException,
			IOException, ClassNotFoundException {
		String base = System.getProperty("starcats.base");
		
		String filename = base + "/" + prefix + ".props";
		
		File propsFile = new File(filename);
		
		InputStream is = new FileInputStream(propsFile);
		
		Properties myprops = new Properties();
		myprops.load(is);
		is.close();

		String host = myprops.getProperty(prefix + ".host");
		String port = myprops.getProperty(prefix + ".port");
		String database = myprops.getProperty(prefix + ".database");

		String url = "jdbc:mysql://" + host + ":" + port + "/" + database;

		String driver = "com.mysql.jdbc.Driver";

		String username = myprops.getProperty(prefix + ".username");
		String password = myprops.getProperty(prefix + ".password");

		Class.forName(driver);

		Connection conn = DriverManager.getConnection(url, username, password);
		
		conn.setAutoCommit(false);
		
		return conn;
	}
	
	protected abstract void prepareSQLStatements(Connection conn) throws SQLException;

	public void load(File[] files) throws IOException, SQLException {
		for (File file : files)
			load(file);
	}
	
	private int lineCount;
	
	public void load(File file) throws IOException, SQLException {
		System.out.println("Loading file " + file.getName());
		
		BufferedReader br = new BufferedReader(new FileReader(file));
		
		lineCount = 0;
		
		for (String line = br.readLine(); line != null; line = br.readLine()) {
			lineCount++;
						
			processLine(line);
			
			if ((lineCount % 1000) == 0)
				System.out.println(" " + lineCount);
			else if ((lineCount % 100) == 0)
				System.out.print('.');
			
			if ((lineCount % LINES_PER_COMMIT) == 0)
				conn.commit();
		}
		
		br.close();

		conn.commit();
		
		System.out.println("\nLines processed: " + lineCount + "\n");
	}
	
	protected abstract void processLine(String line) throws SQLException;
	
	protected void setIntegerColumn(PreparedStatement stmt, int index, String line, int startPos, int endPos) throws SQLException {
		String field = getField(line, startPos, endPos);
		
		if (field == null) {
			stmt.setNull(index, java.sql.Types.INTEGER);
		} else {
			stmt.setInt(index, Integer.parseInt(field));
		}
	}
	
	protected void setFloatColumn(PreparedStatement stmt, int index, String line, int startPos, int endPos) throws SQLException {
		String field = getField(line, startPos, endPos);
		
		if (field == null) {
			stmt.setNull(index, java.sql.Types.FLOAT);
		} else {
			stmt.setFloat(index, Float.parseFloat(field));
		}
	}
	
	protected void setDoubleColumn(PreparedStatement stmt, int index, String line, int startPos, int endPos) throws SQLException {
		double value = getFieldAsDouble(line, startPos, endPos);
		
		if (value == Double.NaN) {
			stmt.setNull(index, java.sql.Types.DOUBLE);
		} else {
			stmt.setDouble(index, value);
		}
	}	
	
	protected void setStringColumn(PreparedStatement stmt, int index, String line, int startPos, int endPos) throws SQLException {
		String field = getField(line, startPos, endPos);
		
		if (field == null) {
			stmt.setNull(index, java.sql.Types.CHAR);
		} else {
			stmt.setString(index, field);
		}
	}
	
	protected void setDoubleColumnFromDMS(PreparedStatement stmt, int index, String line, int signumPos,
			int degreeStartPos, int degreeEndPos, int minuteStartPos, int minuteEndPos, int secondStartPos, int secondEndPos)
			throws SQLException {
		String signumField = signumPos > 0 ? getField(line, signumPos, signumPos + 1) : null;
		
		String degreeField = getField(line, degreeStartPos, degreeEndPos);
		
		String minuteField = getField(line, minuteStartPos, minuteEndPos);
		
		String secondField = secondStartPos > 0 ? getField(line, secondStartPos, secondEndPos) : null;
		
		double value = Double.parseDouble(degreeField) + Double.parseDouble(minuteField)/60.0 +
				(secondField == null ? 0.0 : Double.parseDouble(secondField)/3600.0);
		
		if (signumField != null && signumField.equalsIgnoreCase("-"))
			value = - value;
		
		stmt.setDouble(index, value);
	}
	
	private String getField(String line, int startPos, int endPos) {
		int linelen = line.length();
		
		if (startPos > linelen)
			return null;
		
		int beginIndex = startPos - 1;
		int endIndex = endPos > linelen ? linelen : endPos;
		
		String field = line.substring(beginIndex, endIndex).trim();
		
		return field.length() == 0 ? null : field;
	}

	protected double getFieldAsDouble(String line, int startPos, int endPos) {
		String field = getField(line, startPos, endPos);
		
		if (field == null)
			return Double.NaN;
		
		try {
			return Double.parseDouble(field);
		}
		catch (NumberFormatException e) {
			return Double.NaN;
		}
	}
}
