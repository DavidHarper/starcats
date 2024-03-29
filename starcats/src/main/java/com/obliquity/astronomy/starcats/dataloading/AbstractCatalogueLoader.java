/*
 * starcats - a package for loading stars catalogues into a MySQL database.
 *
 * Copyright (C) 2016-2019 David Harper at obliquity.com
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Library General Public
 * License as published by the Free Software Foundation; either
 * version 2 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Library General Public License for more details.
 * 
 * You should have received a copy of the GNU Library General Public
 * License along with this library; if not, write to the
 * Free Software Foundation, Inc., 59 Temple Place - Suite 330,
 * Boston, MA  02111-1307, USA.
 *
 * See the COPYING file located in the top-level-directory of
 * the archive of this library for complete text of license.
 */

package com.obliquity.astronomy.starcats.dataloading;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.io.BufferedReader;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import java.util.Properties;
import java.util.zip.GZIPInputStream;

public abstract class AbstractCatalogueLoader {
	protected Connection conn;
	
	private static final int LINES_PER_COMMIT = 1000;
	
	protected void run(String[] sources) throws ClassNotFoundException, SQLException, IOException {
		conn = getConnection("starcats");
		
		prepareSQLStatements(conn);
		
		load(sources);
				
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
		
		String dbtype = myprops.getProperty(prefix + ".dbtype", "mysql");

		String url = "jdbc:" + dbtype + "://" + host + ":" + port + "/" + database;
		
		String options = myprops.getProperty(prefix + ".options");
		
		if (options != null)
			url += "?" + options;

		String driver = myprops.getProperty(prefix + ".driver", "com.mysql.jdbc.Driver");

		String username = myprops.getProperty(prefix + ".username");
		String password = myprops.getProperty(prefix + ".password");

		Class.forName(driver);

		Connection conn = DriverManager.getConnection(url, username, password);
		
		conn.setAutoCommit(false);
		
		return conn;
	}
	
	protected abstract void prepareSQLStatements(Connection conn) throws SQLException;

	public void load(String[] sources) throws IOException, SQLException {
		for (String source : sources)
			load(source);
	}
	
	public void load(String source) throws IOException, SQLException {
		System.out.println("Loading from source " + source);
		
		InputStream is = getInputStreamForSource(source);
		
		BufferedReader br = new BufferedReader(new InputStreamReader(is));
		
		load(br);
		
		is.close();
	}
	
	private InputStream getInputStreamForSource(String source) throws IOException {
		return isURL(source) ? getInputStreamForURL(source) : getInputStreamForFilename(source);
	}
	
	private boolean isURL(String source) {
		return source.startsWith("file:") || source.startsWith("http:") || source.startsWith("https:") || source.startsWith("ftp:");
	}
	
	private InputStream getInputStreamForURL(String source) throws IOException {
		URL url = new URL(source);

		InputStream is = url.openStream();
		
		if (source.endsWith(".gz"))
			is = new GZIPInputStream(is);

		return is;
	}
	
	private InputStream getInputStreamForFilename(String source) throws FileNotFoundException {
		return new FileInputStream(source);
	}
	
	public void load(BufferedReader br) throws IOException, SQLException {
		int lineCount = 0;
		
		for (String line = br.readLine(); line != null; line = br.readLine()) {
			lineCount++;
					
			try {
				processLine(line);
			}
			catch (SQLException e) {
				System.err.println("An error occurred at line " + lineCount + " : " + e.getMessage());
				throw e;
			}
			
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
		
		if (Double.isNaN(value)) {
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
		String signumField = signumPos > 0 ? getField(line, signumPos, signumPos) : null;
		
		String degreeField = getField(line, degreeStartPos, degreeEndPos);
		
		String minuteField = getField(line, minuteStartPos, minuteEndPos);
		
		String secondField = secondStartPos > 0 ? getField(line, secondStartPos, secondEndPos) : null;
		
		if (degreeField != null && minuteField != null) {
			double value = Double.parseDouble(degreeField) + Double.parseDouble(minuteField)/60.0 +
					(secondField == null ? 0.0 : Double.parseDouble(secondField)/3600.0);
		
			if (signumField != null && signumField.equalsIgnoreCase("-"))
				value = -value;
		
			stmt.setDouble(index, value);
		} else {
			stmt.setNull(index, java.sql.Types.DOUBLE);
		}
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
