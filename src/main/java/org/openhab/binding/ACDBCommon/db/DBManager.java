/**
 * Copyright (c) 2010-2016, openHAB.org and others.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 */

package org.openhab.binding.ACDBCommon.db;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.openhab.binding.ACDBCommon.internal.ACDBBinding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * DB processing
 * </p>
 *
 * @author Kazuhiro Matsuda
 */
public class DBManager {
	/**
	 * logger 
	 */
	private static final Logger logger = LoggerFactory.getLogger(ACDBBinding.class);

	/**
	 * DBserver
	 */
	public static Map<String, ServerInfo> serverCache = new HashMap<String, ServerInfo>();

	/**
	 * close DB connection
	 *
	 * @throws Exception
	 */
	public static void closeConnection() throws Exception {
		for (Map.Entry<String, ServerInfo> mapI : DBManager.serverCache.entrySet()) {
			ServerInfo serverI = mapI.getValue();
			if (serverI.getConnection() != null && !serverI.getConnection().isClosed()) {
				serverI.getConnection().close();
			}
			serverI.setConnection(null);
		}
	}

	/**
	 * select data
	 *
	 * @param selectSql
	 * @return
	 * @throws Exception
	 */
	public static String select(String selectSql) throws Exception {
		SqlResult sqlResult = sqlParse(selectSql);
		String sql = sqlResult.getSql();
		ServerInfo server = sqlResult.getServer();

		try (PreparedStatement stmt = server.getConnection().prepareStatement(sql)) {
			ResultSet rs = stmt.executeQuery();
			ResultSetMetaData metaData = rs.getMetaData();
			int colCount = metaData.getColumnCount();
			if (rs.next()) {
				if (colCount == 1) {
					return rs.getString(1);
				} else {
					List<String> result = new ArrayList<>();
					for (int i = 1; i <= colCount; i++) {
						String colName = metaData.getColumnName(i);
						result.add(colName + "=" + rs.getString(colName));
					}
					return StringUtils.join(result, "&");
				}
			} else {
				return null;
			}
		}
	}

	/**
	 * update
	 *
	 * @param updateSql
	 * @param value
	 * @throws Exception
	 */
	public static void update(String updateSql, String dateValue) throws Exception {
		SqlResult sqlResult = sqlParse(updateSql);
		String sql = sqlResult.getSql();
		ServerInfo server = sqlResult.getServer();

		Pattern STATE_CONFIG_PATTERN = Pattern.compile("(.*?)\\=(.*?)\\&(.*?)\\=(.*?)");
		Matcher matcher = STATE_CONFIG_PATTERN.matcher(dateValue);

		if (matcher.find()) {
			String[] values = StringUtils.split(dateValue, "&");
			HashMap<String, String> sqlParam = new HashMap<>();
			for (String subValue : values ) {
				sqlParam.put(StringUtils.substringBefore(subValue, "="), StringUtils.substringAfter(subValue, "="));
			}
			update(server, sql, sqlParam);

		} else {
			try (PreparedStatement stmt = server.getConnection().prepareStatement(sql)) {
				logger.debug("DB update with:{} ", dateValue);
				stmt.setString(1, dateValue);
				stmt.executeUpdate();
			}
		}
	}

	/**
	 * insert data
	 *
	 * @param insertSql
	 * @param cmdParam
	 * @throws Exception
	 */
	public static void insert(String insertSql, String dateValue) throws Exception {
		SqlResult sqlResult = sqlParse(insertSql);
		String sql = sqlResult.getSql();
		ServerInfo server = sqlResult.getServer();

		Pattern STATE_CONFIG_PATTERN = Pattern.compile("(.*?)\\=(.*?)\\&(.*?)\\=(.*?)");
		Matcher matcher = STATE_CONFIG_PATTERN.matcher(dateValue);

		if (matcher.find()) {
			String[] values = StringUtils.split(dateValue, "&");
			HashMap<String, String> sqlParam = new HashMap<>();
			for (String subValue : values) {
				sqlParam.put(StringUtils.substringBefore(subValue, "="), StringUtils.substringAfter(subValue, "="));
			}
			insert(server, sql, sqlParam);

		} else {
			try (PreparedStatement stmt = server.getConnection().prepareStatement(sql)) {
				stmt.setString(1, dateValue);
				stmt.executeUpdate();
			}
		}
	}

	/**
	 * execute update
	 *
	 * @param updateSql
	 * @param sqlParam
	 * @throws Exception
	 */
	private static void update(ServerInfo server, String updateSql, HashMap<String, String> sqlParam) throws Exception {
		Pattern sqlPattern = Pattern.compile("(?:value=.)");
		//List<String> paramValues = new ArrayList<String>();

		// analyze SQL statement
		Matcher matcher = sqlPattern.matcher(updateSql);
		StringBuffer newSql = new StringBuffer();

		while (matcher.find()) {
			String columnName = matcher.group(0);
			if (StringUtils.isNotEmpty(columnName)) {
				matcher.appendReplacement(newSql, "value=" + sqlParam.get("value"));
		//		paramValues.add(sqlParam.get("value"));
			}
		}
		matcher.appendTail(newSql);
		logger.debug("### sql:{}", newSql);

		try (PreparedStatement stmt = server.getConnection().prepareStatement(newSql.toString())) {
			stmt.executeUpdate();
		}
	}

        /**
         * execute insert 
         *
         * @param insertSql
         * @param sqlParam
         * @throws Exception
         */
        private static void insert(ServerInfo server, String insertSql, HashMap<String, String> sqlParam) throws Exception {
		Pattern sqlPattern = Pattern.compile("(\\?)");
		Matcher matcher = sqlPattern.matcher(insertSql);
		StringBuffer newSql = new StringBuffer();

		if(matcher.find()) {
			matcher.appendReplacement(newSql, sqlParam.get("value"));
		}

		if(matcher.find()) {
			matcher.appendReplacement(newSql, "'" + sqlParam.get("time") + "'");
		}
		matcher.appendTail(newSql);
		logger.debug("### sql:{}", newSql);

                try (PreparedStatement stmt = server.getConnection().prepareStatement(newSql.toString())) {
                        stmt.executeUpdate();
		}
        }

	/**
	 * parse SQL
	 *
	 * @param allSql
	 * @return result SQL statemant and DB to connect
	 */
	private static SqlResult sqlParse(String allSql) {
		Pattern SQL_PATTERN = Pattern.compile("^([^\'\"]+?)(\\:)(.*)");
		Matcher matcher = SQL_PATTERN.matcher(allSql);
		ServerInfo server = serverCache.get("DefultServer");
		String sql = "";
		if (matcher.matches()) {
			server = serverCache.get(matcher.group(1));
			if (server == null) {
				logger.error("no SQL server found.[{}]", allSql );
			}
			sql = matcher.group(3);
		} else {
			sql = allSql;
		}
		SqlResult result = new SqlResult(sql, server);
		return result;
	}

	/**
	 * SQLの接続サーバと実行するSQLを分析結果用クラス
	 *
	 */
	static class SqlResult {
		// 実行するSQL
		private String sql;
		// 接続サーバ情報
		private ServerInfo server;

		public SqlResult(String sql, ServerInfo server) {
			this.sql = sql;
			this.server = server;
		}

		public String getSql() {
			return sql;
		}

		public void setSql(String sql) {
			this.sql = sql;
		}

		public ServerInfo getServer() {
			return server;
		}

		public void setServer(ServerInfo server) {
			this.server = server;
		}
	}
}
