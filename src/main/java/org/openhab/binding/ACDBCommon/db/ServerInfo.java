/**
 * Copyright (c) 2010-2016, openHAB.org and others.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 */

package org.openhab.binding.ACDBCommon.db;

import java.sql.Connection;
import java.sql.DriverManager;

/**
 * <p>
 * DBサーバ情報
 * </p>
 *
 * @author NTT
 */
public class ServerInfo {

	private String serverId = "";
	private String url = "";
	private String user = "";
	private String password = "";
	private String driverClassName = "";
	private Connection connection;

	public String getServerId() {
		return serverId;
	}

	public void setServerId(String serverId) {
		this.serverId = serverId;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getDriverClassName() {
		return driverClassName;
	}

	public void setDriverClassName(String driverClassName) {
		this.driverClassName = driverClassName;
	}

	public void setConnection(Connection connection) {
		this.connection = connection;
	}

	public ServerInfo(String serverId) {
		this.serverId = serverId;
	}

	@Override
	public String toString() {
		return "server [id=" + serverId + ", url=" + url + ", user=" + user + ", password=" + password + "]";
	}

	/**
	 * DB接続を取得する
	 * 
	 * @return Connection
	 * @throws Exception
	 */
	public Connection getConnection() throws Exception {
		if (connection == null || connection.isClosed()) {
			connection = this.openConnection();
		}
		return connection;
	}

	/**
	 * DB接続を行う
	 *
	 * @return Connection
	 * 
	 * @throws Exception
	 *             接続異常
	 */
	public Connection openConnection() throws Exception {
		// DBのドライバを指定
		Class.forName(driverClassName);
		// データベースに接続 (DB名,ID,パスワードを指定)
		Connection conn = DriverManager.getConnection(url, user, password);
		return conn;
	}
}
