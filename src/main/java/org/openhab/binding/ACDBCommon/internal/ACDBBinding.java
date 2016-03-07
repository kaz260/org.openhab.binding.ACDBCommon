/**
 * Copyright (c) 2010-2016, openHAB.org and others.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 */

package org.openhab.binding.ACDBCommon.internal;

import java.util.Dictionary;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.openhab.binding.ACDBCommon.ACDBBindingProvider;
import org.openhab.binding.ACDBCommon.db.DBManager;
import org.openhab.binding.ACDBCommon.db.ServerInfo;
import org.openhab.core.binding.AbstractActiveBinding;
import org.openhab.core.events.EventPublisher;
import org.openhab.core.items.Item;
import org.openhab.core.items.ItemRegistry;
import org.openhab.core.types.Command;
import org.openhab.core.types.State;
import org.openhab.core.types.TypeParser;
import org.osgi.service.cm.ConfigurationException;
import org.osgi.service.cm.ManagedService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * Binding
 * </p>
 *
 * @author Kazuhiro Matsuda
 * @since 1.8.0
 */
public abstract class ACDBBinding extends AbstractActiveBinding<ACDBBindingProvider>
	implements ManagedService {

	/**
	 * logger 
	 */
	private static final Logger logger = LoggerFactory.getLogger(ACDBBinding.class);
	/**
	 * option format to connect multiple connections
	 */
	private static final Pattern DEVICES_PATTERN = Pattern
		.compile("^(.*?)\\.(url|user|password)$");
	/**
	 * item registry
	 */
	private static ItemRegistry itemRegistry;

	/**
	 * event publisher
	 */
	private static EventPublisher eventPublisher;

	/**
	 * refresh interval
	 * optional, defaults to 60s
	 * [bindingName]:refreshInterval
	 */
	private long refreshInterval = TimeUnit.SECONDS.toMillis(60);

	/**
	 * Item value map
	 */
	private Map<String, String> itemValueMap = new HashMap<>();

	/**
	 * get binding name
	 *
	 * @return binding name
	 */
	protected abstract String getBindingName();

	/**
	 * get driver class
	 *
	 * @return driverClassName
	 */
	protected abstract String getDriverClassName();

	/**
	 * deactivate
	 */
	public void deactivate() {
		try {
			DBManager.closeConnection();
		} catch (Exception e) {
			logger.error(getBindingName() + ":failed to close DB connecton.", e);
		}
		logger.debug(getBindingName() + " binding deactivated");
	}

	@Override
	protected long getRefreshInterval() {
		return refreshInterval;
	}

	@Override
	protected String getName() {
		return getBindingName() + " Refresh Service";
	}

	@Override
	protected void execute() {
		for (ACDBBindingProvider provider : this.providers) {
			for (String itemName : provider.getItemNames()) {
				String selectSql = provider.getSelectSql(itemName);
				if (StringUtils.isBlank(selectSql)) {
					continue;
				}

				try {
					String value = DBManager.select(selectSql);
					String oldValue = itemValueMap.get(itemName);
					if (!itemValueMap.containsKey(itemName) || !Objects.equals(value, oldValue)) {
						Item item = itemRegistry.getItem(itemName);
						State state = TypeParser.parseState(item.getAcceptedDataTypes(), value);
						itemValueMap.put(itemName, value);
						eventPublisher.postUpdate(itemName, state);
						logger.debug("execute   " + itemName + ":" + value + ":" + oldValue);
					}
					logger.debug("old item value: " + itemName + "=" + oldValue);
					logger.debug(getBindingName() + "selected value: " + itemName + "=" + value);
				} catch (Exception e) {
					logger.error(getBindingName() + ":failed to select value.", e);
				}
			}
		}
		logger.debug(getBindingName() + ": execute() method is called!");
	}

	@Override
	protected void internalReceiveUpdate(String itemName, State newState) {
		logger.debug("internalReceiveUpdate({},{})", itemName, newState);
		for (ACDBBindingProvider provider : this.providers) {
			selectDB(itemName, provider);
		}
	}

	protected void internalReceiveCommand(String itemName, Command command) {
		logger.debug("internalReceiveCommand({},{})", itemName, command);

		for (ACDBBindingProvider provider : this.providers) {
			String commandValue = command.toString();
			if (commandValue.indexOf("&") == -1) {
				commandValue = "time=" + commandValue + "&" + "value=" + commandValue;
			}
			logger.debug("### commandValue:{}", commandValue);
			logger.debug("### itemName:{}", itemName);
			logger.debug("### insertSql:{}", provider.getInsertSql(itemName));
			logger.debug("### updateSql:{}", provider.getUpdateSql(itemName));

			if(provider.getInsertSql(itemName) != null) {
				insertDB(itemName, provider, commandValue);
				continue;
			}
			if(provider.getUpdateSql(itemName) != null) {
				updateDB(itemName, provider, commandValue);
			}			
		}
	}

	@Override
	public void updated(Dictionary<String, ?> config) throws ConfigurationException {
		logger.debug(getBindingName() + ":calling updated(config)!");
		String lowerBindingName = StringUtils.lowerCase(getBindingName());
		if (config == null) {
			throw new ConfigurationException(lowerBindingName + ":url",
				"The SQL database URL is missing - please configure the " + lowerBindingName
					+ ":url parameter in openhab.cfg");
		}

		// read DB Server connection Information
		DBManager.serverCache = new HashMap<String, ServerInfo>();
		Enumeration<String> keys = config.keys();
		while (keys.hasMoreElements()) {
			String key = (String) keys.nextElement();

			Matcher matcher = DEVICES_PATTERN.matcher(key);

			if (!matcher.matches()) {
				continue;
			}
			matcher.reset();
			matcher.find();

			String serverId = matcher.group(1);
			ServerInfo server = DBManager.serverCache.get(serverId);
			if (server == null) {
				server = new ServerInfo(serverId);
				DBManager.serverCache.put(serverId, server);
				logger.debug("Created new DBserver Info " + serverId);
			}

			String configKey = matcher.group(2);
			String value = (String) config.get(key);

			if ("url".equals(configKey)) {
				server.setUrl(value);
			} else if ("user".equals(configKey)) {
				server.setUser(value);
			} else if ("password".equals(configKey)) {
				server.setPassword(value);
			} else {
				throw new ConfigurationException(configKey, "the given configKey '" + configKey
					+ "' is unknown");
			}
		}
		// read defult DBServer connection information
		String serverId = "DefultServer";
		ServerInfo server = new ServerInfo(serverId);
		server.setUrl((String) config.get("url"));
		server.setUser((String) config.get("user"));
		server.setPassword((String) config.get("password"));

		DBManager.serverCache.put("DefultServer", server);

		String refreshIntervalString = (String) config.get("refresh");
		if (StringUtils.isNotBlank(refreshIntervalString)) {
			refreshInterval = Long.parseLong(refreshIntervalString);
		}

		try {
			for (Map.Entry<String, ServerInfo> mapI : DBManager.serverCache.entrySet()) {
				ServerInfo serverI = mapI.getValue();
				serverI.setDriverClassName(getDriverClassName());
				if (StringUtils.isBlank(serverI.getUrl())
					|| StringUtils.isBlank(serverI.getUser())
					|| StringUtils.isBlank(serverI.getPassword())) {
					logger.warn("more information needed:" + serverI.toString());
					continue;
				}
				serverI.openConnection();
			}
		} catch (Exception e) {
			logger.error(getBindingName() + ":failed to connect DB.", e);
		}

		setProperlyConfigured(true);
		logger.debug(getBindingName() + ":updated(config) is called!");
	}

	/**
	 * set item registry
	 *
	 * @param itemRegistry
	 */
	public void setItemRegistry(ItemRegistry itemRegistry) {
		ACDBBinding.itemRegistry = itemRegistry;
	}

	/**
	 * unset item registry
	 *
	 * @param itemRegistry
	 */
	public void unsetItemRegistry(ItemRegistry itemRegistry) {
		ACDBBinding.itemRegistry = null;
	}

	/**
	 * set event publisher 
	 *
	 * @param itemRegistry
	 */
	public void setEventPublisher(EventPublisher eventPublisher) {
		ACDBBinding.eventPublisher = eventPublisher;
	}

	/**
	 * unset event publisher
	 *
	 * @param itemRegistry
	 */
	public void unsetEventPublisher(EventPublisher eventPublisher) {
		ACDBBinding.eventPublisher = null;
	}

	private void updateDB(String itemName, ACDBBindingProvider provider, String commandValue) {
		String updateSql = provider.getUpdateSql(itemName);

		String oldValue = itemValueMap.get(itemName);
		itemValueMap.put(itemName, commandValue);

		if (StringUtils.isBlank(updateSql)) {
			return;
		}
		if (!StringUtils.equals(commandValue, oldValue)) {
			try {
				DBManager.update(updateSql, commandValue);
			} catch (Exception e) {
				logger.error(getBindingName() + ":fail to update", e);
			}
		}
	}

	private void insertDB(String itemName, ACDBBindingProvider provider, String commandValue) {
		String insertSql = provider.getInsertSql(itemName);

		if (StringUtils.isBlank(insertSql)) {
			return;
		}

		try {
			DBManager.insert(insertSql, commandValue);
		} catch (Exception e) {
			logger.error(getBindingName() + ":failed to update", e);
		}
	}

	private void selectDB(String itemName, ACDBBindingProvider provider) {
		String selectSql = provider.getSelectSql(itemName);

		if (StringUtils.isBlank(selectSql)) {
			return;
		}

		try {
			String value = DBManager.select(selectSql);
			String oldValue = itemValueMap.get(itemName);
			if (!itemValueMap.containsKey(itemName) || !Objects.equals(value, oldValue)) {
				Item item = itemRegistry.getItem(itemName);
				State state = TypeParser.parseState(item.getAcceptedDataTypes(), value);
				itemValueMap.put(itemName, value.toString());
				eventPublisher.postUpdate(itemName, state);
			}
		} catch (Exception e) {
			logger.error(getBindingName() + ":failed to select data", e);
		}
	}
}
