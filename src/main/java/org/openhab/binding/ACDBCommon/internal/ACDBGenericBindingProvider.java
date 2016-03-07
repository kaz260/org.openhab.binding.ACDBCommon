/**
 * Copyright (c) 2010-2016, openHAB.org and others.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 */

package org.openhab.binding.ACDBCommon.internal;

import org.openhab.binding.ACDBCommon.ACDBBindingProvider;
import org.openhab.core.binding.BindingConfig;
import org.openhab.core.items.Item;
import org.openhab.model.item.binding.AbstractGenericBindingProvider;
import org.openhab.model.item.binding.BindingConfigParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * <p>
 * binding provider implementation
 * </p>
 *
 * @author Kazuhiro Matsuda
 * @since 1.8.0
 */
public abstract class ACDBGenericBindingProvider extends AbstractGenericBindingProvider
	implements ACDBBindingProvider {
	/**
	 * logger 
	 */
	private static final Logger logger = LoggerFactory.getLogger(ACDBBinding.class);
	private static final Pattern BASE_CONFIG_PATTERN = Pattern
		.compile("(<|>>|>)\\[(.*?)\\](\\s|,|$)");

	private static final Pattern BASE_CONFIG_PATTERN_W_COMMAND = Pattern
		.compile("(<|>>|>)\\[([a-zA-Z]+):(.*?)\\](\\s|,|$)");

		@Override
	public void validateItemType(Item item, String bindingConfig)
		throws BindingConfigParseException {}
	@Override
	public void processBindingConfiguration(String context, Item item, String bindingConfig)
		throws BindingConfigParseException {
		logger.debug("binding configuration : " + bindingConfig);
		super.processBindingConfiguration(context, item, bindingConfig);

		ACDBBindingConfig config = new ACDBBindingConfig();
		// analyze configuraton
		Matcher matcher = BASE_CONFIG_PATTERN_W_COMMAND.matcher(bindingConfig);
		if(matcher.matches()) {
			matcher.reset();
                	while (matcher.find()) {
                        	String direction = matcher.group(1);
				logger.debug("### G1:{}", direction);
                        	String command = matcher.group(2);
				logger.debug("### G2:{}", command);
				String sql = matcher.group(3);
				logger.debug("### G3:{}", sql);

                        	if (direction.equals("<")) {
                                	config.selectSql = sql;
                        	} else if (direction.equals(">")) {
                                	config.updateSql = sql;
                        	} else if (direction.equals(">>")) {
                                	config.insertSql = sql;
                        	} else {
                                	throw new BindingConfigParseException(
                                        	"Unknown command given! Configuration must start with '<' or '>' or '>>' ");
                        	}
                	}
		} else {
			matcher = BASE_CONFIG_PATTERN.matcher(bindingConfig);
			if (matcher.matches()) {
				matcher.reset();
                        	while (matcher.find()) {
                                	String direction = matcher.group(1);
                                	String sql = matcher.group(2);
					logger.debug("### G1:{}", direction);
					logger.debug("### G2:{}", sql);

                                	if (direction.equals("<")) {
                                        	config.selectSql = sql;
                                	} else if (direction.equals(">")) {
                                        	config.updateSql = sql;
                                	} else if (direction.equals(">>")) {
                                        	config.insertSql = sql;
                                	} else {
                                        	throw new BindingConfigParseException(
                                                	"Unknown command given! Configuration must start with '<' or '>' or '>>' ");
                                	}
                        	}
			} else {
				throw new BindingConfigParseException("bindingConfig '" + bindingConfig
					+ "' doesn't contain a valid binding configuration");
			}
		}
		addBindingConfig(item, config);
	}


	/**
	 * Binding configuration
	 */
	class ACDBBindingConfig implements BindingConfig {
		/**
		 * SQL for select
		 */
		private String selectSql;
		/**
		 * SQL for update
		 */
		private String updateSql;
		/**
		 * SQL for insert
		 */
		private String insertSql;
	}

	@Override
	public String getUpdateSql(String itemName) {
		ACDBBindingConfig config = (ACDBBindingConfig) bindingConfigs.get(itemName);
		return config != null ? config.updateSql : null;
	}

	@Override
	public String getSelectSql(String itemName) {
		ACDBBindingConfig config = (ACDBBindingConfig) bindingConfigs.get(itemName);
		return config != null ? config.selectSql : null;
	}
	
	@Override
	public String getInsertSql(String itemName) {
		ACDBBindingConfig config = (ACDBBindingConfig) bindingConfigs.get(itemName);
		return config != null ? config.insertSql : null;
	}
}
