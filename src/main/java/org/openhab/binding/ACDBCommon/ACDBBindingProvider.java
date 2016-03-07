/**
 * Copyright (c) 2010-2016, openHAB.org and others.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 */

package org.openhab.binding.ACDBCommon;

import org.openhab.core.binding.BindingProvider;

/**
 * <p>
 * Binding Provider Interface
 * </p>
 *
 * @author Kazuhiro Matsuda
 * @since 1.8.0
 */
public interface ACDBBindingProvider extends BindingProvider {
	/**
	 * get sql for update
	 *
	 * @param itemName
	 * @return SQL
	 */
	String getUpdateSql(String itemName);

	/**
	 * get sql for select
	 *
	 * @param itemName
	 * @return SQL
	 */
	String getSelectSql(String itemName);
	
	/**
	 * get SQL for insert
	 *
	 * @param itemName
	 * @return SQL
	 */
	String getInsertSql(String itemName);
}
