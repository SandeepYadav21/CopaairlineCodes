/************************************************************************
 * Copyright (c) 2017 Tibco Software.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 
 * Contributors:
 *     Tibco Software - initial API and implementation
 *     TIBCO Software Inc. 
 ************************************************************************/
package com.tibco.custom.jms.provider;

import com.tibco.bw.sr.jms.runtime.api.custom.AbstractJMSProvider;

public class CustomJMSProvider extends AbstractJMSProvider {

	/**
	 * This Method must provide an intial context factory class context to get corresponding JMS Provider object
	 * @return returns intial context factory class name 
	 */
	@Override
	public String getInitialContextFactory() {
		return "com.solacesystems.jndi.SolJNDIInitialContextFactory";
	}

}
