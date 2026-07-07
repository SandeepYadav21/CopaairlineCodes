/************************************************************************
 * Copyright (c) 2017 Tibco Software.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 
 * Contributors:
 *     Tibco Software - initial API and implementation
 *     TIBCO Software Inc. 
 ************************************************************************/
package com.tibco.custom.jms.provider;

import com.tibco.bw.sr.jms.runtime.api.custom.AbstractActivator;
import com.tibco.bw.sr.jms.runtime.api.custom.AbstractJMSProvider;

public class Activator extends AbstractActivator {

	/**
	 * This Method must have the custom JMS Provider object instantiated
	 * @return returns a JMS Provider Object
	 */
	@Override
	protected AbstractJMSProvider getProvider() {
		return new CustomJMSProvider();
	}

	/**
	 * This Method will allow to add multiple providers in the same bundle 
	 * @return returns array of JMS Providers
	 */
	/*@Override
	protected AbstractJMSProvider[] getProviders() {
		return new AbstractJMSProvider[]{} ;
	}*/
	
}
