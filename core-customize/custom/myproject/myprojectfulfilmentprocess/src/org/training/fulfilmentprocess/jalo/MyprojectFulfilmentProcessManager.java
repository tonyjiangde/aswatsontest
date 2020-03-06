/*
 * Copyright (c) 2019 SAP SE or an SAP affiliate company. All rights reserved.
 */
package org.training.fulfilmentprocess.jalo;

import de.hybris.platform.jalo.JaloSession;
import de.hybris.platform.jalo.extension.ExtensionManager;
import org.training.fulfilmentprocess.constants.MyprojectFulfilmentProcessConstants;

public class MyprojectFulfilmentProcessManager extends GeneratedMyprojectFulfilmentProcessManager
{
	public static final MyprojectFulfilmentProcessManager getInstance()
	{
		ExtensionManager em = JaloSession.getCurrentSession().getExtensionManager();
		return (MyprojectFulfilmentProcessManager) em.getExtension(MyprojectFulfilmentProcessConstants.EXTENSIONNAME);
	}
	
}
