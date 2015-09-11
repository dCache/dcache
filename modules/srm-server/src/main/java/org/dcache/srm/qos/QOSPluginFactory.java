package org.dcache.srm.qos;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dcache.srm.SRM;
import org.dcache.srm.util.Configuration;

public class QOSPluginFactory {
    private static final Logger logger =
            LoggerFactory.getLogger(QOSPluginFactory.class);

	public static QOSPlugin createInstance(SRM srm) {
        Configuration configuration = srm.getConfiguration();
		QOSPlugin qosPlugin = null;
		String qosPluginClass = configuration.getQosPluginClass();
		if (qosPluginClass != null) {
			try {
				Class<? extends QOSPlugin> pluginClass =
						Thread.currentThread().getContextClassLoader().loadClass(
								qosPluginClass).asSubclass(QOSPlugin.class);
				qosPlugin = pluginClass.newInstance();
				qosPlugin.setSrm(srm);
				logger.debug("Created new qos plugin of type " + qosPluginClass);
			} catch (Exception e) {
				logger.error("Could not create class " + qosPluginClass, e);
			}
		}
		return qosPlugin;
	}

}
