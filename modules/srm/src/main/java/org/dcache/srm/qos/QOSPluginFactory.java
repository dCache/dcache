package org.dcache.srm.qos;

import org.dcache.srm.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QOSPluginFactory {
    private static final Logger logger =
            LoggerFactory.getLogger(QOSPluginFactory.class);

	static public QOSPlugin createInstance(Configuration configuration) {
		QOSPlugin qosPlugin = null;
		try {
			Class<? extends QOSPlugin> pluginClass =
                                Thread.currentThread().getContextClassLoader().loadClass(configuration.getQosPluginClass()).asSubclass(QOSPlugin.class);
			qosPlugin = pluginClass.newInstance();
			qosPlugin.setSrmConfiguration(configuration);
			logger.debug("Created new qos plugin of type "+configuration.getQosPluginClass());
		}
		catch(Exception e) {
			logger.error("Could not create class "+configuration.getQosPluginClass(),e);
		}
		
		return qosPlugin;
	}

}
