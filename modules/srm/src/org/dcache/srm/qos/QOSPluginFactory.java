package org.dcache.srm.qos;

import org.dcache.srm.util.Configuration;
import org.apache.log4j.Logger;

public class QOSPluginFactory {
    private static final Logger logger =
            Logger.getLogger(QOSPluginFactory.class);

	static public QOSPlugin createInstance(Configuration configuration) {
		QOSPlugin qosPlugin = null;
		try {
			Class pluginClass = Thread.currentThread().getContextClassLoader().loadClass(configuration.getQosPluginClass());
			qosPlugin = (QOSPlugin)pluginClass.newInstance();
			qosPlugin.setSrmConfiguration(configuration);
			logger.debug("Created new qos plugin of type "+configuration.getQosPluginClass());
		}
		catch(Exception e) {
			logger.error("Could not create class "+configuration.getQosPluginClass(),e);
		}
		
		return qosPlugin;
	}

}
