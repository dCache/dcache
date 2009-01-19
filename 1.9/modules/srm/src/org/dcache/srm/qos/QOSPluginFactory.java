package org.dcache.srm.qos;

import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.util.Configuration;
import java.lang.Thread;

public class QOSPluginFactory {

	static public QOSPlugin createInstance(Configuration configuration) {
		QOSPlugin qosPlugin = null;
		try {
			Class pluginClass = Thread.currentThread().getContextClassLoader().loadClass(configuration.getQosPluginClass());
			qosPlugin = (QOSPlugin)pluginClass.newInstance();
			qosPlugin.setSrmConfiguration(configuration);
			configuration.getStorage().log("Created new qos plugin of type "+configuration.getQosPluginClass());
		}
		catch(Exception e) {
			configuration.getStorage().elog("Could not create class "+configuration.getQosPluginClass());
		}
		
		return qosPlugin;
	}

}
