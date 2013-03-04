package org.dcache.cells;

import com.sun.messaging.ConnectionFactory;

import javax.jms.JMSException;

import java.util.Properties;

public class OpenMqConnectionFactoryFactory
{
    public static ConnectionFactory
        createConnectionFactory(Properties properties)
    {
        ConnectionFactory cf = new ConnectionFactory();
        try {
            for (String key: properties.stringPropertyNames()) {
                cf.setProperty(key, (String) properties.get(key));
            }
        } catch (JMSException e) {
            throw new RuntimeException("Error configuring ConnectionFactory", e);
        }
        return cf;
    }
}
