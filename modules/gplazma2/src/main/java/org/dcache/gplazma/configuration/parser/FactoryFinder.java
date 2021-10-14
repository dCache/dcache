package org.dcache.gplazma.configuration.parser;

/**
 * Package private and not exposed through API Subject to change without notice
 *
 * @author timur
 */
class FactoryFinder {

    /**
     * a simpleminded implementation of the factory finder a more sofisticated and secure one can be
     * used if needed
     */
    static Object find(String factoryId, String fallbackClassName) throws
          ClassNotFoundException,
          InstantiationException,
          IllegalAccessException {
        String className = System.getProperties().getProperty(factoryId, fallbackClassName);
        return newInstance(className);
    }

    static Object newInstance(String className) throws
          ClassNotFoundException,
          InstantiationException,
          IllegalAccessException {
        return Class.forName(className).newInstance();
    }
}
