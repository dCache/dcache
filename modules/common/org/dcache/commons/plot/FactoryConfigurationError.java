
package org.dcache.commons.plot;

/**
 *
 * @author timur
 */
public class FactoryConfigurationError  extends Error{

    public FactoryConfigurationError(String msg) {
        super(msg);
    }

    public FactoryConfigurationError(String msg, Throwable cause) {
        super(msg, cause);
    }
}
