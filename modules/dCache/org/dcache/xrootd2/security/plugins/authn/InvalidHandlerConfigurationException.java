package org.dcache.xrootd2.security.plugins.authn;

/**
 * Thrown by authentication factory if instantiation of essential components
 * fails.
 * @author tzangerl
 *
 */
public class InvalidHandlerConfigurationException extends Exception {


    /**
     * generated serialVersionUID
     */
    private static final long serialVersionUID = -2820638430180259321L;

    public InvalidHandlerConfigurationException(Throwable t) {
        super(t);
    }

    public InvalidHandlerConfigurationException(String msg, Throwable t) {
        super(msg, t);
    }

    @Override
    public String toString() {
        String result = getMessage() + ": ";

        for (StackTraceElement element : getStackTrace()) {
            result += element.toString() + "\n";
        }

        return result;
    }
}
