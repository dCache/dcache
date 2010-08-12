package org.dcache.commons.plot;

/**
 * This class represent different Exception that might be thrown during plotting
 * process, including File IO, JDBC, XML processing and etc
 *
 * @author timur and tao
 */
public class PlotException extends Exception {

    private static final long serialVersionUID = -2867063841784498461L;

    public PlotException(String message) {
        super(message);
    }

    public PlotException(String message, Throwable source) {
        super(message, source);
    }
}
