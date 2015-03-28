package org.dcache.services.info.base;

/**
 * Indicates that a StatePath refers to something that simply doesn't exist.
 *
 * @author Paul Millar <paul.millar@desy.de>
 */
public class NoMetricStatePathException extends BadStatePathException {

    private static final long serialVersionUID = 1;
    static final String MESSAGE_PREFIX = "path does not exist: ";

    public NoMetricStatePathException(String path) {
        super(MESSAGE_PREFIX + path);
    }
}
