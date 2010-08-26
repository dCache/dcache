package org.dcache.xrootd.core.stream;

public class TooMuchLogicalStreamsException extends Exception {

    static final long serialVersionUID = 26758236163347571L;

    public TooMuchLogicalStreamsException(String msg) {
        super(msg);
    }

}
