package org.dcache.ftp.data;

public class FTPException extends Exception
{
    private static final long serialVersionUID = -5833261869723000768L;

    public FTPException() {
        super();
    }

    public FTPException(String s) {
        super(s);
    }
}
