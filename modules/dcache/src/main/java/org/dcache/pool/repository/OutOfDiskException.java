package org.dcache.pool.repository;

import java.io.IOException;

/**
 * IOException that indicates lack of available space.
 */
public class OutOfDiskException extends IOException {

    private static final long serialVersionUID = 94815748828719063L;

    public OutOfDiskException() {
        super();
    }

    public OutOfDiskException(String message) {
        super(message);
    }
}
