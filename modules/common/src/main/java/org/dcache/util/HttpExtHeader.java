package org.dcache.util;

public record HttpExtHeader() {
    public static final String DIGEST = "Digest";
    public static final String REPR_DIGEST = "Repr-Digest";
    public static final String WANT_DIGEST = "Want-Digest";
    public static final String WANT_REPR_DIGEST = "Want-Repr-Digest";
}