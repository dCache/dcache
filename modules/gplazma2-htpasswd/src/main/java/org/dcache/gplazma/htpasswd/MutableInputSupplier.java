package org.dcache.gplazma.htpasswd;

import com.google.common.io.InputSupplier;

import java.io.Reader;

interface MutableInputSupplier<R extends Reader> extends InputSupplier<R>
{
    long lastModified();
}
