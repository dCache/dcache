package org.dcache.gplazma.loader;

import java.io.IOException;
import java.net.URL;
import java.util.Enumeration;
import java.util.Vector;

/**
 * When Java code searches for a single match at the predefined location, the
 * first added resource is used. When searching for all resources at that
 * location then all resources will be listed in the order they were added.
 * <p>
 * This ClassLoader allows code to break that hierarchy: by making a
 * ClassLoader's parent an instance of ResourceBlockingClassLoader and
 * setting isBlocking to true will prevent calls to getResource() from
 * accessing parent resources.
 *
 * @author Paul Millar <paul.millar@desy.de>
 */
public class ResourceBlockingClassLoader extends ClassLoader {

    private static final Vector<URL> NO_URLS = new Vector<>();
    private boolean _isBlocking;

    public ResourceBlockingClassLoader() {
        this(getSystemClassLoader());
    }

    public ResourceBlockingClassLoader( ClassLoader parent) {
        super(parent);
    }

    public synchronized boolean getIsBlocking() {
        return _isBlocking;
    }

    public synchronized void setIsBlocking( boolean isBlocking) {
        _isBlocking = isBlocking;
    }

    @Override
    public synchronized URL getResource( String name) {
        if( _isBlocking) {
            return null;
        }

        return super.getResource( name);
    }

    @Override
    public synchronized Enumeration<URL> getResources( String name)
            throws IOException {
        if( _isBlocking) {
            return NO_URLS.elements();
        }

        return super.getResources( name);
    }
}
