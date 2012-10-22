package org.dcache.gplazma.loader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Vector;

/**
 * The Utf8DataClassLoader is a {@link ClassLoader} that provides access to
 * zero or more resources at some fixed location.
 * <p>
 * The class initially has zero resources at that location and additional
 * resources may be added by calling the {@link #addResource} method. The
 * resources are added as strings and made available as a UTF-8 encoded
 * byte-stream of the supplied String's data.
 */
public class Utf8DataClassLoader extends ClassLoader {
    private static final String URL_SCHEMA = "test";
    private static final String URL_HOSTNAME = "ignored-host.invalid";
    private static final int URL_DEFAULT_PORT = -1;

    private final URLStreamHandler _handler = new TestURLStreamHandler();
    private final List<String> _resourceData = new ArrayList<>();
    private final String _path;

    public Utf8DataClassLoader( String path) {
        this(path, getSystemClassLoader());
    }

    public Utf8DataClassLoader( String path, ClassLoader parent) {
        super(parent);
        _path = path;
    }

    public void addResource( PluginXmlGenerator xmlData) {
        _resourceData.add( xmlData.toString());
    }

    public void addResource( String xmlData) {
        _resourceData.add( xmlData);
    }

    @Override
    protected URL findResource( String name) {
        if( name.equals( _path) && !_resourceData.isEmpty()) {
            return newUrlForIndex( 0);
        } else {
            return null;
        }
    }

    private URL newUrlForIndex( int index) {
        URL result;

        String filename = _path + "?" + index;

        try {
            result =
                    new URL( URL_SCHEMA, URL_HOSTNAME, URL_DEFAULT_PORT,
                             filename, _handler);
        } catch (MalformedURLException e) {
            throw new RuntimeException( "Failed to create local URL", e);
        }
        return result;
    }

    @Override
    protected Enumeration<URL> findResources( String name) throws IOException {
        Vector<URL> results = new Vector<>();

        if( name.equals( _path)) {
            for( int i = 0; i < _resourceData.size(); i++) {
                URL resourceUrl = newUrlForIndex( i);
                results.add( resourceUrl);
            }
        }

        return results.elements();
    }

    /**
     * A custom handler to allow reading of a "test" URL
     */
    public class TestURLStreamHandler extends URLStreamHandler {
        @Override
        protected URLConnection openConnection( URL url) throws IOException {
            return new TestURLConnection( url);
        }
    }

    /**
     * A custom URLConnection that allows reading of String data as a UTF-8
     * encoded sequence of bytes.
     */
    public class TestURLConnection extends URLConnection {
        private static final String CHARSET_NAME_UTF_8 = "UTF-8";
        private byte[] _rawData;

        public TestURLConnection( URL url) {
            super( url);
            String queryPart = url.getQuery();
            int index = Integer.valueOf( queryPart);
            String data = _resourceData.get( index);
            _rawData = getByteContent( data);
        }

        @Override
        public void connect() throws IOException {
            // nothing needs to be done as data is local
        }

        @Override
        public InputStream getInputStream() throws IOException {
            InputStream is = new ByteArrayInputStream( _rawData);
            return is;
        }

        private byte[] getByteContent( String content) {
            Charset utf8 = Charset.forName( CHARSET_NAME_UTF_8);
            return content.getBytes( utf8);
        }
    }
}
