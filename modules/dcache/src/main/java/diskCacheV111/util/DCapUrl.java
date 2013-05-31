package diskCacheV111.util;

import java.net.URI;
import java.net.URISyntaxException;

public class DCapUrl {

    private static final String REQUIRED_URI_SCHEME_SUFFIX = "dcap";

    private final URI _uri;

    /**
     * Construct a DCapUrl from given string (for example "dcap://host:port/dir1/dir2/file")
     *
     * @throws IllegalArgumentException If the given string is not a valid dCap URI
     */
    public DCapUrl(String dCapUrl) throws IllegalArgumentException {

        try {
            _uri = new URI(dCapUrl);
        } catch (URISyntaxException ue) {
            // be complaint with dCache API specification
            throw new IllegalArgumentException("Invalid dCap URI: " + ue);
        }

        if( !_uri.isAbsolute()) {
            throw new IllegalArgumentException("Missing schema in dCap URI: " + _uri);
        }

        String scheme = _uri.getScheme();

        if( !scheme.toLowerCase().endsWith( REQUIRED_URI_SCHEME_SUFFIX)) {
            throw new IllegalArgumentException("Invalid URI scheme '+ scheme+': " + _uri);
        }

        if( _uri.isOpaque()) {
            throw new IllegalArgumentException("dCap URIs are not opaque: " + _uri);
        }

        if( _uri.getAuthority() == null) {
            throw new IllegalArgumentException("Authority not present in dCap URI: " + _uri);
        }

        String path = _uri.getPath();

        if( path == null) {
            throw new IllegalArgumentException("Missing path in dCap url: " + _uri);
        }

        if( !path.startsWith( "/")) {
            throw new IllegalArgumentException("Non-absolute path in dCap url: " + _uri);
        }
    }

    /**
     *
     * @return The decoded path component of this DCapUrl, or null if the path is undefined
     */
    public String getFilePart() {
        return _uri.getPath();
    }

    /**
     *
     * @return The protocol component of this DCapUrl, or null if the protocol is undefined
     */
    public String getProtocol() {
        return _uri.getScheme();
    }

}
