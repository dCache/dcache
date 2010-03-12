package diskCacheV111.util;

import java.net.URI;
import java.net.URISyntaxException;

public class DCapUrl {

    private final URI _uri;

    /**
     * construct a DCapUrl from given string ( dcap://host:port/path/file )
     *
     * @param dCapUrl
     * @throws IllegalArgumentException If the given string violates RFC 2396, as augmented by the above deviations
     */
    public DCapUrl(String dCapUrl) throws IllegalArgumentException {

        try {
            _uri = new URI(dCapUrl);
        } catch (URISyntaxException ue) {
            // be complaint with dCache API specification
            throw new IllegalArgumentException("Invalid dcap url : " + ue);
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
