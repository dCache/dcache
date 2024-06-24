package diskCacheV111.util;

import java.net.URI;
import java.util.Map;


public class CtaLocationExtractor implements HsmLocation {

    private final URI _uri;

    public CtaLocationExtractor(URI location) {
        _uri = location;
    }


    @Override
    public URI location() {
        return _uri;
    }


    @Override
    public Map<Integer, String> toLevels() {
        return null;
    }

}
