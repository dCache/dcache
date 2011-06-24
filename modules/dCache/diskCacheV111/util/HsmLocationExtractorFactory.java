package diskCacheV111.util;

import java.net.URI;


/**
 *
 * factory to parce storage location URI and return corresponding extractor
 *
 *  <strong>[scheme:][//authority][path][?query][#fragment]</strong>
 *  where:
 * 	scheme    : hsm type
 * 	authority : instance id
 * 	path+query: opaque to dCache HSM specific data
 *
 *  example:
 * 	osm://desy-main/?store=h1&bfid=1234
 *	osm://desy-copy/?store=h1_d&bfid=5678
 */
public class HsmLocationExtractorFactory {

    private HsmLocationExtractorFactory() {
    }

    public static HsmLocation extractorOf(URI location) throws IllegalArgumentException {

        URI validatedUri = validate(location);
        HsmLocation extractor = null;
        String hsmType = validatedUri.getScheme();

        if ("osm".equals(hsmType)) {
            extractor = new OsmLocationExtractor(validatedUri);
        } else if ("enstore".equals(hsmType)) {
            extractor = new EnstoreLocationExtractor(validatedUri);
        } else if ("hpss".equals(hsmType)) {
            extractor = new HpssLocationExtractor(validatedUri);
        } else {
            throw new IllegalArgumentException("hsmType " + hsmType
                    + " not supported. FIXME: make it dynamic");
        }
        return extractor;
    }

    /**
     * Validate gived URI with hsm location rules:
     * <pre>
     *  <strong>[scheme:][//authority][path][?query][#fragment]</strong>
     *  where:
     * 	scheme    : hsm type
     * 	authority : instance id
     * 	path+query: opaque to dCache HSM specific data
     * </pre>
     * @param location
     * @return location is it's valid
     * @throws IllegalArgumentException if location violates hsm rules.
     */
    public static URI validate(URI location) throws IllegalArgumentException {

        if(location.getScheme() == null) {
            throw new IllegalArgumentException("hsm type not defined");
        }

        if(location.getAuthority() == null) {
            throw new IllegalArgumentException("hsm instance id not defined");
        }

        if (location.getPath() == null || location.getPath().isEmpty() ) {
            throw new IllegalArgumentException("hsm-specific opaque data not defined");
        }

        return location;
    }
}
