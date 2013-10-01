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

        validate(location);
        HsmLocation extractor;
        String hsmType = location.getScheme();

        switch (hsmType) {
        case "osm":
            extractor = new OsmLocationExtractor(location);
            break;
        case "enstore":
            extractor = new EnstoreLocationExtractor(location);
            break;
        case "hpss":
            extractor = new HpssLocationExtractor(location);
            break;
        default:
            throw new IllegalArgumentException("hsmType " + hsmType
                    + " not supported. FIXME: make it dynamic");
        }
        return extractor;
    }

    /**
     * Validate given URI with hsm location rules:
     * <pre>
     *  <strong>[scheme:][//authority][path][?query][#fragment]</strong>
     *  where:
     * 	scheme    : hsm type
     * 	authority : instance id
     * 	path+query: opaque to dCache HSM specific data
     * </pre>
     * @param location
     * @throws IllegalArgumentException if location violates hsm rules.
     */
    public static void validate(URI location) throws IllegalArgumentException {

        if(location.getScheme() == null) {
            throw new IllegalArgumentException("hsm type not defined in " + location);
        }

        if(location.getAuthority() == null) {
            throw new IllegalArgumentException("hsm instance id not defined in " + location);
        }
    }
}
