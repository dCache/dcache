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

        HsmLocation extractor = null;
        String hsmType = location.getScheme();

        if ("osm".equals(hsmType)) {
            extractor = new OsmLocationExtractor(location);
        } else if ("enstore".equals(hsmType)) {
            extractor = new EnstoreLocationExtractor(location);
        } else if ("hpss".equals(hsmType)) {
            extractor = new HpssLocationExtractor(location);
        } else {
            throw new IllegalArgumentException("hsmType " + hsmType
                    + " not supported. FIXME: make it dynamic");
        }
        return extractor;
    }
}
