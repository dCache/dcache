package diskCacheV111.util;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;


public class OsmLocationExtractor implements HsmLocation {


	private final URI _uri;

	/**
	 *
	 * @param location
	 * @throws IllegalArgumentException if location is not an OSM location
	 */
	public OsmLocationExtractor(URI location) throws IllegalArgumentException {
		_uri = location;
	}

	/**
	 * Extract location {@link URI} from level map.
	 * @param level map
	 * @throws IllegalArgumentException if location is not an OSM location
	 */
	public OsmLocationExtractor(Map<Integer, String> levels) throws IllegalArgumentException {

		String storageInfo = levels.get(1);
		if(storageInfo == null ) {
			throw new IllegalArgumentException("OSM uses level 1 only");
		}
                _uri = parseLevel(storageInfo);

	}

	@Override
        public URI location() {
		return _uri;
	}

    public static URI parseLevel(String storageInfo) throws IllegalArgumentException {

        boolean isLegacy = false;
        String[] st =  storageInfo.split("[ \t]");
        if (st.length < 3) {
            throw new IllegalArgumentException("Invalid content of Level 1 (3/4 fields expected):" + storageInfo);
        }
        if (st.length > 4) {
            //legacy staff in level-1
            isLegacy = true;
        }
        StringBuilder sb = new StringBuilder("osm://");
        String store = st[0];
        String group = st[1];
        String bfid = st[2];
        String instance = st.length > 3 && !isLegacy ? st[3] : "osm";
        sb.append(instance).append("/?");
        sb.append("store=").append(store).append("&");
        sb.append("group=").append(group).append("&");
        sb.append("bfid=").append(bfid);

        try {
            return new URI(sb.toString());
        } catch (URISyntaxException e) {
            //should never happen, but nevertheless
            throw new IllegalArgumentException("failed to generate URI from level: " + storageInfo);
        }
    }

	@Override
        public Map<Integer, String> toLevels() {

		Map<Integer, String> levelData = new HashMap<>(1);

		Map<String, String> parsed = parseURI(_uri);

		String asLevel = parsed.get("store") +
						 " " + parsed.get("group") +
						 " " + parsed.get("bfid") +
						 " " + _uri.getAuthority();

		levelData.put(1, asLevel);

		return levelData;
	}




	private static Map<String, String> parseURI(URI location) throws IllegalArgumentException {


		Map<String,String> values = new HashMap<>();



		String query = location.getQuery();
                if (query == null) {
                    throw new IllegalArgumentException("Invalid URI format: " + location);
                }

		String[] storageInfo = query.split("&");
		if(storageInfo.length != 3) {
			throw new IllegalArgumentException("Invalid URI format: " + location);
		}


		for( String s: storageInfo) {
			String[] ss = s.split("=");
			if(ss.length != 2 ) {
				throw new IllegalArgumentException("Invalid URI format: " + location);
			}
			values.put(ss[0], ss[1]);
		}

		return values;
	}

}
