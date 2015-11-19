package diskCacheV111.util;

import java.net.URI;
import java.util.Map;

public interface HsmLocation {

	/**
	 *
	 * @return URI representation of location information
	 */
	URI location();

	/**
	 *
	 * @return pnfs level based representation of location information
	 */
	Map<Integer, String> toLevels();
}
