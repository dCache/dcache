package org.dcache.vehicles;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;

import diskCacheV111.vehicles.Message;

/**
 * Query the info provider for a serialised representation of dCache's internal state.
 *
 * @author Paul Millar <paul.millar@desy.de>
 */
public class InfoGetSerialisedDataMessage  extends Message {

	private final List<String> _pathElements;
        private final String _serialiser;

	/**
	 * Auto-generated number.
	 */
	private static final long serialVersionUID = -2650923676987449094L;


	/** Our serialised representation of dCache's state */
	private String _data;

	/**
	 * Request a complete dump of dCache's state in the specified
         * serialisation.
	 */
	public InfoGetSerialisedDataMessage(String serialiser) {
            this(null, serialiser);
	}

	/**
	 * Request serialisation of dCache's state.  Only the specified subtree
         * is returned.
	 */
	public InfoGetSerialisedDataMessage(List <String> pathElements, String serialiser) {
		super( true);
                _pathElements = pathElements == null ? null : ImmutableList.copyOf(pathElements);
                _serialiser = serialiser;
	}

	/**
	 * Provide the serialised representation of dCache's state.
	 * @return
	 */
	public String getSerialisedData() {
		return _data;
	}

        public String getSerialiser()
        {
            return _serialiser;
        }

	/**
	 * Set the serialised Data.
	 */
	public void setData( String serialisedData) {
		_data = serialisedData;
	}

	/**
	 * Obtain the a List of path elements.
	 * @return
	 */
	public List<String> getPathElements() {
		return _pathElements;
	}

	/**
	 * Is true when we require a complete dump, false otherwise.
	 * @return
	 */
	public boolean isCompleteDump() {
		return _pathElements==null;
	}
}
