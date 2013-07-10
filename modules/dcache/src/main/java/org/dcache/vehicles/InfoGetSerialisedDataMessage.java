package org.dcache.vehicles;

import java.util.List;

import diskCacheV111.vehicles.Message;

/**
 * Query the info provider for a serialised representation of dCache's internal state.
 *
 * @author Paul Millar <paul.millar@desy.de>
 */
public class InfoGetSerialisedDataMessage  extends Message {

	private final List<String> _pathElements;

	/**
	 * Auto-generated number.
	 */
	private static final long serialVersionUID = -2650923676987449094L;


	/** Our serialised representation of dCache's state */
	private String _data;

	/**
	 * Request a complete dump of dCache's state in the default format,
	 * which is XML.
	 */
	public InfoGetSerialisedDataMessage() {
            this(null);
	}

	/**
	 * Request an XML dump of dCache's state, expanding only from
	 * @param pathElements a list of elements, excluding the initial "dCache".
	 */
	public InfoGetSerialisedDataMessage( List <String> pathElements) {
		super( true);
		_pathElements = pathElements;
	}

	/**
	 * Provide the serialised representation of dCache's state.
	 * @return
	 */
	public String getSerialisedData() {
		return _data;
	}

	/**
	 * Set the serialised Data.
	 * @param serialisedData  The XML data.
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
