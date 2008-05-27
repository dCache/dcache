package org.dcache.vehicles;

import diskCacheV111.vehicles.Message;

/**
 * Query the info provider for a serialised representation of dCache's internal state.
 * 
 * @author Paul Millar <paul.millar@desy.de>
 */
public class InfoGetSerialisedDataMessage  extends Message {

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
		super( true);
	}
	
	/**
	 * Provide the serialised representation of dCache's state.
	 * @return
	 */
	public String getSerialisedData() {
		return _data;
	}
	
	public void setData( String serialisedData) {
		_data = serialisedData;
	}
}
