package org.dcache.services.infoCollector;

import java.io.Serializable;

/**
 * Implementation of the <code>Schema</code> to exoport
 * the information using the <em>Glue Schema v1.2</em> organization.
 */
public class GlueSchemaV1_2 extends Schema{
	
	private static final long serialVersionUID = -8035876156296337291L;
	
	
	public static class StorageElement implements Serializable{
		private static final long serialVersionUID = -8035876156296337291L;
		
		public StorageArea[] storage_area = null;
		public ControlProtocol[] control_protocol = null;
		public AccessProtocol[] access_protocol = null;
		
		public String uniqueID = null;
		public String informationServiceURL = null;
		public String sizeTotal = null;
		public String sizeFree = null;
		public String architecture = null;

	}
	
	
	public static class StorageArea implements Serializable{
		private static final long serialVersionUID = -8035876156296337291L;
		
		public String localID = null;
		public String path = null;
		public String type = null;
		
		public String state_usedSpace = null;
		public String state_availableSpace = null;
		
		public String policy_quota = null;
		public String policy_minFileSize  = null;
		public String policy_maxFileSize = null;
		public String policy_maxData = null;
		public String policy_maxNumFiles = null;
		public String policy_maxPinDuration = null;
		
	}
	
	
	public static class ControlProtocol implements Serializable{
		private static final long serialVersionUID = -8035876156296337291L;
		
		public String localID = null;
		public String endPoint = null;
		public String type = null;
		public String version = null;
		public String capability = null;
	}

	
	public static class AccessProtocol implements Serializable{
		private static final long serialVersionUID = -8035876156296337291L;
		
		public String localID = null;
		public String endPoint = null;
		public String type = null;
		public String version = null;
	        public int port = 0;
		public String capability = null;
	}
	
	/**
	 * Access point to get all information
	 */
	public StorageElement se = new StorageElement();
}
