package org.dcache.services.infoCollector;

import java.io.*;

/**
 * Schema class.<br>
 * This abstract class is empty but it's very useful to have a single
 * superclass to handle in every class in this package. So if you want
 * to change the <code>Schema</code> the impact is minimum.<br><br>
 * A Schema that extends this class organizes the same information in a
 * particular way, following the requirements of the external system on
 * which you want export them.<br><br>
 * A Schema is ever created inside a SchemaFiller object.    
 **/
abstract public class Schema implements Serializable{

	private static final long serialVersionUID = -8035876156296337291L;
	
}
