package org.dcache.services.infoCollector;

/**
* Schema class.<br>
* This abstract class is very poor but it's very useful to have a single
* superclass to handle in every class in this package. So if you want
* to change the <code>Schema</code> the impact is minimum.<br><br>
* A SchemaFiller contains the reference to the <code>InfoBase</code> object and 
* to a <code>Schema</code> specialized object. 
* A specialized type of this class is tightly coupled with the specialized
* type of the <code>Schema</code> with which it deals.
* In few words: "A SchemaFiller implementation have to know well its Schema
* implementation to modify it".  
**/
abstract public class SchemaFiller {
	
	InfoBase inf = null;
	
	Schema schema = null;
	
	/**
	 * This method reads the information from the <code>InfoBase</code> and
	 * put them in the Schema.<br>
	 * This method is called from <code>InfoExporter</code> without knowledge 
	 * of what type of Schema is adopted.
	 */
	abstract public void fillSchema();

}
