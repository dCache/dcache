package infoDynamicSE;

/**
 * InfoImporter abstract class.<br>
 * An InfoImporter object has to import the dynamic information from an SE
 * server and have to provide the <code>getLDIF()</code> method to return
 * that dynamic information in the LDIF format in a String.<br> 
 * This application complies LCG GIP 1.0.22, so it distinguish the <em>plugin</em> 
 * from the <em>provider</em>.<br>
 * A <b>plugin</b> provide dynamic values to the attributes already present in
 * the static ldif file. It can not add a new <code>dn:</code> node or remove an
 * existent one.<br>
 * A <b>provider</b> provides completely new <code>dn:</code> nodes, that can appeare
 * and disappeare dynamically. In this case, static ldif file is red only to know 
 * the chunk key.<br>
 * This class provide the <code>createLDIF()</code> method that launches the
 * <code>executePlugin()</code> or the <code>executeProvider()</code> method, 
 * according with mode selected, and they build the ldif to publish.<br>
 * <code>executePlugin()</code> and <code>executeProvider()</code> are abtract
 * methods. They have been implemented in the <code>InfoImporter</code> 
 * SE-specific extension class. There they know how to retrive the dynamic 
 * information from the SE, so in the specific way provided from that SE type.<br><br>
 * To support a new SE type with the <em>InfoImporter</em> for LCG-GIP environment, 
 * it's enought extends this class and implements executePlugin() and executeProvider() 
 * method to retrive the dynamic information.<br>
 * The implementation of these methods has also to know what specific kind of schema is 
 * used to organize the information (the required classes have to be in the classpath).<br>
 * <br>So an implementation of these methods takes the information from the specific SE, in the 
 * way that it (and only it) knows, and it translates the information in a format the complies the 
 * specific Schema adopted that it (and only it) knows.
 * <br>Finally these methods write the information in the <code>_ldif</code> String. 
 * @see InfoProvider
 **/
public abstract class InfoImporter {
	
	
	/** This is the String where the dynamic LDIF is built.<br>
	 *  Using ldif implies that a hierarchical schema has been defined to
	 *  organize the information.<br>
	 *  In the <em><code>InfoCollector-Info</code></em> **/
	protected StringBuffer _ldif = new StringBuffer() ;
	
	
	/**
	 * This method determine the application behaviour, according to the 'mode' specified
	 * as argument. It launches the method related to 'mode' argument to create LDIF.<br>
	 * Each launched method is an abstract method because it have to know how to extract
	 * information from the SE, so in the specific way provided from that SE type.<br>
	 * That is done in a <code>InfoImporter</code> SE-specific extension class.<br><br>  
	 */
	protected void createLDIF(){
		if(InfoProvider.getmode().compareToIgnoreCase("provider")==0){
			executeProvider(); 
		}else if(InfoProvider.getmode().compareToIgnoreCase("plugin")==0){
			executePlugin();
		}
	}
	
	
	/**
	 * This method is implemented in an <code>InfoImporter</code> SE-specific extension 
	 * class to retrive the dynamic information from the SE in the specific way provided 
	 * from that SE type, for a given Schema, in <b>plugin</b> mode.<br>
	 */
	protected abstract void executePlugin();
	
	
	/**
	 * This method is implemented in an <code>InfoImporter</code> SE-specific extension 
	 * class to retrive the dynamic information from the SE in the specific way provided 
	 * from that SE type, for a given Schema, in <b>provider</b> mode.<br>
	 */
	protected abstract void executeProvider();
	
	
	/**
	 * This method initializes the <code>_ldif</code> String and return it.
	 * @return <code>_ldif</code> String
	 */
	public String getLDIF(){
		createLDIF();
		return _ldif.toString();
	}

}
