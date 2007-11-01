package infoDynamicSE;

import java.io.*;

/**
 * LCG InfoProvider component to publish in MDS the dynamic information about an SE.<br>
 * This class is the *main class* of the package and when it's called it import the
 * dynamic information from the SE system and provide them to the grid information system.
 * <br>This implementation is actually designed to work with <em>dCache</em> and <em>DPM</em> 
 * SE, but it's extendible.
 * The information from this SE are delivered to MDS.
 * This implementation is designed for an <em>LCG</em> environment (tested starting from 2.6.0 
 * version) that include the <em>GIP</em> (Generic Information Provider) package.<br> 
 * It complies an advanced version of GIP, newer than GIP inside LCG 2.6.0 
 * (actually is lcg-info-generic-1.0.22-1).<br>
 * Using the <em>GIP-GIN</em> package, it's possible to publish in R-GMA system also, than
 * is possible to use the SE supported from this component in the <em>gLite</em> environment 
 * also. 
 * <br><br>
 * In particular, this class check the arguments passed at the command line to find all
 * the parameters needed to obtain the information from the SE and to know the name of 
 * the LDIF file that contains the <i>static information</i> for the SE.
 * <br><br>
 * <b>Very important:</b> This class and all classes called by this one can use the 
 * <i>Standard Output</i> *ONLY* to put out the dynamic information in LDIF format.
 * The output of this class doesn't appeare in any place, it will be parsed from 
 * <code>lcg-info-generic</code> script, so if some message different from the LDIF
 * declarations are sent on the Standard Output stream, it isn't read and make the
 * output NOT-UNDERSTANDABLE for the GIP script.<br>
 * The debug messages can be sent without any problem on the  <i>Standard Error</i> 
 * stream.
 * <br><br>
 * This class seeing the arguments recived, knows for what kind of SE it works.<br>
 * It creates an <code>InfoImporter</code> object that has a SE-specific type.
 * Every <code>InfoImporter</code> object has a <code>getLDIF()</code> method that
 * return a String that contains the dynamic information in LDIF format.
 * This String is finally sent on the <i>Standard Output</i>.
 * <br><br>
 * Notice that the LDIF format require that a hierarchical <em>schema</em> it is defined to
 * organize the information. This is achieved including in the classpath of this
 * package the classes in which the schema is defined. <br>
 * The ONLY point in which the used Schema is known, is inside the <code>InfoImporter</code>
 * SE-specific implementation class (It not impacts the abstact class).
 * 
 * @see InfoImporter
 */
public class InfoProvider {
	
	/** Pointer to an <code>InfoImporter</code> SE-specific object **/
	static InfoImporter _info;
	
	/** Usage instruction sent on the standard error stream if some essential
	 * argument is omitted. **/
	static private String _usage = 
		"\n\t Usage: InfoProvider  -mode=plugin|provider                                \\" +
		"\n\t                      -ldif=<static_ldif>                  				\\" +
		"\n\t                      -type=dcache|dpm                                     \\" +
		"\n\t                     [-host=<dCache_hostname>]                             \\" +
		"\n\t                     [-port=<dCache_InfoCollector_port>]                   \\" +
		"\n\t                     [-qry=<dpm-qryconf_path>]";
	
	static private String _mode;
	
	static private String _ldif;
	
	static public String getmode(){ return _mode; }
	
	static public String getldif(){ return _ldif; }
	
	/**
	 * Main method.<br>
	 * This method checks the arguments passed at the command line, creates
	 * the <code>InfoImporter</code> SE-specific object and prints out the
	 * dynamic information. 
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException{
		
		if(args.length == 0){
			System.err.println(_usage);
			return;
		}
		
		Opts.parse(args);
		
		// Check for coehrent arguments usage
		if( Opts.get("mode")==null || 
			Opts.get("type")==null ||
			Opts.get("ldif")==null ||
		    (Opts.get("mode").compareToIgnoreCase("plugin")!=0 
				&& Opts.get("mode").compareToIgnoreCase("provider")!=0 )
		   ){	
				System.err.println(_usage);
				return;
		}else{
			_mode = Opts.get("mode");
			_ldif = Opts.get("ldif");
		}
		 
        // --- dCache ---
		if(Opts.get("type").compareToIgnoreCase("dcache")==0 && 
				Opts.get("host")!=null){ 		
			_info = new InfoImporter_dcache();	
        // --- DPM ---
		}else if(Opts.get("type").compareToIgnoreCase("dpm")==0 &&
				Opts.get("qry")!=null){
			_info = new InfoImporter_dpm();
		}else{
			System.err.println(_usage);
			return;
		} 
		
		System.out.println(_info.getLDIF());
		return;
	}

	
}
