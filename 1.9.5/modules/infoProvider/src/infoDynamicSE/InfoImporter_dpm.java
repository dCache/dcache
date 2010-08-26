package infoDynamicSE;

import java.io.*;
import java.text.DecimalFormat;
import java.util.*;
import java.util.regex.*;


/**
 * InfoImporter implementation specific for:<ul> 
 * <li>SE = <em>dpm</em></li> 
 * <li>Schema for dynamic information = <em>none</em> (few information)</li>
 * </ul><br>
 * For now, only the plugin mode is available and  dynamic information are 
 * only picked from DPM server parsing the output from the <code>dpm-qryconf</code> 
 * command.<br> 
 * These information are related to global space.
 * @see InfoImporter  
 */
public class InfoImporter_dpm extends InfoImporter{
	
	/** Static file's content is stored here line-by-line **/
	private ArrayList _sfile = new ArrayList();
	
	
	private String _dpmQry = "";
	
	//private String _pool;
	//private String _gid;
	
	private long _total = 0;
	private long _free = 0;
	private long _used = 0;
	
	
	/**
	 * Constructor<br>
	 * In this method the static ldif file is read and the content is stored
	 * line-by-line in an ArrayList. After, the command <code>dpm-qryconf</code>
	 * is launched and its output (from stdout) is stored in a String to be 
	 * parsed subsequently.
	 */
	public InfoImporter_dpm (){
		
		String s;
		
		try{
			BufferedReader in = 	
				new BufferedReader(new FileReader(InfoProvider.getldif()));
			while((s = in.readLine()) != null) 
				_sfile.add(s);
			in.close();
		}catch(Exception e){
			e.printStackTrace(System.err);
		}
		
		try{
			Process p = Runtime.getRuntime().exec(Opts.get("qry")); // exec("/opt/lcg/bin/dpm-qryconf");
			BufferedReader in = 
				new BufferedReader(
						new InputStreamReader(p.getInputStream()));
			while((s = in.readLine()) != null) _dpmQry += '\n' + s;
			in.close();
			//System.out.println(_dpmQry);
			parseQry();
		}catch(IOException ioe){
			ioe.printStackTrace(System.err);
		}
	}
	
	/**
	 * This method parses <code>dpm-qryconf</code> command output to
	 * find total space and free space global information. <i>Regular 
	 * expressions</i> are here used.
	 */
	private void parseQry(){
		
		//Pattern p1 = Pattern.compile("^POOL\\s(\\w+)", Pattern.CASE_INSENSITIVE + Pattern.MULTILINE);
		//Pattern p2 = Pattern.compile("GID\\s(\\d+)", Pattern.CASE_INSENSITIVE + Pattern.MULTILINE);
		Pattern p3 = Pattern.compile("^\\s+CAPACITY\\s+([\\d\\.kMGTP]+)\\s+FREE\\s+([\\d\\.kMGTP]+)", 
													Pattern.CASE_INSENSITIVE + Pattern.MULTILINE);
		Matcher m;
		//m = p1.matcher(_dpmQry);
		//while(m.find()) _pool = m.group(1);
		//m = p2.matcher(_dpmQry);
		//while(m.find()) _gid = m.group(1);
		m = p3.matcher(_dpmQry);
		while(m.find()){
			_total = toBytes(m.group(1));
			_free = toBytes(m.group(2));
			_used = _total - _free;
		}
	}
	
	/**
	 * This methods converts a string like 10G, 30T, 100K, etc. in
	 * a long number that is the equivalent number of bytes. 
	 */
	private long toBytes(String num){
		double value = Double.valueOf(num.substring(0,num.length()-2)).doubleValue();
		double pow=1;
		char p = num.charAt(num.length()-1);
		switch(p){
			case 'k': pow=1E3; break;
			case 'M': pow=1E6; break;
			case 'G': pow=1E9; break;
			case 'T': pow=1E12; break;
			case 'P': pow=1E15; break;
		}
		return (long)(value*pow);
	}
	
	/**
	 * Dynamic Information building as <b>plugin</b> in the LDIF format.<br>
	 * This method scrolls the static ldif list and when it found a node that
	 * contains a dynamic attribute to update, it put in the LDIF ouput
	 * (<code>_ldif</code>) the full '<em>dn:</em>' line followed by the 
	 * attributes and relative dynamically updated values.<br>
	 * The <i>regular expressions</i> are employed to find the dynamic 
	 * attributes in the static ldif.<br>
	 * The information managed in plugin mode are typcally space-related.
	 * This method puts in ldif output information for global space and for
	 * each VO it replicates the global space information. 
	 * Glue Schema V1.2 are complied, so global space is expressed as int in 
	 * Gbytes while VO space as int in Kbytes. 
	 **/
	protected void executePlugin(){
		
		String s;
		DecimalFormat f = new DecimalFormat("#");
		
		if(_sfile.isEmpty()) return;
		
		Pattern p1 = Pattern.compile("^dn:\\s*GlueSEUniqueID=.*", Pattern.CASE_INSENSITIVE);
		Pattern p2 = Pattern.compile("^dn:\\s*GlueSALocalID=(\\w+).*", Pattern.CASE_INSENSITIVE);
		Matcher m;
		
		Iterator it = _sfile.iterator();
		while(it.hasNext()){
			s = (String)it.next();
			m = p1.matcher(s);
			while(m.find()){
				_ldif.append(s+'\n');
				s = f.format(_total/1E9);
				_ldif.append("GlueSESizeTotal: "+ s +'\n');
				s = f.format(_free/1E9);
				_ldif.append("GlueSESizeFree: "+ s +'\n'+'\n');
			}
			m = p2.matcher(s);
			while(m.find()){
				_ldif.append(s+'\n');
				s = f.format(_free/1E3);
				_ldif.append("GlueSAStateAvailableSpace: "+ s +'\n');
				s = f.format(_used/1E3);
				_ldif.append("GlueSAStateUsedSpace: "+ s +'\n'+'\n'); 
			}
		}
	}
	
	
	/**
	 * Dynamic Information building as <b>provider</b> in the LDIF format.<br>
	 * <i>not implemented</i>
	 */
	protected void executeProvider(){}
	

}


