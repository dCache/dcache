package infoDynamicSE;

import java.net.*;
import java.io.*;
import java.util.*;
import java.util.regex.*;
import org.dcache.services.infoCollector.GlueSchemaV1_2;


/**
 * InfoImporter implementation specific for:<ul> 
 * <li>SE = <em>dCache</em></li> 
 * <li>Schema for dynamic information = <em>Glue Schema v1.2</em></li>
 * </ul><br>
 * This class is the complete implementation of the InfoCollector-InfoProvider 
 * architecture.<br>
 * In this class dynamic information are picked from dCache server by mean the
 * <em>InfoCollector</em> component. This information are stored in this object,
 * and then organized in LDIF format.<br> 
 * To build dynamic LDIF String, the static LDIF file is used as a skeleton for
 * 'plugin' mode and to know the chunk key in 'provider' mode. 
 * @see InfoImporter  
 */
public class InfoImporter_dcache extends InfoImporter{
	
	
	/** Schema adopted. The information arrived from dCache 
	 *  are encapsulated in a GlueSchemaV1_2 object that will
	 *  be linkend here.
	 **/
	private GlueSchemaV1_2 _schema = null;
	
	
	/** Static file's content is stored here line-by-line **/
	ArrayList _sfile = new ArrayList();
	
	
	/**
	 * Constructor<br>
	 * In this method the static ldif file is read and the content is stored
	 * line-by-line in an ArrayList.<br> After that it opens a tcp connection 
	 * with dCache system about which we want information. Throught this 
	 * connection will be imported a <code>GlueSchemaV1_2</code> object from 
	 * dCache InfoCollector component. The object imported is upcasted and 
	 * its reference is linked to <code>_schema<code>.
	 */
	public InfoImporter_dcache (){
		
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
		
		String port = "22111";
		if(Opts.get("port")!=null) port=Opts.get("port");
		try{
			InetAddress addr = InetAddress.getByName(Opts.get("host"));
			Socket socket = new Socket(addr,Integer.parseInt(port));
		
			ObjectInputStream in =
				new ObjectInputStream(
						socket.getInputStream());
			
			Object obj = in.readObject(); 
			_schema = (GlueSchemaV1_2)obj;
			
			in.close();
			socket.close();		
		}catch(Exception e){
			e.printStackTrace(System.err);
		}	
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
	 * each VO space.
	 **/
	protected void executePlugin(){
		
		if(_schema==null || _sfile.isEmpty()) return;
		
		// Find "dn: GlueSEUniqueID=" line to update GlueSESizeTotal and GlueSESizeFree values
		//Pattern p1 = Pattern.compile("^dn:\\s* GlueSEUniqueID=([\\w+\\.]+).*", Pattern.CASE_INSENSITIVE);
		// deriese: This didnt include dashes. 
		Pattern p1 = Pattern.compile("^dn:\\s* GlueSEUniqueID=(" + 
					     "[A-Za-z](?:[-A-Za-z0-9]{0,61}[A-Za-z0-9])?" +        // valid hostname
					     "(?:\\.[A-Za-z](?:[-A-Za-z0-9]{0,61}[A-Za-z0-9])?)*" + // valid .domain
					     "),.*", Pattern.CASE_INSENSITIVE);

		// Find "dn: GlueSALocalID=" line to know the VO's names and to update 
		// 	 	GlueSAStateAvailableSpace and GlueSAStateUsedSpace for each VO.
		Pattern p2 = Pattern.compile("^dn:\\s* GlueSALocalID=(\\w+).*", Pattern.CASE_INSENSITIVE);
		Matcher m;
		
		String s;
		Iterator it = _sfile.iterator();
		while(it.hasNext()){
			s = (String)it.next();
			m = p1.matcher(s);
			while(m.find()){
				_ldif.append(s+'\n');
				_ldif.append("GlueSESizeTotal: "+_schema.se.sizeTotal+'\n');
				_ldif.append("GlueSESizeFree: "+_schema.se.sizeFree+'\n'+'\n');
			}
			m = p2.matcher(s);
			while(m.find()){
				for(int i=0; i<_schema.se.storage_area.length; i++)
					if(_schema.se.storage_area[i].localID.compareTo(m.group(1))==0){
						_ldif.append(s+'\n');
						_ldif.append("GlueSAStateAvailableSpace: "+_schema.se.storage_area[i].state_availableSpace+'\n');
						_ldif.append("GlueSAStateUsedSpace: "+_schema.se.storage_area[i].state_usedSpace+'\n'+'\n');
				} 
			}
		
		}
	}
	
	
	
	
	/**
	 * Dynamic Information building as <b>provider</b> in the LDIF format.<br>
	 * This method puts information in the ldif output as new 'dn:' nodes that
	 * will be simply added to the ldif published by MDS. 
	 * The information added here are typically about Access protocols and Control
	 * protocols.<br>
	 * This method scrolls the static ldif list to find the "dn: GlueSEUniqueID="
	 * line. This 'dn:' is needed for the chunk key in new nodes. 
	 * <i>Regular expressions</i> are employed to find that line.
	 **/
	protected void executeProvider(){
		
		if(_schema==null || _sfile.isEmpty()) return;
		
		String _GlueSEUniqueID = "localhost.localdomain";
		String _GlueSiteUniqueID = "unknown";
		
		// Find "dn: GlueSEUniqueID=" line to update GlueSESizeTotal and GlueSESizeFree values
		//Pattern p1 = Pattern.compile("^dn:\\s* GlueSEUniqueID=([\\w+\\.]+).*", Pattern.CASE_INSENSITIVE);
		// deriese: This didnt include dashes. 
		Pattern p1 = Pattern.compile("^dn:\\s* GlueSEUniqueID=(" + 
					     "[A-Za-z](?:[-A-Za-z0-9]{0,61}[A-Za-z0-9])?" +        // valid hostname
					     "(?:\\.[A-Za-z](?:[-A-Za-z0-9]{0,61}[A-Za-z0-9])?)*" + // valid .domain
					     "),.*", Pattern.CASE_INSENSITIVE);
		Matcher m;
		
		String s;
		Iterator it = _sfile.iterator();
		while(it.hasNext()){
			s = (String)it.next();
			m = p1.matcher(s);
			while(m.find()){
				_GlueSEUniqueID = m.group(1);
			}
                        if( s.startsWith("GlueForeignKey: GlueSiteUniqueID") ){
                           int pos = s.indexOf("=");
                           if( ( pos > -1 ) && ( pos < ( s.length() - 1 ) ) )_GlueSiteUniqueID = s.substring(pos+1);
                        }
		}
		
		if(_schema.se.access_protocol != null)
			for(int i=0; i<_schema.se.access_protocol.length; i++){
				_ldif.append('\n' + "dn: GlueSEAccessProtocolLocalID=" + _schema.se.access_protocol[i].localID + ',');
				_ldif.append("GlueSEUniqueID=" + _GlueSEUniqueID + ",mds-vo-name=local,o=grid\n");
				_ldif.append("objectClass: GlueSETop\n");
				_ldif.append("objectClass: GlueSEAccessProtocol\n");
				_ldif.append("objectClass: GlueKey\n");
				_ldif.append("objectClass: GlueSchemaVersion\n");
				_ldif.append("GlueSEAccessProtocolLocalID: " + _schema.se.access_protocol[i].localID + '\n');
				_ldif.append("GlueSEAccessProtocolType: " + _schema.se.access_protocol[i].type + '\n');
				_ldif.append("GlueSEAccessProtocolEndpoint: " + _schema.se.access_protocol[i].endPoint + '\n');
				//_ldif.append("GlueSEAccessProtocolCapability: \n");
				_ldif.append("GlueSEAccessProtocolVersion: " + _schema.se.access_protocol[i].version + '\n');
				_ldif.append("GlueSEAccessProtocolPort: " + _schema.se.access_protocol[i].port + '\n');
				_ldif.append("GlueChunkKey: GlueSEUniqueID=" + _GlueSEUniqueID + '\n');
				_ldif.append("GlueSchemaVersionMajor: 1" + '\n');
				_ldif.append("GlueSchemaVersionMinor: 2" + '\n');
			}
		
		if(_schema.se.control_protocol != null){
			for(int i=0; i<_schema.se.control_protocol.length; i++){
				_ldif.append('\n' + "dn: GlueSEControlProtocolLocalID=" + _schema.se.control_protocol[i].localID + ',');
				_ldif.append("GlueSEUniqueID=" + _GlueSEUniqueID + ",mds-vo-name=local,o=grid\n");
				_ldif.append("objectClass: GlueSETop\n");
				_ldif.append("objectClass: GlueSEControlProtocol\n");
				_ldif.append("objectClass: GlueKey\n");
				_ldif.append("objectClass: GlueSchemaVersion\n");
				_ldif.append("GlueSEControlProtocolLocalID: " + _schema.se.control_protocol[i].localID + '\n');
				_ldif.append("GlueSEControlProtocolType: " + _schema.se.control_protocol[i].type + '\n');
				_ldif.append("GlueSEControlProtocolEndpoint: " + _schema.se.control_protocol[i].endPoint + '\n');
				//_ldif.append("GlueSEControlProtocolCapability: \n");
				_ldif.append("GlueSEControlProtocolVersion: " + _schema.se.control_protocol[i].version + '\n');
				_ldif.append("GlueChunkKey: GlueSEUniqueID=" + _GlueSEUniqueID + '\n');
				_ldif.append("GlueSchemaVersionMajor: 1" + '\n');
				_ldif.append("GlueSchemaVersionMinor: 2" + '\n');

				_ldif.append('\n' + "dn: GlueServiceUniqueID=" + _schema.se.control_protocol[i].endPoint + ',');
				_ldif.append("mds-vo-name=local,o=grid\n");
				_ldif.append("objectClass: GlueTop\n");
				_ldif.append("objectClass: GlueService\n");
				_ldif.append("objectClass: GlueKey\n");
				_ldif.append("objectClass: GlueSchemaVersion\n");
				_ldif.append("GlueServiceUniqueID: " + _schema.se.control_protocol[i].endPoint + '\n');
				_ldif.append("GlueServiceName: " + _schema.se.control_protocol[i].endPoint + '\n');
				_ldif.append("GlueServiceType: srm_v1\n");
				_ldif.append("GlueServiceVersion: " + _schema.se.control_protocol[i].version + '\n');
				_ldif.append("GlueServiceEndpoint: " + _schema.se.control_protocol[i].endPoint + '\n');
				_ldif.append("GlueServiceURI: " + _schema.se.control_protocol[i].endPoint + '\n');
				_ldif.append("GlueServiceAccessPointURL: " + _schema.se.control_protocol[i].endPoint + '\n');
				_ldif.append("GlueServiceStatus: OK\n");
				_ldif.append("GlueForeignKey: GlueSiteUniqueID="+_GlueSiteUniqueID+"\n");
				_ldif.append("GlueSchemaVersionMajor: 1" + '\n');
				_ldif.append("GlueSchemaVersionMinor: 2" + '\n');

			}
		}
		
	}
	
	


}
