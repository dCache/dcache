/*
 * AuthorizationConfig.java
 *
 * Created on March 6, 2005
 */
                                                                                                                                                                                                     
package diskCacheV111.services.authorization;

import java.util.*;
import java.io.*;
import java.lang.*;

/**
 *
 *  @author Abhishek Singh Rana
 */

public class AuthorizationConfig {

	private static final String CONFIG_FILENAME="dcachesrm-gplazma.policy";
	public static final int MAX_PLUGINS=4;

	public static final String VO_MAPPING_PLUGIN_SWITCH_MARK="saml-vo-mapping=";
	public static final String VO_MAPPING_PLUGIN_PRIORITY_MARK="saml-vo-mapping-priority=";
	public static final String VO_MAPPING_PLUGIN_CONF_1_MARK="mappingServiceUrl=";
    public static final String VO_MAPPING_PLUGIN_CONF_2_MARK="saml-vo-mapping-cache-lifetime=";

    public static final String KPWD_PLUGIN_SWITCH_MARK="kpwd=";
	public static final String KPWD_PLUGIN_PRIORITY_MARK="kpwd-priority=";
	public static final String KPWD_PLUGIN_CONF_1_MARK="kpwdPath=";

	public static final String GRIDMAPFILE_PLUGIN_SWITCH_MARK="grid-mapfile=";
	public static final String GRIDMAPFILE_PLUGIN_PRIORITY_MARK="grid-mapfile-priority=";
	public static final String GRIDMAPFILE_PLUGIN_CONF_1_MARK="gridMapFilePath=";
	public static final String GRIDMAPFILE_PLUGIN_CONF_2_MARK="storageAuthzPath=";

	public static final String GPLAZMA_LITE_VOROLE_MAPPING_PLUGIN_SWITCH_MARK="gplazmalite-vorole-mapping=";
	public static final String GPLAZMA_LITE_VOROLE_MAPPING_PLUGIN_PRIORITY_MARK="gplazmalite-vorole-mapping-priority=";
	public static final String GPLAZMA_LITE_VOROLE_MAPPING_CONF_1_MARK="gridVoRolemapPath=";
	public static final String GPLAZMA_LITE_VOROLE_MAPPING_CONF_2_MARK="gridVoRoleStorageAuthzPath=";

	public static final String VO_MAPPING_SIGNAL="VO_MAPPING_green";
	public static final String GRIDMAPFILE_SIGNAL="GRIDMAPFILE_green";
	public static final String KPWD_SIGNAL="KPWD_green";
	public static final String GPLAZMA_LITE_VOROLE_MAPPING_SIGNAL="GPLAZMA_LITE_VOROLE_MAPPING_green";

	private HashMap authServConfig = new HashMap();
	private static HashMap authServConfig_static;
  private static long prev_refresh_time=0;

  public AuthorizationConfig(String filename)
  	throws IOException {
     	try {
        	String fileSeparator = System.getProperty("file.separator");
        	String[] testFilenamePath = filename.split(fileSeparator);
        	String testFilename = testFilenamePath[testFilenamePath.length-1];
         	if (!testFilename.equals(CONFIG_FILENAME)) {
           		System.out.println("Authorization Policy configuration filename " + testFilename + " is not as expected.");
				System.out.println("WARNING: Possible security violation.");
        	}
		} catch(Exception se) {
        	System.err.println("Exception in testing file: " +se);
     	}
		//System.out.println("- - -                 Activating     g P L A Z M A    Policy   Loader                 - - - ");
          read(filename);
    }

  private synchronized void read(String filename) throws IOException {
      long current_time = System.currentTimeMillis();
      File config = new File(filename);
      boolean readable = config.canRead();
      if(!readable) System.out.println("WARNING: Could not read policy file " + filename + ". Will try to use cached copy.");
      if(readable && config.lastModified() > prev_refresh_time) {
        FileReader fr = new FileReader(config);
        BufferedReader reader = new BufferedReader(fr);
        try {
          read(reader);
        } finally {
          reader.close();
        }
        prev_refresh_time = current_time;
        authServConfig_static = authServConfig;
      } else {
        authServConfig = authServConfig_static;
      }
  }


  private void read(BufferedReader reader)
	throws IOException {
        boolean eof = false;
        String line;
        while((line = reader.readLine()) != null) {
            line = line.trim();
			parseLine(VO_MAPPING_PLUGIN_SWITCH_MARK, line);
			parseLine(VO_MAPPING_PLUGIN_PRIORITY_MARK, line);
			parseLine(VO_MAPPING_PLUGIN_CONF_1_MARK, line);
            parseLine(VO_MAPPING_PLUGIN_CONF_2_MARK, line);
            parseLine(KPWD_PLUGIN_SWITCH_MARK, line);
			parseLine(KPWD_PLUGIN_PRIORITY_MARK, line);
			parseLine(KPWD_PLUGIN_CONF_1_MARK, line);
			parseLine(GRIDMAPFILE_PLUGIN_SWITCH_MARK, line);
			parseLine(GRIDMAPFILE_PLUGIN_PRIORITY_MARK, line);
			parseLine(GRIDMAPFILE_PLUGIN_CONF_1_MARK, line);
			parseLine(GRIDMAPFILE_PLUGIN_CONF_2_MARK, line);
			parseLine(GPLAZMA_LITE_VOROLE_MAPPING_PLUGIN_SWITCH_MARK, line);
			parseLine(GPLAZMA_LITE_VOROLE_MAPPING_PLUGIN_PRIORITY_MARK, line);
			parseLine(GPLAZMA_LITE_VOROLE_MAPPING_CONF_1_MARK, line);
			parseLine(GPLAZMA_LITE_VOROLE_MAPPING_CONF_2_MARK, line);
        }
	}

	private void parseLine(String thisMark, String thisLine) {
		if(thisLine.startsWith(thisMark)) {
			fillMap(thisMark,thisLine);
		}	
	}	
	
	private void fillMap(String currentMark, String currentLine) {
		String mapValue = currentLine.substring(currentMark.length()).trim();
		String mapKey = currentMark;
			authServConfig.put(mapKey,mapValue);
	}

	private Vector buildPriorityConfig() 
	throws Exception {
		String switchOfVOMapping = getVOMappingSwitch();
		switchOfVOMapping = removeQuotes(switchOfVOMapping);
		String switchOfKpwd = getKpwdSwitch();
		switchOfKpwd = removeQuotes(switchOfKpwd);
		String switchOfGridMapFile = getGridMapFileSwitch();
		switchOfGridMapFile = removeQuotes(switchOfGridMapFile);
		String switchOfGPLiteVORoleMapping = getGPLiteVOMappingSwitch();
		switchOfGPLiteVORoleMapping = removeQuotes(switchOfGPLiteVORoleMapping);

		Vector pluginPriorityConfig = new Vector(MAX_PLUGINS, 1);
		pluginPriorityConfig.setSize(MAX_PLUGINS);
		
		if (switchOfGPLiteVORoleMapping != null && switchOfGPLiteVORoleMapping != "" && switchOfGPLiteVORoleMapping.equals("ON")) {
			int priorityOfGPLiteVORoleMapping = getGPLiteVORoleMappingPriority();
			//System.out.println("priorityOfGPLiteVORoleMapping is: " +priorityOfGPLiteVORoleMapping);
			if (priorityOfGPLiteVORoleMapping > 0 && priorityOfGPLiteVORoleMapping < MAX_PLUGINS+1) {
				pluginPriorityConfig.setElementAt(GPLAZMA_LITE_VOROLE_MAPPING_SIGNAL, priorityOfGPLiteVORoleMapping-1);
			}
		}
		if (switchOfGridMapFile != null && switchOfGridMapFile != "" && switchOfGridMapFile.equals("ON")) {
			int priorityOfGridMapFile = getGridMapFilePriority();
			//System.out.println("priorityOfGridMapFile is: " +priorityOfGridMapFile);	
			if (priorityOfGridMapFile > 0 && priorityOfGridMapFile < MAX_PLUGINS+1) {
				pluginPriorityConfig.setElementAt(GRIDMAPFILE_SIGNAL, priorityOfGridMapFile-1);
			}
		}	
		if (switchOfVOMapping != null && switchOfVOMapping != "" && switchOfVOMapping.equals("ON")) {
			int priorityOfVOMapping = getVOMappingPriority();
			//System.out.println("priorityOfVOMapping is: " +priorityOfVOMapping);
			if (priorityOfVOMapping > 0 && priorityOfVOMapping < MAX_PLUGINS+1) {
				pluginPriorityConfig.setElementAt(VO_MAPPING_SIGNAL, priorityOfVOMapping-1);
			}		
		}
		if (switchOfKpwd != null && switchOfKpwd != "" && switchOfKpwd.equals("ON")) {
			int priorityOfKpwd = getKpwdPriority();
			//System.out.println("priorityOfKpwd is: " +priorityOfKpwd);
			if (priorityOfKpwd > 0 && priorityOfKpwd < MAX_PLUGINS+1) {
				pluginPriorityConfig.setElementAt(KPWD_SIGNAL, priorityOfKpwd-1);
			}	 
		}
		return pluginPriorityConfig;
	}		

	private String removeQuotes(String quotedString)
	throws Exception {
		if (quotedString != null) {
			quotedString = quotedString.replace('\"',' ').trim();
		}
		return quotedString;
	}	
	
	private int getVOMappingPriority() 
	throws Exception {
		return getPriority(VO_MAPPING_PLUGIN_PRIORITY_MARK);
	}	

	private int getKpwdPriority()
	throws Exception {
		return getPriority(KPWD_PLUGIN_PRIORITY_MARK);
	}	

	private int getGridMapFilePriority() 
	throws Exception {
		return getPriority(GRIDMAPFILE_PLUGIN_PRIORITY_MARK);
	}

	private int getGPLiteVORoleMappingPriority() 
	throws Exception {
		return getPriority(GPLAZMA_LITE_VOROLE_MAPPING_PLUGIN_PRIORITY_MARK);
	}	

	private int getPriority(String thisPriorityMark) 
	throws Exception {
		String priority = (String)authServConfig.get(thisPriorityMark);
		//System.out.println("string priority of " +thisPriorityMark+ " is :" +priority);
		if (priority != null) {
			priority = priority.replace('\"',' ').trim();
		}	
		if (  (priority != null) && ( 
		(priority.matches('\"'+"[\\d]"+'\"')) || 
		(priority.matches("[\\s\\d\\s]")) || 
		(priority.matches('\"'+"[\\W\\d\\W]"+'\"')) 
		)  
		) {
			priority = priority.replace('\"',' ').trim();
			return Integer.parseInt(priority);
		} 
		else {
			return -1;
		}	
	}	
	
	private String getVOMappingSwitch() {
		return (String)authServConfig.get(VO_MAPPING_PLUGIN_SWITCH_MARK);
	}	

	private String getKpwdSwitch() {
		return (String)authServConfig.get(KPWD_PLUGIN_SWITCH_MARK);
	}	

	private String getGridMapFileSwitch() {
		return (String)authServConfig.get(GRIDMAPFILE_PLUGIN_SWITCH_MARK);
	}

	private String getGPLiteVOMappingSwitch() {
		return (String)authServConfig.get(GPLAZMA_LITE_VOROLE_MAPPING_PLUGIN_SWITCH_MARK);
	}

	public String getMappingServiceUrl() 
	throws Exception {
		return removeQuotes((String)authServConfig.get(VO_MAPPING_PLUGIN_CONF_1_MARK));
	}	

    public String getMappingServiceCacheLifetime()
	throws Exception {
		return removeQuotes((String)authServConfig.get(VO_MAPPING_PLUGIN_CONF_2_MARK));
	}

    public String getKpwdPath()
	throws Exception {
		return removeQuotes((String)authServConfig.get(KPWD_PLUGIN_CONF_1_MARK));
	}
	
	public String getGridMapFilePath() 
	throws Exception {
		return removeQuotes((String)authServConfig.get(GRIDMAPFILE_PLUGIN_CONF_1_MARK));
	}

	public String getStorageAuthzPath() 
	throws Exception {
		return removeQuotes((String)authServConfig.get(GRIDMAPFILE_PLUGIN_CONF_2_MARK));
	}

	public String getGridVORoleMapPath()
	throws Exception {
		return removeQuotes((String)authServConfig.get(GPLAZMA_LITE_VOROLE_MAPPING_CONF_1_MARK));
	}	
	
	public String getGridVORoleStorageAuthzPath()
	throws Exception {
		return removeQuotes((String)authServConfig.get(GPLAZMA_LITE_VOROLE_MAPPING_CONF_2_MARK));	
	}
	
	public Vector getpluginPriorityConfig() 
	throws Exception {
		Vector sendpluginPriorityConfig = buildPriorityConfig();
		return sendpluginPriorityConfig;
	}

	public String getKpwdSignal()
	throws Exception {
		return KPWD_SIGNAL;
	}

	public String getVOMappingSignal()
	throws Exception {
		return VO_MAPPING_SIGNAL;
	}

	public String getGridMapFileSignal()
	throws Exception {
		return GRIDMAPFILE_SIGNAL;
	}
	public String getGPLiteVORoleMappingSignal()
	throws Exception {
		return GPLAZMA_LITE_VOROLE_MAPPING_SIGNAL;
	}
	
} //end of AuthorizationConfig class
