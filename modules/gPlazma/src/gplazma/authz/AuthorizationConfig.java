/*
 * AuthorizationConfig.java
 *
 * Created on March 6, 2005
 */

package gplazma.authz;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.io.*;
import java.lang.*;

/**
 *
 *  @author Abhishek Singh Rana
 */

public class AuthorizationConfig {

    private static final Logger log = LoggerFactory.getLogger(AuthorizationConfig.class);
    private long authRequestID;

    private String authConfigFileName;
    public static final int MAX_PLUGINS=5;

    public static final String XACML_MAPPING_PLUGIN_SWITCH_MARK="xacml-vo-mapping=";
    public static final String XACML_MAPPING_PLUGIN_PRIORITY_MARK="xacml-vo-mapping-priority=";
    public static final String XACML_MAPPING_PLUGIN_CONF_1_MARK="XACMLmappingServiceUrl=";
    public static final String XACML_MAPPING_PLUGIN_CONF_2_MARK="xacml-vo-mapping-cache-lifetime=";

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
    public static final String GPLAZMA_LITE_VOROLE_MAPPING_CONF_3_MARK="vomsValidation=";

    public static final String XACML_MAPPING_SIGNAL="XACML_MAPPING_green";
    public static final String VO_MAPPING_SIGNAL="VO_MAPPING_green";
    public static final String GRIDMAPFILE_SIGNAL="GRIDMAPFILE_green";
    public static final String KPWD_SIGNAL="KPWD_green";
    public static final String GPLAZMA_LITE_VOROLE_MAPPING_SIGNAL="GPLAZMA_LITE_VOROLE_MAPPING_green";

    public static final String GPLAZMA_USE_SAZ_SWITCH_MARK="saz-client=";
    public static final String GPLAZMA_SAZ_SERVER_HOST_MARK="SAZ_SERVER_HOST=";
    public static final String GPLAZMA_SAZ_SERVER_PORT_MARK="SAZ_SERVER_PORT=";
    public static final String GPLAZMA_SAZ_SERVER_DN_MARK="SAZ_SERVER_DN=";

    private HashMap authServConfig = new HashMap();
    private static HashMap authServConfig_static;
    private static long prev_refresh_time=0;

    public AuthorizationConfig(String filename, long authRequestID)
            throws IOException {
        this.authRequestID=authRequestID;
        authConfigFileName = filename;
        log.debug("AuthorizationConfig reading " + filename);
        read(filename);
    }

    private synchronized void read(String filename) throws IOException {
        long current_time = System.currentTimeMillis();
        File config = new File(filename);
        boolean readable = config.canRead() || prev_refresh_time==0;
        if(!readable) log.warn("WARNING: Could not read policy file " + filename + ". Will use cached copy.");
        if(readable && config.lastModified() >= prev_refresh_time) {
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
        String line;
        while((line = reader.readLine()) != null) {
            line = line.trim();
            parseLine(XACML_MAPPING_PLUGIN_SWITCH_MARK, line);
            parseLine(XACML_MAPPING_PLUGIN_PRIORITY_MARK, line);
            parseLine(XACML_MAPPING_PLUGIN_CONF_1_MARK, line);
            parseLine(XACML_MAPPING_PLUGIN_CONF_2_MARK, line);
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
            parseLine(GPLAZMA_LITE_VOROLE_MAPPING_CONF_3_MARK, line);
            parseLine(GPLAZMA_USE_SAZ_SWITCH_MARK, line);
            parseLine(GPLAZMA_SAZ_SERVER_HOST_MARK, line);
            parseLine(GPLAZMA_SAZ_SERVER_PORT_MARK, line);
            parseLine(GPLAZMA_SAZ_SERVER_DN_MARK, line);
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
        authServConfig.put(mapKey, mapValue);
    }

    private Vector buildPriorityConfig()
            throws Exception {
        String switchOfXACMLMapping = getXACMLMappingSwitch();
        switchOfXACMLMapping = removeQuotes(switchOfXACMLMapping);
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

        if ("ON".equals(switchOfGPLiteVORoleMapping)) {
            int priorityOfGPLiteVORoleMapping = getGPLiteVORoleMappingPriority();
            //log.trace("priorityOfGPLiteVORoleMapping is: " +priorityOfGPLiteVORoleMapping);
            if (priorityOfGPLiteVORoleMapping > 0 && priorityOfGPLiteVORoleMapping < MAX_PLUGINS+1) {
                pluginPriorityConfig.setElementAt(GPLAZMA_LITE_VOROLE_MAPPING_SIGNAL, priorityOfGPLiteVORoleMapping-1);
            }
        }
        if ("ON".equals(switchOfGridMapFile)) {
            int priorityOfGridMapFile = getGridMapFilePriority();
            //log.trace("priorityOfGridMapFile is: " +priorityOfGridMapFile);
            if (priorityOfGridMapFile > 0 && priorityOfGridMapFile < MAX_PLUGINS+1) {
                pluginPriorityConfig.setElementAt(GRIDMAPFILE_SIGNAL, priorityOfGridMapFile-1);
            }
        }
        if ("ON".equals(switchOfVOMapping)) {
            int priorityOfVOMapping = getVOMappingPriority();
            //log.trace("priorityOfVOMapping is: " +priorityOfVOMapping);
            if (priorityOfVOMapping > 0 && priorityOfVOMapping < MAX_PLUGINS+1) {
                pluginPriorityConfig.setElementAt(VO_MAPPING_SIGNAL, priorityOfVOMapping-1);
            }
        }
        if ("ON".equals(switchOfXACMLMapping)) {
            int priorityOfXACMLeMapping = getXACMLMappingPriority();
            //log.trace("priorityOfXACMLeMapping is: " +priorityOfXACMLeMapping);
            if (priorityOfXACMLeMapping > 0 && priorityOfXACMLeMapping < MAX_PLUGINS+1) {
                pluginPriorityConfig.setElementAt(XACML_MAPPING_SIGNAL, priorityOfXACMLeMapping-1);
            }
        }
        if ("ON".equals(switchOfKpwd)) {
            int priorityOfKpwd = getKpwdPriority();
            //log.trace("priorityOfKpwd is: " +priorityOfKpwd);
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

    private int getXACMLMappingPriority()
            throws Exception {
        return getPriority(XACML_MAPPING_PLUGIN_PRIORITY_MARK);
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
        //log.trace("string priority of " +thisPriorityMark+ " is :" +priority);
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

    private String getXACMLMappingSwitch() {
        return (String)authServConfig.get(XACML_MAPPING_PLUGIN_SWITCH_MARK);
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

    public String getVOMappingServiceUrl()
            throws Exception {
        return removeQuotes((String)authServConfig.get(VO_MAPPING_PLUGIN_CONF_1_MARK));
    }

    public String getVOMappingServiceCacheLifetime()
            throws Exception {
        return removeQuotes((String)authServConfig.get(VO_MAPPING_PLUGIN_CONF_2_MARK));
    }

    public String getXACMLMappingServiceUrl()
            throws Exception {
        return removeQuotes((String)authServConfig.get(XACML_MAPPING_PLUGIN_CONF_1_MARK));
    }

    public String getXACMLMappingServiceCacheLifetime()
            throws Exception {
        return removeQuotes((String)authServConfig.get(XACML_MAPPING_PLUGIN_CONF_2_MARK));
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

    public boolean getVOMSValidation()
            throws Exception {
        String switchOfVOMSValidation = getVOMSValidationSwitch();
        return switchOfVOMSValidation != null && !switchOfVOMSValidation.equals("") && switchOfVOMSValidation.equals("true");
    }

    private String getVOMSValidationSwitch()
            throws Exception {
        return removeQuotes((String)authServConfig.get(GPLAZMA_LITE_VOROLE_MAPPING_CONF_3_MARK));
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

    public String getXACMLMappingSignal()
            throws Exception {
        return XACML_MAPPING_SIGNAL;
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

    public boolean getSazClientOn()
            throws Exception {
        String switchOfSazClient = getSazClientSwitch();
        return switchOfSazClient != null && !switchOfSazClient.equals("") && switchOfSazClient.equals("ON");
    }

    private String getSazClientSwitch()
            throws Exception {
        return removeQuotes((String)authServConfig.get(GPLAZMA_USE_SAZ_SWITCH_MARK));
    }

    public String getSazServerHost()
            throws Exception {
        return removeQuotes((String)authServConfig.get(GPLAZMA_SAZ_SERVER_HOST_MARK));
    }

    public String getSazServerPort()
            throws Exception {
        return removeQuotes((String)authServConfig.get(GPLAZMA_SAZ_SERVER_PORT_MARK));
    }

    public String getSazServerDN()
            throws Exception {
        return removeQuotes((String)authServConfig.get(GPLAZMA_SAZ_SERVER_DN_MARK));
    }

    public String getAuthConfigFileName() {
        return authConfigFileName;
    }

} //end of AuthorizationConfig class
