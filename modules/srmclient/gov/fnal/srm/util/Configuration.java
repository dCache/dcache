// $Id: Configuration.java,v 1.79 2007/10/25 01:37:22 litvinse Exp $
// $Author: litvinse $
/*
COPYRIGHT STATUS:
  Dec 1st 2001, Fermi National Accelerator Laboratory (FNAL) documents and
  software are sponsored by the U.S. Department of Energy under Contract No.
  DE-AC02-76CH03000. Therefore, the U.S. Government retains a  world-wide
  non-exclusive, royalty-free license to publish or reproduce these documents
  and software for U.S. Government purposes.  All documents and software
  available from this server are protected under the U.S. and Foreign
  Copyright Laws, and FNAL reserves all rights.
 
 Distribution of the software available from this server is free of
 charge subject to the user following the terms of the Fermitools
 Software Legal Information.
 
 Redistribution and/or modification of the software shall be accompanied
 by the Fermitools Software Legal Information  (including the copyright
 notice).
 
 The user is asked to feed back problems, benefits, and/or suggestions
 about the software to the Fermilab Software Providers.
 
 
 Neither the name of Fermilab, the  URA, nor the names of the contributors
 may be used to endorse or promote products derived from this software
 without specific prior written permission.
 
 
 
  DISCLAIMER OF LIABILITY (BSD):
 
  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
  "AS IS" AND ANY EXPRESS OR IMPLIED  WARRANTIES, INCLUDING, BUT NOT
  LIMITED TO, THE IMPLIED  WARRANTIES OF MERCHANTABILITY AND FITNESS
  FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL FERMILAB,
  OR THE URA, OR THE U.S. DEPARTMENT of ENERGY, OR CONTRIBUTORS BE LIABLE
  FOR  ANY  DIRECT, INDIRECT,  INCIDENTAL, SPECIAL, EXEMPLARY, OR
  CONSEQUENTIAL DAMAGES  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
  OF SUBSTITUTE  GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
  BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY  OF
  LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
  NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT  OF THE USE OF THIS
  SOFTWARE, EVEN IF ADVISED OF THE  POSSIBILITY OF SUCH DAMAGE.
 
 
  Liabilities of the Government:
 
  This software is provided by URA, independent from its Prime Contract
  with the U.S. Department of Energy. URA is acting independently from
  the Government and in its own private capacity and is not acting on
  behalf of the U.S. Government, nor as its contractor nor its agent.
  Correspondingly, it is understood and agreed that the U.S. Government
  has no connection to this software and in no manner whatsoever shall
  be liable for nor assume any responsibility or obligation for any claim,
  cost, or damages arising out of or resulting from the use of the software
  available from this server.
 
 
  Export Control:
 
  All documents and software available from this server are subject to U.S.
  export control laws.  Anyone downloading information from this server is
  obligated to secure any necessary Government licenses before exporting
  documents or software obtained from this server.
 */

/*
 * Configuration.java
 *
 * Created on April 23, 2003, 10:19 AM
 */

package gov.fnal.srm.util;

import java.io.IOException;
import java.io.FileWriter;
import java.io.File;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.FactoryConfigurationError;
import javax.xml.parsers.ParserConfigurationException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;
import org.w3c.dom.Comment;
import org.xml.sax.SAXException;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import org.dcache.srm.Logger;
import java.util.Map;
import java.util.HashMap;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
/**
 *
 * @author  timur
 */
public class Configuration {

	public static final String SRMCPCONFIGNAMESPACE="srmcp.srm.fnal.gov";
	private static final String webservice_pathv1 = "srm/managerv1";
	private static final String webservice_pathv2 = "srm/managerv2";
        
        @Option(
                name = "default_port",
                description = "default SRM port number",
                defaultValue = "8443", 
                required=false,
                log=true,
                save=true
                )
                private int srmDefaultPortNumber;

	public int getSrmDefaultPortNumber() {
		return srmDefaultPortNumber;
	}

 	public void setSrmDefaultPortNumber(int port) {
		this.srmDefaultPortNumber=port;
	}

        @Option(
                name = "debug",
                description = "boolean flag enables debug information (including stack traces) to be printed, true or false",
                defaultValue = "false", 
                required=false,
                log=true,
                save=true
                )
                private boolean debug;

	public boolean isDebug() {
		return debug;
	}

 	public void setDebug(boolean debug) {
		this.debug = debug;
	}

        @Option(
                name = "srmcphome",
                description = "path to srmcp home directory",
                defaultValue = "..", 
                required=false,
                log=true,
                save=true
                )
                private String srmcphome;

	public String getSrmcphome() {
		return srmcphome;
	}

	public void setSrmcphome(String srmcphome) {
		this.srmcphome = srmcphome;
	}

        @Option(
                name = "urlcopy",
                description = "path to the urlcopy script",
                defaultValue = "sbin/urlcopy.sh", 
                required=false,
                log=true,
                save=true
                )
                private String urlcopy;

	public String getUrlcopy() {
		return urlcopy;
	}

	public void setUrlcopy(String urlcopy) {
		this.urlcopy = urlcopy;
	}

        @Option(
                name = "gsiftpclient",
                description = "name of ftp client \"globus-url-copy\" or \"kftp\"",
                defaultValue = "globus-url-copy", 
                required=false,
                log=true,
                save=true
                )
                private String gsiftpclinet;

	public String getGsiftpclinet() {
		return gsiftpclinet;
	}

	public void setGsiftpclinet(String gsiftpclinet) {
		this.gsiftpclinet = gsiftpclinet;
	}

        @Option(
                name = "help",
                description = "help switch, true or false",
                defaultValue = "false", 
                required=false,
                log=true
                )
                private boolean help;


	public boolean isHelp() {
		return help;
	}

	public void setHelp(boolean help) {
		this.help = help;
	}

        @Option(
                name = "h",
                description = "help switch, true or false",
                defaultValue = "false", 
                required=false,
                log=true
                )
                private boolean is_help;


        @Option(
                name = "gsissl",
                description = "true if use http over gsi over ssl for SOAP invocations\n"+
                "or false to use plain http (no authentication or encryption)",
                defaultValue = "true", 
                required=false,
                log=true,
                save=true
                )
                private boolean gsissl;

	public boolean isGsissl() {
		return gsissl;
	}

	public void setGsissl(boolean gsissl) {
		this.gsissl = gsissl;
	}

        @Option(
                name = "mapfile",
                description = " path to the \"glue\" mapfile",
                defaultValue = "conf/SRMServerV1.map", 
                required=false,
                log=true,
                save=true
                )
                private String glue_mapfile;

 	public String getGlue_mapfile() {
		return glue_mapfile;
	}

	public void setGlue_mapfile(String glue_mapfile) {
		this.glue_mapfile = glue_mapfile;
	}

        @Option(
                name = "webservice_path",
                description = "the path to the wsdl in the web service url \"srm/managerv1.wsdl\" in case of srm in dcache",
                required=false,
                log=true,
                save=true
                )
                private String webservice_path;

        public String getWebservice_path() {
		String ws_path = webservice_path;
		if(ws_path ==null) {
			if(srm_protocol_version == 2) {
				ws_path = webservice_pathv2;
			} else {
				ws_path = webservice_pathv1;
			}
		}
		return ws_path;
	}

	public void setWebservice_path(String webservice_path) {
		this.webservice_path = webservice_path;
	}

        @Option(
                name = "webservice_protocol",
                description = "the protocol on which srm web service is published (usually http, https or httpg)",
                defaultValue = "http",
                required=false,
                log=true,
                save=true
                )
                private String webservice_protocol;

        public java.lang.String getWebservice_protocol() {
		return webservice_protocol;
	}

	public void setWebservice_protocol(java.lang.String webservice_protocol) {
		this.webservice_protocol = webservice_protocol;
	}
    
        
        @Option(
                name = "useproxy",
                description = "true to use user proxy or false to use certificates directly",
                defaultValue = "true",
                required=false,
                log=true,
                save=true
                )
                private boolean useproxy;
        
        public boolean isUseproxy() {
		return useproxy;
	}
        
	public void setUseproxy(boolean useproxy) {
		this.useproxy = useproxy;
	}

        @Option(
                name = "x509_user_proxy",
                description = "absolute path to user proxy",
                required=false,
                log=true,
                save=true
                )
                private String x509_user_proxy;

	public String getX509_user_proxy() {
		return x509_user_proxy;
	}
    
	public void setX509_user_proxy(String x509_user_proxy) {
		this.x509_user_proxy = x509_user_proxy;
	}

        @Option(
                name = "x509_user_cert",
                description = "absolute path to user (or host) certificate",
                required=false,
                log=true,
                save=true
                )
                private String x509_user_cert;

        public String getX509_user_cert() {
		return x509_user_cert;
	}

	public void setX509_user_cert(String x509_user_cert) {
		this.x509_user_cert = x509_user_cert;
	}

        @Option(
                name = "x509_user_key",
                description = "absolute path to user (or host) private key",
                required=false,
                log=true,
                save=true
                )
                private String x509_user_key;
        
        public String getX509_user_key() {
		return x509_user_key;
	}
    
	public void setX509_user_key(String x509_user_key) {
		this.x509_user_key = x509_user_key;
	}

        @Option(
                name = "x509_user_trusted_certificates",
                description = "absolute path to the trusted certificates directory",
                defaultValue = "/etc/grid-security/certificates",
                required=false,
                log=true,
                save=true
                )
                private String x509_user_trusted_certificates;
        
	public String getX509_user_trusted_certificates() {
		return x509_user_trusted_certificates;
	}
        
	public void setX509_user_trusted_certificates(String x509_user_trusted_certificates) {
		this.x509_user_trusted_certificates = x509_user_trusted_certificates;
	}

        @Option(
                name = "globus_tcp_port_range",
                description = "globus tcp port range",
                required=false,
                log=true,
                save=true
                )
                private String globus_tcp_port_range;
        
	public String getGlobus_tcp_port_range() {
		return globus_tcp_port_range;
	}
        
	public void setGlobus_tcp_port_range(String globus_tcp_port_range) {
		this.globus_tcp_port_range = globus_tcp_port_range;
	}
        
        @Option(
                name = "gss_expected_name",
                description = "gss expected name",
                required=false,
                log=true,
                save=true
                )
                private String gss_expected_name;
        
      	public String getGss_expected_name() {
		if (gss_expected_name == null){
			gss_expected_name = "host";
		}
		return gss_expected_name;
	}
    
	public void setGss_expected_name(String gss_expected_name) {
		this.gss_expected_name = gss_expected_name;
	}
  
        @Option(
                name = "protocols",
                description = "comma separated list of protocol names",
                defaultValue = "gsiftp,dcap,http",
                required=false,
                log=true,
                save=true
                )
                private String protocols_list;

  	public java.lang.String getProtocolsList() {
		return protocols_list;
	}

	public void setProtocolsList(java.lang.String protocols_list) {
		this.protocols_list = protocols_list;
	}

	private String[] protocols = new String[]   {"gsiftp","dcap","http"};

        public java.lang.String[] getProtocols() {
		return this.protocols;
	}

	public void setProtocols(java.lang.String[] protocols) {
		this.protocols = protocols;
	}

        
        @Option(
                name = "pushmode",
                description = "true for pushmode and false for pullmode",
                defaultValue = "false",
                required=false,
                log=true,
                save=true
                )
                private boolean pushmode;
        
      	public boolean isPushmode() {
		return pushmode;
	}

	public void setPushmode(boolean pushmode) {
		this.pushmode = pushmode;
	}
        
        @Option(
                name = "buffer_size",
                description = "buffer size, nonnegative integer",
                defaultValue = "131072",
                unit="bytes",
                required=false,
                log=true,
                save=true
                )
                private int buffer_size;
        
    	public int getBuffer_size() {
		return buffer_size;
	}

	public void setBuffer_size(int buffer_size) {
		this.buffer_size = buffer_size;
	}
        
        @Option(
                name = "tcp_buffer_size",
                description = "tcp buffer size, nonnegative integer, (0 means do not set tcp_buffer_size at all)",
                defaultValue = "0",
                unit="bytes",
                required=false,
                log=true,
                save=true
                )
                private int tcp_buffer_size;
        
	public int getTcp_buffer_size() {
		return tcp_buffer_size;
	}
        
	public void setTcp_buffer_size(int tcp_buffer_size) {
		this.tcp_buffer_size = tcp_buffer_size;
	}
      
        @Option(
                name = "streams_num",
                description = "number of streams, nonnegative integer",
                defaultValue = "10",
                required=false,
                log=true,
                save=true
                )
                private int streams_num;

	public int getStreams_num() {
		return streams_num;
	}
        
	public void setStreams_num(int streams_num) {
		this.streams_num = streams_num;
	}
        
        @Option(
                name = "conf",
                description = "name of the configuration file",
                defaultValue = "config.xml",
                required=false,
                log=true
                )
                private String config_file;

        @Option(
                name = "save_config_file",
                description = "path to the file in which the new configuration will be saved",
                required=false,
                log=true
                )
                private String save_config_file;
        
    	public java.lang.String getSave_config_file() {
		return save_config_file;
	}
        
	public void setSave_config_file(java.lang.String save_config_file) {
		this.save_config_file = save_config_file;
	}

	private Logger logger;
        
        @Option(
                name = "do_remove",
                description = "remove files when executing srm-release-files",
                defaultValue = "false",
                required=false,
                log=true,
                save=true
                )
                private boolean doRemove;

        public boolean getDoRemove() { 
		return this.doRemove;
	}
        
	public void setDoRemove(boolean yes) { 
		this.doRemove=yes;
	}
        
        @Option(
                name = "copy",
                description = " performs srm \"get\", \"put\", or \"copy\" depending on arguments",
                defaultValue = "false",
                required=false,
                log=true
                )
                private boolean copy;
       
        public boolean isCopy() {
		return copy;
	}

	public void setCopy(boolean copy) {
		this.copy = copy;
	}

        @Option(
                name = "bringOnline",
                description = "performs srmBringOnline",
                defaultValue = "false",
                required=false,
                log=true
                )
                private boolean bringOnline;
       
        public boolean isBringOnline() {
		return bringOnline;
	}

	public void setBringOnline(boolean bringOnline) {
		this.bringOnline = bringOnline;
	}
        
        @Option(
                name = "ping",
                description = "performs srm ping command (useful for diagnostics and version info)",
                defaultValue = "false",
                required=false,
                log=true
                )
                private boolean ping;
        
        public boolean isPing() {
		return ping;
	}

	public void setPing(boolean ping) {
		this.ping = ping;
	}
	//
	// SrmReserveSpace parameters
	//
        @Option(
                name = "reserveSpace",
                description = "performs explicit space reservation",
                defaultValue = "false",
                required=false,
                log=true
                )
                private boolean reserveSpace;

  	public boolean isReserveSpace() {
		return reserveSpace;
	}

        public void setReserveSpace(boolean reserveSpace) {
		this.reserveSpace = reserveSpace;
	}

        @Option(
                name = "array_of_client_networks",
                description = "comma separated array of client networks",
                required=false,
                log=true,
                save=true
                )
                private String array_of_client_networks;
        
	private String[] arrayOfClientNetworks;
        
        @Option(
                name = "retention_policy",
                description = "retention policy",
                required=false,
                log=true,
                save=true
                )
                private String retentionPolicy;

	public String getRetentionPolicy() {
		return retentionPolicy;
	}
    
	public void setRetentionPolicy(String s) {
		retentionPolicy=s;
	}
        
        @Option(
                name = "space_desc",
                description = "space reservation description",
                required=false,
                log=true
                )
                private String spaceTokenDescription;

 	public String getSpaceTokenDescription() {
		return spaceTokenDescription;
	}
    
	public void setSpaceTokenDescription(String s) {
		spaceTokenDescription=s;
	}

        @Option(
                name = "access_latency",
                description = "access latency",
                required=false,
                log=true,
                save=true
                )
                private String accessLatency;

	public String getAccessLatency() {
		return accessLatency;
	}
    
	public void setAccessLatency(String s) {
		accessLatency=s;
	}

        @Option(
                name = "access_pattern",
                description = "access pattern (\"TRANSFER_MODE\"|\"PROCESSING_MODE\")",
                defaultValue = "TRANSFER_MODE",
                required=false,
                log=true,
                save=true
                )
                private String accessPattern;

	public String getAccessPattern() {
		return accessPattern;
	}
    
	public void setAccessPattern(String s) {
		accessPattern=s;
	}

        @Option(
                name = "connection_type",
                description = "connection type, (\"WAN\"|\"LAN\")",
                defaultValue = "WAN",
                required=false,
                log=true,
                save=true
                )
                private String connectionType;

    	public String getConnectionType() {
		return connectionType;
	}
    
	public void setConnectionType(String s) {
		connectionType=s;
	}
        
        @Option(
                name = "desired_size",
                description = "desired space reservation size",
                defaultValue = "0",
                unit="bytes",
                required=false,
                log=true
                )
                private long desiredReserveSpaceSize;

     	public long getDesiredReserveSpaceSize() {
		return desiredReserveSpaceSize;
	}
    
	public void setDesiredReserveSpaceSize(long size) {
		desiredReserveSpaceSize=size;
	}

        @Option(
                name = "guaranteed_size",
                description = "guaranteed space reservation size",
                defaultValue = "0",
                unit="bytes",
                required=false,
                log=true
                )
                private long guaranteedReserveSpaceSize;

        
    	public long getGuaranteedReserveSpaceSize() {
		return guaranteedReserveSpaceSize;
	}
    
	public void setGuaranteedReserveSpaceSize(long size) {
		guaranteedReserveSpaceSize=size;
	}
 

        @Option(
                name = "lifetime",
                description = "desired lifetime in seconds",
                defaultValue = "0",
                unit="seconds",
                required=false,
                log=true
                )
                private long desiredLifetime;

	public long getDesiredLifetime() {
		return desiredLifetime;
	}
	//
	// SrmReleaseSpace parameters
	//
        @Option(
                name = "releaseSpace",
                description = "performs release of explicit  space reservation",
                defaultValue = "false",
                required=false,
                log=true
                )
                private boolean releaseSpace;

        public boolean isReleaseSpace() {
		return releaseSpace;
	}

        @Option(
                name = "space_token",
                description = "space reservation token",
                required=false,
                log=true
                )
                private String spaceToken;

       	public String getSpaceToken() {
		return spaceToken;
	}
        
	public void setSpaceToken(String s) {
		spaceToken=s;
	}
 
        @Option(
                name = "force",
                description = "force space reservation release",
                defaultValue = "false",
                required=false,
                log=true,
                save=true
                )
                private boolean forceFileRelease;
        
        
        public boolean getForceFileRelease() {
		return forceFileRelease;
	}
        
	public void setForceFileRelease(boolean yes) {
		forceFileRelease=yes;
	}
	//
	// SrmGetSpaceMetaData parameters
        // 
        @Option(
                name = "getSpaceMetaData",
                description = "retrieves and prints metadata for given space tokens",
                defaultValue = "false",
                required=false,
                log=true
                )
                private boolean getSpaceMetaData;

        public boolean isGetSpaceMetaData() {
		return getSpaceMetaData;
	}
        
	public void setGetSpaceMetaData(boolean getSpaceMetaData) {
		this.getSpaceMetaData = getSpaceMetaData;
	}
        
	private String[] spaceTokensList;
	private String[] from;
	private String to;


        @Option(
                name = "space_tokens",
                description = "comma separated list of space reservation tokens",
                required=false,
                log=true
                )
                private String space_tokens_list;
        
        @Option(
                name = "copyjobfile",
                description = "is the path to the text file containing list of sources and destination",
                required=false,
                log=true,
                save=true
                )
                private String copyjobfile;

	public java.lang.String getCopyjobfile() {
		return copyjobfile;
	}
        
	public void setCopyjobfile(java.lang.String copyjobfile) {
		this.copyjobfile = copyjobfile;
	}

        @Option(
                name = "wsdl_url",
                description = "full url to web service wsd>, this options overrides \"-webservice_path\" and \"-webservice_protocol\" options",
                required=false,
                log=true,
                save=true
                )
                private String wsdl_url;

	public java.lang.String getWsdl_url() {
		return wsdl_url;
	}

	public void setWsdl_url(java.lang.String wsdl_url) {
		this.wsdl_url = wsdl_url;
	}

        @Option(
                name = "use_urlcopy_script",
                description = "if true, use urlcopy script, otherwise use java native copiers",
                defaultValue = "false",
                required=false,
                log=true,
                save=true
                )
                private boolean use_urlcopy_script;

        
        public boolean isUse_urlcopy_script() {
		return use_urlcopy_script;
	}
        
	public void setUse_urlcopy_script(boolean use_urlcopy_script) {
		this.use_urlcopy_script = use_urlcopy_script;
	}


        @Option(
                name = "getFileMetaData",
                description = "gets srm meta data for specified surls",
                defaultValue = "false",
                required=false,
                log=true
                )
                private boolean getFileMetaData;
        
       	public boolean isGetFileMetaData() { return getFileMetaData; }

 	public void setGetFileMetaData(boolean getFileMetaData) {
		this.getFileMetaData = getFileMetaData;
	}
               
        @Option(
                name = "ls",
                description = "list content of directory",
                defaultValue = "false",
                required=false,
                log=true
                )
                private boolean ls;
        
        public boolean isls() { return ls; }

        public void setLs(boolean l) { 
                ls=l;
        }

        @Option(
                name = "getSpaceTokens",
                description =  "gets space tokens belonging to this user",
                defaultValue = "false",
                required=false,
                log=true
                )
                private boolean getSpaceTokens;

        public boolean isGetSpaceTokens() {
		return getSpaceTokens;
	}
        
	public void setGetSpaceTokens(boolean getSpaceTokens) {
		this.getSpaceTokens = getSpaceTokens;
	}

        @Option(
                name = "rm",
                description =  "remove file(s)",
                defaultValue = "false",
                required=false,
                log=true
                )
                private boolean is_rm;

        public boolean isRm() { return is_rm; }
        
        public void setRm(boolean r) { 
                is_rm = r;
        }

        @Option(
                name = "rmdir",
                description =  "remove empty directory tree",
                defaultValue = "false",
                required=false,
                log=true
                )
                private boolean is_rmdir;

        public boolean isRmdir() { return is_rmdir; }
        
        public void setRmdir(boolean r) { 
                is_rmdir = r;
        }

        @Option(
                name = "mv",
                description =  "performs srm \"mv\" of files and directories ",
                defaultValue = "false",
                required=false,
                log=true
                )
                private boolean is_mv;

        public boolean isMove() { return is_mv; }
        
        public void setMove(boolean r) { 
                is_mv = r;
        }

        @Option(
                name = "mkdir",
                description =  "create directory ",
                defaultValue = "false",
                required=false,
                log=true
                )
                private boolean is_mkdir;

        public boolean isMkdir() { return is_mkdir; }
        
        public void setMkdir(boolean r) { 
                is_mkdir = r;
        }

        @Option(
                name = "getPermissions",
                description =  "get permission of files",
                defaultValue = "false",
                required=false,
                log=true
                )
                private boolean getPermission;

        public boolean isGetPermission() { return getPermission;}

        public void setGetPermission(boolean r) { 
                getPermission = r;
        }

        @Option(
                name = "checkPermissions",
                description =  "check permission of files",
                defaultValue = "false",
                required=false,
                log=true
                )
                private boolean checkPermission;

        public boolean isCheckPermission() { return checkPermission;}

        public void setCheckPermission(boolean r) { 
                checkPermission = r;
        }

        @Option(
                name = "setPermissions",
                description =  "set permissions of files",
                defaultValue = "false",
                required=false,
                log=true
                )
                private boolean setPermission;

        public boolean isSetPermission() { return setPermission;}

        public void setSetPermission(boolean r) { 
                setPermission = r;
        }

        @Option(
                name = "getRequestSummary",
                description =  "is to retrieve a summary of the submitted request",
                defaultValue = "false",
                required=false,
                log=true
                )
                private boolean is_getRequestSummary;

        public boolean isGetRequestSummary() {
		return is_getRequestSummary;
	}
        
        public void setGetRequestSummary(boolean r) {
                is_getRequestSummary=r;
        }

        @Option(
                name = "getRequestTokens",
                description =  "retrieves request tokens for the clients requests, given client provided request description",
                defaultValue = "false",
                required=false,
                log=true
                )
                private boolean is_getRequestTokens;

	public boolean isGetRequestTokens() {
		return is_getRequestTokens;
	}
    
	public void setGetRequestTokens(boolean getRequestTokens) {
		this.is_getRequestTokens = getRequestTokens;
	}

        @Option(
                name = "abortFiles",
                description =  "to abort files",
                defaultValue = "false",
                required=false,
                log=true
                )
                private boolean is_AbortFiles;

	public boolean isAbortFiles() {
		return is_AbortFiles;
	}

	public void setAbortFiles(boolean yes) { 
		this.is_AbortFiles = yes;
	}

        @Option(
                name = "releaseFiles",
                description =  " to unpin files",
                defaultValue = "false",
                required=false,
                log=true
                )
                private boolean is_ReleaseFiles;

	public boolean isReleaseFiles() {
		return is_ReleaseFiles;
	}

	public void setReleaseFiles(boolean yes) { 
		this.is_ReleaseFiles = yes;
	}

        @Option(
                name = "request_desc",
                description =  "request token description",
                required=false,
                log=true
                )
                private String userRequestDescription;

	public String getUserRequestDescription() { 
		return userRequestDescription;
	}
    
	public void setUserRequestDescription(String desc) { 
		this.userRequestDescription=desc;
	}
    
        @Option(
                name = "type",
                description =  "permission type <ADD|REMOVE|CHANGE>",
                required=false,
                log=true
                )
                private String setPermissionType;
        
	public String getSetPermissionType() { 
		return this.setPermissionType;
	}
        
        public void setSetPermissionType(String x) { 
		this.setPermissionType=x;
	}

        @Option(
                name = "owner",
                description =  "owner permission mode <NONE,X,W,WR,R,RX,RW,RWX>",
                required=false,
                log=true
                )
                private String setOwnerPermissionMode;

        public String getSetOwnerPermissionMode() { 
		return this.setOwnerPermissionMode;
	}
        
	public void setSetOwnerPermissionMode(String x) { 
		this.setOwnerPermissionMode=x;
	}

        @Option(
                name = "group",
                description =  "group permission mode <NONE,X,W,WR,R,RX,RW,RWX>",
                required=false,
                log=true
                )
                private String setGroupPermissionMode;

        public String getSetGroupPermissionMode() { 
		return this.setGroupPermissionMode;
	}
        
	public void setSetGroupPermissionMode(String x) { 
		this.setGroupPermissionMode=x;
	}
    

        @Option(
                name = "other",
                description =  "world permission mode <NONE,X,W,WR,R,RX,RW,RWX>",
                required=false,
                log=true
                )
                private String setOtherPermissionMode;

        public String getSetOtherPermissionMode() { 
		return this.setOtherPermissionMode;
	}
        
	public void setSetOtherPermissionMode(String x) { 
		this.setOtherPermissionMode=x;
	}
    
	private String setPermissionSurl;

         @Option(
                name = "request_tokens",
                description =  "<id>,<id1>,<id2>... (comma separated list of Request Token(s))",
                required=false,
                log=true
                )
                 private String requestTokens;
	//
	// srmExtendFileLifeTime parameters
	//
         @Option(
                name = "request_token",
                description =  "request token",
                required=false,
                log=true
                )
                 private String srmExtendFileLifetimeRequestToken;
        
	public String getExtendFileLifetimeRequestToken() { 
		return srmExtendFileLifetimeRequestToken;
	}
        
        public void setExtendFileLifetimeRequestToken(String token) { 
		srmExtendFileLifetimeRequestToken=token;
	}

        @Option(
                name = "file_lifetime",
                description =  "number of seconds to add to current time to extend file lifetime of SURL(s)",
                required=false,
                log=true,
                save=true
                )
                private Integer newFileLifetime;

        public Integer getNewFileLifetime() { return newFileLifetime; } 

	public void setNewFileLifetime(Integer lt) { newFileLifetime=lt; } 


        @Option(
                name = "pin_lifetime",
                description =  "number of seconds to add to current time to extend pin lifetime of SURL(s)",
                required=false,
                log=true,
                save=true
                )
                private Integer newPinLifetime;

        public Integer getNewPinLifetime() { return newPinLifetime; } 

	public void setNewPinLifetime(Integer lt) { newPinLifetime=lt; } 

        @Option(
                name = "extendFileLifetime",
                description =  "exend file(s) lifetimes",
                defaultValue = "false",
                required=false,
                log=true
                )
                private boolean  extendFileLifetime;
        
        public boolean isExtendFileLifetime() { return extendFileLifetime; } 

        public void setExtendFileLifetime(boolean x) { extendFileLifetime=x; }
        
        @Option(
                name = "advisoryDelete",
                description =  "performs AdvisoryDelete",
                defaultValue = "false",
                required=false,
                log=true
                )
                private boolean  advisoryDelete;
        
        public boolean isAdvisoryDelete() { return advisoryDelete; } 

        public void setAdvisoryDelete(boolean x) { advisoryDelete=x; }

        @Option(
                name = "getRequestStatus",
                description =  "gets Request Status",
                defaultValue = "false",
                required=false,
                log=true
                )
                private boolean  getRequestStatus;
        
        public boolean isGetRequestStatus() { return getRequestStatus; } 

        public void setGetRequestStatus(boolean x) { getRequestStatus=x; }

	private String getRequestStatusSurl;

         @Option(
                name = "request_id",
                description =  "request token",
                defaultValue = "0",
                required=false,
                log=true
                )
                 private int getRequestStatusId;

        public int getGetRequestStatusId() {
		return getRequestStatusId;
	}

	public void setGetRequestStatusId(int getRequestStatusId) {
		this.getRequestStatusId = getRequestStatusId;
	}

        @Option(
                name = "getStorageElementInfo",
                description = "gets StorageElementInfo",
                defaultValue = "false",
                required=false,
                log=true
                )
                private boolean  getStorageElementInfo;

	public boolean isGetStorageElementInfo() {
		return getStorageElementInfo;
	}
    
	public void setGetStorageElementInfo(boolean getStorageElementInfo) {
		this.getStorageElementInfo = getStorageElementInfo;
	}
        
	private String storageElementInfoServerWSDL;

        @Option(
                name = "retry_timeout",
                description =  "number of miliseconds to sleep after a failure",
                defaultValue = "10000",
                unit="milliseconds",
                required=false,
                log=true,
                save=true
                )
                private long retry_timeout;
        
  	public long getRetry_timeout() {
		return retry_timeout;
	}
	public void setRetry_timeout(long retry_timeout) {
		this.retry_timeout = retry_timeout;
	}
      

        @Option(
                name = "retry_num",
                description =  "<number of retries before client gives up>",
                defaultValue = "20",
                required=false,
                log=true,
                save=true
                )
                private int retry_num;
        
	public int getRetry_num() {
		return retry_num;
	}
        
	public void setRetry_num(int retry_num) {
		this.retry_num = retry_num;
	}
    

        @Option(
                name = "connect_to_wsdl",
                description =  "connect to wsdl instead of  connecting to the server directly",
                defaultValue = "false",
                required=false,
                log=true,
                save=true
                )
                private boolean connect_to_wsdl;
        
      	public boolean isConnect_to_wsdl() {
		return connect_to_wsdl;
	}

	public void setConnect_to_wsdl(boolean connect_to_wsdl) {
		this.connect_to_wsdl = connect_to_wsdl;
	}
        
        @Option(
                name = "delegate",
                description =  "enables or disables the delegation of the user credenital to the server",
                defaultValue = "true",
                required=false,
                log=true,
                save=true
                )
                private boolean delegate;
        
        public boolean isDelegate() {
		return delegate;
	}
        
	public void setDelegate(boolean delegate) {
		this.delegate = delegate;
	}

        @Option(
                name = "full_delegation",
                description =  	"specifies type (full or limited) of delegation delegation",
                defaultValue = "true",
                required=false,
                log=true,
                save=true
                )
                private boolean full_delegation;

 	public boolean isFull_delegation() {
		return full_delegation;
	}
        
	public void setFull_delegation(boolean full_delegation) {
		this.full_delegation = full_delegation;
	}

        @Option(
                name = "version",
                description =  	"print SRM version information",
                defaultValue = "false",
                required=false,
                log=true
                )
                private boolean version;


        @Option(
                name = "report",
                description =  	"the path to the report file",
                required=false,
                log=true,
                save=true
                )
                private String report;

	public java.lang.String getReport() {
		return report;
	}

	public void setReport(java.lang.String report) {
		this.report = report;
	}
        
        @Option(
                name = "server_mode",
                description =  	"gridftp server mode for data transfer (\"passive\" or \"active\")",
                defaultValue = "passive",
                required=false,
                log=true,
                save=true
                )
                private String server_mode;

        public String getServerMode() { return server_mode; } 
        
        synchronized public void setServerMode(String x) { 
                server_mode=x;
                passive_server_mode=x.equalsIgnoreCase("passive");
        }
        
	private boolean passive_server_mode=true;

        public boolean isPassiveServerMode() {
		return passive_server_mode;
	}
        
	synchronized public void setPassiveServerMode(boolean mode) {
                if (mode) { 
                        server_mode="passive";
                        passive_server_mode = mode;
                }
                else { 
                        server_mode="active";
                        passive_server_mode = mode;
               }
	}
        
        
        @Option(
                name = "storagetype",
                description =  	"<permanent|volatile|durable> to specify kind of storage to use",
                defaultValue = "permanent",
                required=false,
                log=true,
                save=true
                )
                private String storagetype;
        
      	public String getStorageType() {
		if (storagetype == null){
			storagetype = "permanent";
		}
		return storagetype;
	}
        
	public void setStorageType(String storage_type) {
		this.storagetype = storage_type;
	}

        @Option(
                name = "stage",
                description =  	"performs srm \"get\", without actual copy of the files with the hope that this will trigger staging in srm managed",
                defaultValue = "false",
                required=false,
                log=true
                )
                private boolean stage;

        public boolean isStage() {
		return stage;
	}
 
        public void setStage(boolean stage) {
                this.stage = stage;
	}

        @Option(
                name = "l",
                description = "changes srm ls to long format mode, may result in dramatic increase in execution time",
                defaultValue = "false",
                required=false,
                log=true,
                save=true
                )
                private boolean longLsFormat;

        public boolean isLongLsFormat() {
		return longLsFormat;
	}
        
        public void  setLongLsFormat(boolean x) {
		longLsFormat=x;
	}
        
        @Option(
                name = "recursion_depth",
                description = "<integer> controls how deep to descend into direcotory trees",
                defaultValue = "1",
                required=false,
                log=false
                )
                private int recursionDepth;

        public int getRecursionDepth() {
		return recursionDepth;
	}

        public void setRecursionDepth(int x) {
		recursionDepth=x;
	}

        @Option(
                name = "recursive",
                description = "enables recursive empty directory deletion",
                defaultValue = "false",
                required=false,
                log=true
                )
                private boolean recursive;

	public boolean isRecursive() {
		return recursive;
	}

	public void setRecursive(boolean recursive) {
		this.recursive = recursive;
	}


        @Option(
                name = "offset",
                description = "<integer> offset to start number of elements to report",
                defaultValue = "0",
                required=false,
                log=true,
                save=true
                )
                private int lsOffset;

        public int getLsOffset() {
		return lsOffset;
	}
        
        public void setLsOffset(int x) { 
                lsOffset=x;
        }

        @Option(
                name = "count",
                description = "<integer> number of elements to report",
                defaultValue = "0",
                required=false,
                log=true,
                save=true
                )
                private int lsCount;

        public int getLsCount() {
		return lsCount;
	}
        
        public void setLsCount(int x) { 
                lsCount=x;
        }

        @Option(
                name = "srm_protocol_version",
                description = "<srm protocol version>",
                defaultValue = "0",
                required=false,
                log=true,
                save=true
                )
                private int srm_protocol_version;
        
	public int getSrmProtocolVersion() {
		return srm_protocol_version;
	}
        
	public void setSrmProtocolVersion(int srmProtocolVersion) {
		this.srm_protocol_version = srmProtocolVersion;
	}

        @Option(
                name = "1",
                description = "<srm protocol version>",
                defaultValue = "false",
                required=false,
                log=false
                )
                private boolean isSrmv1;

        @Option(
                name = "2",
                description = "<srm protocol version>",
                defaultValue = "false",
                required=false,
                log=false
                )
                private boolean isSrmv2;
        
        @Option(
                name = "request_lifetime",
                description = "<num of seconds> request lifetime",
                defaultValue = "86400",
                unit = "seconds",
                required=false,
                log=true,
                save=true
                )
                private long request_lifetime;

       	public long getRequestLifetime() {
		return request_lifetime;
	}
    
	public void setRequestLifetime(long requestLifetime) {
		this.request_lifetime = requestLifetime;
	}
        
        @Option(
                name = "priority",
                description = "specify request priority, 0 is lowest",
                defaultValue = "0",
                required=false,
                log=true,
                save=true
                )
                private Integer priority;


       	public Integer getRequestPriority() { 
		return priority;
	}
    
	public void setRequestPriority(int p) { 
		this.priority = p;
	}
        
	private Map<String,String> extraParameters;
        
        @Option(
                name = "overwrite_mode",
                description = "<ALWAYS|NEVER|WHEN_FILES_ARE_DIFFERENT>",
                required=false,
                log=true,
                save=true
                )
                private String overwriteMode;

	public String getOverwriteMode() {
		return overwriteMode;
	}

	public void setOverwriteMode(String overwriteMode) {
		this.overwriteMode = overwriteMode;
	}
        //
	// checksum options
        //
        @Option(
                name = "cksm_type",
                description = "<type|negotiate> calculate and verify server and client checksum values using this type (adler32|MD4|MD5| .... ). If checksum value has been omitted, missing value will be computed over the local file. If negotiate is set - client will attempt to negotiate cksm type for the file checksum value avilable at the server. For gridftp transfers to/from servers that support checksumming features",
                required=false,
                log=true,
                save=true
                )
                private String cksm_type;

        
	public String getCksmType(){ 
		return this.cksm_type;
	}

	public void setCksmType(String type){
		this.cksm_type = type;
	}

        @Option(
                name = "cksm_value",
                description = "<checksum HEX value> override dynamic calucation of the local checksum with this value. If cksm_type option has not been set, adler32 will be assumed. For gridftp transfers to/from servers that support checksumming features",
                required=false,
                log=true
                )
                private String cksm_value;
        
        
	public String getCksmValue(){ 
		return this.cksm_value;
	}

	public void setCksmValue(String value){
		this.cksm_value = value;
	}
        
	private String arrayOfRequestTokens[] = null;

        @Option(
                name = "abortRequest",
                description =  "to abort request",
                defaultValue = "false",
                required=false,
                log=true
                )
                private boolean is_AbortRequest;

	public boolean isAbortRequest() {
		return is_AbortRequest;
	}

	public void setAbortRequest(boolean yes) { 
		this.is_AbortRequest = yes;
	}

	private String srmUrl;
	private String surls[];

	/** Creates a new instance of Configuration */
	public Configuration() {
		extraParameters = new HashMap<String,String>();
	}
    
	private String general_options =
		" General Options :\n"+
		"\t-version enables printing version number\n"+
		"\t-debug=<true or false> true enables debug output, default is false \n"+
		"\t-srmcphome=<path to srmcp product dir>\n"+
		"\t-gsissl=<true or false> true uses gsi https, default is false\n"+
		"\t-mapfile=<mapfile> to specify glue mapfile\n"+
		"\t-wsdl_url=<full url to web service wsdl> this options overrides\n"+
		"\t-webservice_path and -webservice_protocol options\n"+
		"\t-webservice_path=<webservice_path> to specify web service path component\n"+
		"\t\t of web service URL (for example \"srm/managerv1.wsdl\")\n"+
		"\t-webservice_protocol=<webservice_protocol> to specify the\n"+
		"\t\t webservice protocol ( for example \"http\",\"https\" or \"httpg\")\n"+
		"\t-use_proxy=<true or false> true if srmcp should use grid proxy\n"+
		"\t\t false if it should use certificate and key directly,\n"+
		"\t\t defult value is true\n"+
		"\t-x509_user_proxy=<path to user grid proxy>\n"+
		"\t-x509_user_cert=<path to user grid certificate>\n"+
		"\t-x509_user_key=<path to user private key>\n"+
		"\t-x509_user_trusted_certificates=<path to the directory with cerificates\n"+
		"\t\t of trusted Certificate Authorities (CAs)>\n"+
		"\t-globus_tcp_port_range=<min value>:<max value>,\n"+"" +
		"\t\t a range of ports open for tcp connections specified as\n"+
		"\t\t a pair of positive integers separated by \":\",\n" +
		"\t\t not set by default \n"+
		"\t-gss_expected_name=<gss expected name in the srm server DN> default value is host\n"+
		"\t-srm_protocol_version=<srm protocol version>\n"+
		"\t\t or just specify -1  or -2 \n"+
		"\t-conf=<path to the configuration file> default value is config.xml\n"+
		"\t-save_conf=<path to the file in which the new configuration will be saved>\n"+
		"\t\t no transfer will be performed if this option is specified\n"+
		"\t-retry_timeout=<number of miliseconds to sleep after a failure\n"+
		"\t\t before the client tries to retry>\n"+
		"\t-retry_num=<number of retries before client gives up>\n"+
		"\t-connect_to_wsdl=<true or false, false by default> \n"+
		"\t\t srm client now connects directly to the service without reading\n" +
		"\t\t the wsdl first but for the compatibility with the old implementation,\n"+
		"\t\t especially if srm urls available point to the location of the wsdl,\n"+
		"\t\t we make the old way of connecting to the wsdl location first available\n"+
		"\t-delegate=<true or false, true by default> \n"+
		"\t\t enables or disables the delegation of the user credenital to the server\n"+
		"\t-full_delegation=<true or false, true by default> \n"+
		"\t\t if delegation is enabled, it specifies the type of delegation\n"+
		"\t\t if this option is set to true the delegation is full, otherwise limited\n"+
		"\t-h or -help for this help \n";
    
	private String storage_info_options =
		" Storage Info options :\n"+
		"Example:\n" +
		"      srm-storage-element-info  https://fndca.fnal.gov:8443/srm/infoProvider1_0.wsdl\n";
    
	private String reserveSpace_options =
		"reserve space options: \n"+
		"\t-space_desc=<Description> a description of space reservation, can be used for future reference\n"+
		"\t-retention_policy=<REPLICA|CUSTODIAL|OUTPUT>\n"+
		"\t-access_latency=<ONLINE|NEARLINE>\n"+
		"\t-access_pattern=<TRANSFER_MODE|PROCESSING_MODE>\n"+
		"\t-array_of_client_networks=netowrk1[,network2[...]]\n"+
		"\t-protocols=protocol1[,protocol2[...]] \n"+
		"\t-connection_type=<WAN|LAN>\n"+
		"\t-desired_size in Bytes \n"+
		"\t-guaranteed_size in Bytes \n"+
		"\t-lifetime=<num of seconds> desired lifetime in seconds, -1 for infinite lifetime \n";

	private String getSpaceTokensOptions =
		" get-space-tokens-options :\n"+
		"\t-space_desc=<Description> a description of space reservation\n";
		
    
	private String releaseSpace_options =
		" release options: \n"+
		"\t-space_token=<Space Reservation Token to be released>\n"+
		"\t-force=<true|false>\n";
    
	private String getSpaceMetaData_options=
		" srm-get-space-metadata options:\n"+
		"\t-space_tokens=<id>,<id1>,<id2>... (Space Reservation Token(s))\n";
    
	private String getRequestSummary_options=
		" srm-get-request-summary options:\n"+
		"\t-request_tokens=<id>,<id1>,<id2>... (Request Token(s))\n";

	private String abortRequest_options=
		" srm-abort-request options: \n"+
		"\t-request_tokens=<id>,<id1>,<id2>... (Request Token(s))\n";

	private String abortFiles_options=
		" srm-abort-files options: \n"+
		"\t-request_tokens=<id>,<id1>,<id2>... (Request Token(s))\n";

	private String releaseFiles_options=
		" srm-release-files options: \n"+
		"\t-request_tokens=<id>,<id1>,<id2>... (Request Token(s))\n"+
		"\t-do_remove=<true|false>\n";

	private String getRequestTokens_options=
		" srm-get-request-tokens options:\n"+
		"\t-request_desc=<Description> (Request Token Description)\n";
    
    
	private String copy_options =
		" copy options :\n"+
		"\t-urlopy=<urlcopy path> to specify the path to  universal url_copy script\n"+
		"\t\t see $SRM_PATH/bin/url-copy.sh for example\n"+
		"\t-buffer_size=<integer> to set the buffer size to a value \n"+
		"\t\t different then default(2048)\n"+
		"\t-tcp_buffer_size=<integer> to set the tcp buffer size to a value \n"+
		"\t\t if option is not specified or set to 0,\n"+
		"\t\t then the default tcp buffer size is used\n"+
		"\t-streams_num=<integer> to set the number of streams used by gridftp \n"+
		"\t\t if number of stream is set to 1, then stream mode is used, otherwise\"+" +
		"\t\t extended block mode is used\n"+
		"\t-server_mode=<active or passive> to set (gridftp) server mode for data transfer, passive by default\n"+
		"\t\t this option will have affect only if transfer is performed in a stream mode (see -streams_num)\n"+
		"\t-storagetype=<permanent|volatile|durable> to specify kind of storage to use,\"permanent\" by default\n"+
		"\t-protocols=protocol1[,protocol2[...]] \n"+
		"\t\t the comma separated list of supported TURL protocols\n"+
		"\t-space_token=<Space Reservation Token> identifying space to put file in\n"+
		"\t-retention_policy=<REPLICA|CUSTODIAL|OUTPUT>\n"+
		"\t-access_latency=<ONLINE|NEARLINE>\n"+
		"\t-overwrite_mode=<ALWAYS|NEVER|WHEN_FILES_ARE_DIFFERENT>\n"+
		"\t\tserver default overwrite policy is used if this is not specified\n"+
		"\t-pushmode=<true or false>  true to use the push mode in case\n"+
		"\t\t of srm Mass Storage Systems (MSS) to MSS copy, \n"+
		"\t\t false to use the pull mode, the default mode is pull mode (false)\n"+
		"\t-srmstage=<true or false, false by default> \n"+
		"\t\t if set to true - the source files are staged only onto disk cache\n"+
		"\t\t and not transferred to client right away> \n"+
		"\t\t if set to false - the source files are transferred to the client\n"+
		"\t-use_urlcopy_script=<true or false> use java native copiers of use urcopy script\n"+
		"\t-srm_protocol_version=<1 or 2> 1 for srm 1.1 or 2 for srm 2.2, no other protocols are supported\n"+
		"\t\t or just specify -1  or -2 \n"+
		"\t-priority=<int> specify job priority \n"+
		"\t-request_lifetime=<num of seconds> request lifetime in seconds\n"+
		"\t-cksm_type=<type|negotiate> calculate and verify server and client checksum values using this type (adler32|MD4|MD5| .... ).\n\tIf checksum value has been omitted, missing value will be computed over the local file.\n\t If negotiate is set - client will attempt to negotiate cksm type for the file checksum value avilable at the server.\n\tFor gridftp transfers to/from servers that support checksumming features\n"+
		"\t-cksm_value=<checksum HEX value> override dynamic calucation of the local checksum with this value.\n\tIf cksm_type option has not been set, adler32 will be assumed.\n\tFor gridftp transfers to/from servers that support checksumming features\n"+
		"\t-copyjobfile=<file> where <file> is the path to the text file containing \n"+
		"\t\t the list of sources and destination\n"+
		"\t\t each line has a format : <sorce-url> <destination-url>\n"+
		"\t-report=<report_file> where <report_file> is the path to the report file\n"+
		"\t\t if specified, it will contain the resutls of the execution srmcp \n"+
		"\t\t the each line in the file will have the following format:\n"+
		"\t\t<src url> <dst url> <return code> [<error>]\n"+
		"the following return codes are supported:\n"+
		"\t\t 0 - success\n"+
		"\t\t 1 - general error\n"+
		"\t\t 2 - file exists, can not overwrite\n"+
		"\t\t 3 - user permission error\n" +
		"Example of srm put:\n" +
		"\t\t srmcp file:////bin/sh srm://myhost.mydomain.edu:8443//dir1/dir2/sh-copy\n"+
		"Example of srm get:\n" +
		"\t\t srmcp srm://myhost.mydomain.edu:8443//dir1/dir2/sh-copy file:///localdir/sh\n"+
		"Example of srm copy (srm to srm):\n" +
		"\t\t srmcp srm://myhost.mydomain.edu:8443//dir1/dir2/sh-copy srm://anotherhost.org:8443/newdir/sh-copy\n"+
		"Example of srm copy (gsiftp to srm):\n" +
		"\t\t srmcp gsiftp://ftphost.org//path/file srm://myhost.mydomain.edu:8443//dir1/dir2/file\n";
    
	private String bring_online_options =
		" bring online options :\n"+
		"\t-storagetype=<permanent|volatile|durable> to specify kind of storage to use,\"permanent\" by default\n"+
		"\t-protocols=protocol1[,protocol2[...]] \n"+
		"\t\t the comma separated list of supported TURL protocols\n"+
		"\t-space_token=<Space Reservation Token> identifying space to put file in\n"+
		"\t-retention_policy=<REPLICA|CUSTODIAL|OUTPUT>\n"+
		"\t-access_latency=<ONLINE|NEARLINE>\n"+
		"\t-srm_protocol_version=<1 or 2> 1 for srm 1.1 or 2 for srm 2.2, no other protocols are supported\n"+
		"\t\t or just specify -1  or -2 \n"+
		"\t-request_lifetime=<num of seconds> request lifetime in seconds\n"+
		"\t-lifetime=<num of seconds> desired lifetime of online state in seconds, -1 for infinite \n"+
		"\t-priority=<int> specify job priority \n"+
		"\t-report=<report_file> where <report_file> is the path to the report file\n"+
		"\t\t if specified, it will contain the resutls of the execution srm-bring-online \n";
    
    
	private String move_options =
		" mv options :\n"+
		"\t-copyjobfile=<file> where <file> is the path to the text file containing \n"+
		"\t\t the list of sources and destination\n"+
		"\t\t each line has a format : <sorce-url> <destination-url>\n"+
		"New directory can include path, as long as all sub directories exist.\n"+
		"Moves within single storage system are allowed (you can't mv from one SRM to another SRM \n"+
		" (or from/to remote/local filesystem, use copy and delete)).\n"+
		"Examples: \n"+
		"\t\t srm -mv srm://fledgling06.fnal.gov:8443/srm/managerv2?SFN=/from_path/ \n"+
		" srm://fledgling06.fnal.gov:8443/srm/managerv2?SFN=/to_path/ \n";

	private String rmdir_options =
		" rmdir options :\n"+
		"\t-recursive[=<boolean>] recursive empty directory deletion.\n"+
		"\t\t -rmdir is defined in SRM specification as :\n"+
		"\t\t \"applies to dir doRecursiveRemove is false by edefault. To distinguish from \n"+
		"\t\t srmRm(), this function is for directories only. \"\n"+
		"\t\t so it is unclear id the directories must be empty. \n"+
		"\t\t We interpret \"rmdir\" as Unix \"rmdir\" which allows to remove only empty directories \n"+
		"\t\t extending it to have an ability to remove trees of empty directories. \n"+
		"\t\t Removal of multiple directories is not supported \n"+
		"\t\t Removal of files is not supported (use -rm).\n"+
		"Examples: \n"+
		"\t\t srmrmdir srm://fledgling06.fnal.gov:8443/srm/managerv2?SFN=/dir \n"+
		"\t\t srm -rmdir  srm://fledgling06.fnal.gov:8443/srm/managerv2?SFN=/dir \n"+
		"\t\t srm -rmdir -recursive=true srm://fledgling06.fnal.gov:8443/srm/managerv2?SFN=/dir \n";
    
	private String mkdir_options =
		" mkdir options :\n"+
		"No options are defined for \"mkdir \". New directory  can include path,\n"+
		"as long as all sub directories exist.\n"+
		"Examples: \n"+
		"\t\t srm -mkmdir srm://fledgling06.fnal.gov:8443/srm/managerv2?SFN=/dir/path/ \n";
    
	private String rm_options =
		" rm options :\n"+
		"\t\t no additional options are suported for srmrm. \n"+
		"\t\t Applies to files only.\n"+
		"Examples: \n"+
		"\t\t srmrm srm://fledgling06.fnal.gov:8443/srm/managerv2?SFN=/dir/file  \n"+
		"\t\t srm -rm  srm://fledling06.fnal.gov:8443/srm/managerv2?SFN=/dir/file  \n";
    
	private String ls_options =
		" ls options :\n"+
		"\t-l changes srm ls to long format mode, may result in dramatic increase in execution time\n"+
		"\t-recursion_depth=<integer> controls how deep to descend into direcotory trees\n"+
		"\t-count=<integer> number of elements to report \n"+
		"\t-offset=<integer> offset to start number of elements to report \n"+
		"\t\t 0 means do not descend at all, equivalent to unix ls -d option, 0 is the default value\n";
    
	private String stage_options =
		" stage options:\n";
    
	private String getPermission_options =
		" getpermission options:\n";

	private String checkPermission_options =
		" checkpermission options:\n";

	private String setPermission_options =
		" setPermission options :\n"+
		"\t-type=<ADD|REMOVE|CHANGE> \n"+
		"\t-owner=<NONE,X,W,WR,R,RX,RW,RWX> \n"+
		"\t-other=<NONE,X,W,WR,R,RX,RW,RWX> \n"+
		"\t-group=<NONE,X,W,WR,R,RX,RW,RWX> \n";

	private String extendFileLifetime_options =
		" extendFileLifetime options :\n"+
		"\t-request_token=<string> \n"+
		"\t-file_lifetime=<int> \n"+
		"\t-pin_lifetime=<int> \n";
	
	public final String usage() {
		if (getStorageElementInfo) {
			return
				"Usage: get-storage-element-info [command line options] endpoint-wsdl-url\n"+
				" default options will be read from configuration file \n"+
				" but can be overridden by the command line options\n"+
				" the command line options are one or more of the following:\n"+
				(isHelp()==true?general_options+storage_info_options:storage_info_options);
		}
		if (getFileMetaData) {
			return
				"Usage:get-file-metadata [command line options]  srmurl\n"+
				" default options will be read from configuration file \n"+
				" but can be overridden by the command line options\n"+
				" the command line options are one or more of the following:\n"+
				general_options;
		}
		if(getSpaceTokens) {
			return
				"Usage:get-space-tokens [command line options]  srmurl\n"+
				" default options will be read from configuration file \n"+
				" but can be overridden by the command line options\n"+
				" the command line options are one or more of the following:\n"+
				(isHelp()==true?general_options+getSpaceTokensOptions:getSpaceTokensOptions);
		}
		if (advisoryDelete) {
			return
				"Usage:advisory-delete [command line options]  srmurl(s)\n"+
				" default options will be read from configuration file \n"+
				" but can be overridden by the command line options\n"+
				" the command line options are one or more of the following:\n"+
				general_options;
		}
		if (getRequestStatus) {
			return
				"Usage:get-request-status  [command line options]  srmurl requestId\n"+
				" where srmurl is one of the surl specified in the original request\n" +
				" and is used for determening the srm endpoint location\n"+
				" default options will be read from configuration file \n"+
				" but can be overridden by the command line options\n"+
				" the command line options are one or more of the following:\n"+
				general_options;
		}
		if (copy) {
			return
				"Usage: srmcp [command line options] source(s) destination\n"+
				" or  : srmcp [command line options] -copyjobfile  <file>\n"+
				"       either source(s) or destination or both should be (an) srm url\n"+
				"       default options will be read from configuration file \n"+
				"       but can be overridden by the command line options\n"+
				"       the command line options are one or more of the following:\n"+
				(isHelp()==true?general_options+copy_options:copy_options);
		}
		if (bringOnline) {
			return
				"Usage: srm-bring-online [command line options] srmUrl(s)\n"+
				"       default options will be read from configuration file \n"+
				"       but can be overridden by the command line options\n"+
				"       the command line options are one or more of the following:\n"+
				(isHelp()==true?general_options+bring_online_options:bring_online_options);
		}
        
		if (reserveSpace) {
			return
				"Usage: srm-reserve-space [command line options]  srmUrl\n"+
				"       default options are read from configuration file\n"+
				"       but can be overridden by the command line options\n"+
				"       the command line options are one or more of the following:\n"+
				(isHelp()==true?general_options+reserveSpace_options:reserveSpace_options);
		}
		if(getSpaceMetaData) {
			return
				"Usage: srm-get-space-metadata [commnad line options] srmUrl "+
				" default options will be read from configuration file \n"+
				" but can be overridden by the command line options\n"+
				" the command line options are one or more of the following:\n"+
				(isHelp()==true?general_options+getSpaceMetaData_options:getSpaceMetaData_options);
            
            
		}
		if (releaseSpace) {
			return
				"Usage: srm-release-space [command line options]  srmUrl\n"+
				" default options will be read from configuration file \n"+
				" but can be overridden by the command line options\n"+
				" the command line options are one or more of the following:\n"+
				(isHelp()==true?general_options+releaseSpace_options:releaseSpace_options);
		}
		if (ls) {
			return
				"Usage: srmls [command line options] srmUrl [[srmUrl]...]\n" +
				" default options will be read from configuration file \n"+
				" but can be overridden by the command line options\n"+
				" the command line options are one or more of the following:\n"+
				(isHelp()==true?general_options+ls_options:ls_options);
		}
		if (is_rm) {
			return
				"Usage: srmrm [command line options] srmUrl [[srmUrl]...]\n" +
				" default options will be read from configuration file \n"+
				" but can be overridden by the command line options\n"+
				" the command line options are one or more of the following:\n"+
				(isHelp()==true?general_options+rm_options:rm_options);
		}
		if (is_mv) {
			return
				"Usage: srmmv [command line options] sourceSrmUrl destinationSrmUrl \n"+
				" default options will be read from configuration file \n"+
				" but can be overridden by the command line options\n"+
				" the command line options are one or more of the following:\n"+
				(isHelp()==true?general_options+move_options:move_options);
		}
		if (is_getRequestSummary) {
			return
				"Usage: srm-get-request-summary [command line options] srmUrl \n"+
				" default options will be read from configuration file \n"+
				" but can be overridden by the command line options\n"+
				" the command line options are one or more of the following:\n"+
				(isHelp()==true?general_options+getRequestSummary_options:getRequestSummary_options);
		}
		if (is_AbortRequest) {
			return
				"Usage: srm-abort-request [command line options] srmUrl \n"+
				" default options will be read from configuration file \n"+
				" but can be overridden by the command line options\n"+
				" the command line options are one or more of the following:\n"+
				(isHelp()==true?general_options+abortRequest_options:abortRequest_options);
		}
		if (is_AbortFiles) {
			return
				"Usage: srm-abort-files [command line options] srmUrl [[srmUrl]...] \n"+
				" default options will be read from configuration file \n"+
				" but can be overridden by the command line options\n"+
				" the command line options are one or more of the following:\n"+
				(isHelp()==true?general_options+abortFiles_options:abortFiles_options);
		}
		if (is_ReleaseFiles) {
			return
				"Usage: srm-relese-files [command line options] srmUrl [[srmUrl]...] \n"+
				" default options will be read from configuration file \n"+
				" but can be overridden by the command line options\n"+
				" the command line options are one or more of the following:\n"+
				(isHelp()==true?general_options+releaseFiles_options:releaseFiles_options);
		}
		if (is_getRequestTokens) {
			return
				"Usage: srm-get-request-tokens [command line options] srmUrl  \n"+
				" default options will be read from configuration file \n"+
				" but can be overridden by the command line options\n"+
				" the command line options are one or more of the following:\n"+
				(isHelp()==true?general_options+getRequestTokens_options:getRequestTokens_options);
		}
		if (is_rmdir) {
			return
				"Usage: srmrmdir [command line options] srmUrl \n" +
				" default options will be read from configuration file \n"+
				" but can be overridden by the command line options\n"+
				" the command line options are one or more of the following:\n"+
				(isHelp()==true?general_options+rmdir_options:rmdir_options);
		}
		if (is_mkdir) {
			return
				"Usage: srmmkdir [command line options] srmUrl \n" +
				" default options will be read from configuration file \n"+
				" but can be overridden by the command line options\n"+
				" the command line options are one or more of the following:\n"+
				(isHelp()==true?general_options+mkdir_options:mkdir_options);
		}
		if(stage) {
			return
				"Usage: srmstage [command line options] srmUrl [[srmUrl]...] \n" +
				" default options will be read from configuration file \n"+
				" but can be overridden by the command line options\n"+
				" the command line options are one or more of the following:\n"+
				(isHelp()==true?general_options+stage_options:stage_options);
            
		}
		if(getPermission) {
			return
				"Usage: srmgetpermission [command line options] srmUrl [[srmUrl]...] \n" +
				" default options will be read from configuration file \n"+
				" but can be overridden by the command line options\n"+
				" the command line options are one or more of the following:\n"+
				(isHelp()==true?general_options+getPermission_options:getPermission_options);
		}
		if(checkPermission) {
			return
				"Usage: srmcheckpermission [command line options] srmUrl [[srmUrl]...] \n" +
				" default options will be read from configuration file \n"+
				" but can be overridden by the command line options\n"+
				" the command line options are one or more of the following:\n"+
				(isHelp()==true?general_options+checkPermission_options:checkPermission_options);
		}
		if(setPermission) {
			return
				"Usage: srmsetpermission [command line options] srmUrl [[srmUrl]...] \n" +
				" default options will be read from configuration file \n"+
				" but can be overridden by the command line options\n"+
				" the command line options are one or more of the following:\n"+
				(isHelp()==true?general_options+setPermission_options:setPermission_options);
		}
		if(extendFileLifetime) { 
			return
				"Usage: srm-extend-file-lifetime [command line options] srmUrl [[srmUrl]...] \n" +
				" default options will be read from configuration file \n"+
				" but can be overridden by the command line options\n"+
				" the command line options are one or more of the following:\n"+
				(isHelp()==true?general_options+extendFileLifetime_options:extendFileLifetime_options);
		}
		if(ping) {
			return
				"Usage: srmping  [command line options] srmUrl \n" +
				" default options will be read from configuration file \n"+
				" but can be overridden by the command line options\n"+
				" the command line options are one or more of the following:\n"+
				general_options;
		}
		return
			"Usage: srm [command option] [command line options] arguments\n"+
			"where command option is one of the following :\n"+
			" -copy                   performs srm \"get\", \"put\", or \"copy\" depending \n"+
			"                         on arguments \n"+
			" -bringOnline            performs srmBringOnline \n"+
			" -stage                  performs srm \"get\", without actual copy of the files \n"+
			"                         with the hope that this will trigger staging in srm managed\n"+
			"                         hierarchical storage system\n"+
			" -mv                     performs srm \"mv\" of files and directories \n"+
			" -ls                     list content of directory \n"+
			" -rm                     remove file(s) \n"+
			" -mkdir                  create directory \n"+
			" -rmdir                  remove empty directory tree \n"+
			" -getPermissions	      get file(s) permissions \n"+
			" -checkPermissions	      check file permissions \n"+
			" -setPermissions	      set file permissions \n"+
			" -extendFileLietime      extend lifetime of existing SURL(s) pf volatile and durable files \n"+
			"                         or pinned lifetime of TURL(s)\n"+
			" -advisoryDelete         performs srm advisory delete \n"+
			" -abortRequest           to abort request.  \n"+
			" -abortFiles             to abort files.  \n"+
			" -releaseFiles           to unpin files.  \n"+
			" -getRequestStatus       obtains and prints srm request status \n"+
			" -getRequestSummary      is to retrieve a summary of the previously submitted request.  \n"+
			" -getGetRequestTokens    retrieves request tokens for the clients requests, given client provided request description.\n"+
			"                         This is to accommodate lost request tokens. This can also be used for getting all request tokens.\n"+
			" -getFileMetaData        gets srm meta data for specified surls\n"+
			" -getSpaceTokens         gets space tokens belonging to this user\n"+
			"                         which have a corresponding description (if provided)\n"+
			" -getStorageElementInfo  prints storage element info, in particular\n"+
			"                         it prints total, availabale and used space amount\n"+
			" -reserveSpace           performs explicit space reservation \n"+
			" -releaseSpace           performs release of explicit  space reservation \n"+
			" -getSpaceMetaData       retrieves and prints metadata for given space tokens\n"+
			" -ping                   performs srm ping command (useful for diagnostics and version info) \n"+
			" -h,-help                prints this help\n"+
			" type srm [command option] -h for more info about a particular option";
	}
    
	public void parseArguments(String args[]) throws Exception {
                Args _args = new Args(args);
                //
                // Set all fields to default values 
                //
                OptionParser.setDefaults(this);
                //
                // Need to parse "conf" option first, so we set the value
                // of config_file field.
                // 
                OptionParser.parseOption(this,"conf",_args);
		File f = new File(config_file);
		if (f.exists() && f.canRead()) {
			read(config_file);
		}
                //
                // Now parse only specified options, so we achieve a situation where
                // class fields are set to defaults, then possibly overriden by 
                // values from config file and then possibly overriden by command  
                // line options.
                //
                OptionParser.parseSpecifiedOptions(this,_args);
                extraParameters.put("priority",priority.toString());
                if (is_help) { 
                        help=true;
                }
                if (gsissl) 
			this.webservice_protocol ="https";
                else 
			this.webservice_protocol ="http";

                if (retry_timeout <= 0) {
                        throw new IllegalArgumentException("illegal retry timeout : "+
                                                           retry_timeout);
                }
                if(retry_num < 0) {
                        throw new IllegalArgumentException("illegal number of retries : "+
                                                           retry_num);
                }
                boolean versionIsSpecified = isSrmv1|isSrmv2;
                if ((versionIsSpecified && srm_protocol_version!=0) || (isSrmv1&&isSrmv2)) { 
			throw new IllegalArgumentException(
				"only one option of -srm_protocol_version, -1 or -2 should be specified");
                }
                if (isSrmv1) { 
                        srm_protocol_version=1;
                }
                else if (isSrmv2) {
                        srm_protocol_version=2; 
                }
                else if (srm_protocol_version==0) { 
                        srm_protocol_version=1;
                }
                if(srm_protocol_version != 1 && srm_protocol_version != 2) {
                        throw new IllegalArgumentException("illegal srm protocol version "+ srm_protocol_version);
                }
                if(request_lifetime <= 0 && request_lifetime != -1) {
                        throw new IllegalArgumentException("illegal value for request lifetime"+
                                                           request_lifetime);
                }
		if (version || debug) {
			System.err.println(new Version().toString());
		}
		if( isHelp()) {
			return;
		}
		if(version) { 
			System.exit(0);
		}

		if (!( is_mv ^
		       is_mkdir ^
		       is_rmdir ^
		       is_rm ^
		       ls ^
		       copy ^
		       bringOnline ^
		       ping ^
		       getFileMetaData ^
		       advisoryDelete ^
		       getRequestStatus ^
		       getStorageElementInfo ^
		       stage ^
		       getPermission ^
		       checkPermission ^
		       setPermission ^
		       extendFileLifetime ^
		       releaseSpace ^
		       reserveSpace ^
		       getSpaceMetaData ^
		       getSpaceTokens ^
		       is_getRequestSummary ^
		       is_getRequestTokens ^
		       is_AbortRequest ^
		       is_AbortFiles ^
		       is_ReleaseFiles
			    )) {
			throw new IllegalArgumentException(
				"one and only one of the following options must be " +
				"specified:\n\n" + usage());
		}
 
                int numberOfArguments = _args.argc();
                
		if (numberOfArguments==0 && copyjobfile==null) { 
			usage();
			throw new IllegalArgumentException("Please specify command line arguments\n"+usage());
			 
		}
		if (numberOfArguments==0 && copyjobfile==null) { 
			usage();
			throw new IllegalArgumentException("Please specify command line arguments\n"+usage());
		}

		if (getFileMetaData ||advisoryDelete  ) {
                        surls = new String[numberOfArguments];
                        for(int i=0;i<numberOfArguments;i++){surls[i]=_args.argv(i);}
		}
		else if (getRequestStatus) {
			if (numberOfArguments == 1) {
                                getRequestStatusSurl = _args.argv(0);
			} 
			else {
				if (version) {
					System.exit(1);
				}
				throw new IllegalArgumentException(
					"one and only one storage element info server " +
					"wsdl url should be specified");
			}
		} 
		else if (is_getRequestTokens) {
			getRequestStatusSurl =  _args.argv(0); 
		}
		else if (is_getRequestSummary) {
			getRequestStatusSurl =  _args.argv(0); 
			arrayOfRequestTokens=readListOfOptions(requestTokens,",");
		}
		else if (is_AbortRequest) {
			srmUrl = _args.argv(0); 
			arrayOfRequestTokens=readListOfOptions(requestTokens,",");
		}
		else if (is_AbortFiles || is_ReleaseFiles ) {
			srmUrl= _args.argv(0); 
			if(numberOfArguments>1) { 
				surls = new String[numberOfArguments];
                                for(int i=0;i<numberOfArguments;i++){surls[i]=_args.argv(i);}
			}
			else { 
				srmUrl = _args.argv(0);
			}
                        arrayOfRequestTokens=readListOfOptions(requestTokens,",");
			if(arrayOfRequestTokens!=null&&surls!=null) { 
				throw new IllegalArgumentException(
					"specify request token or list of surls (exclusively)");
			}
		}
		else if (getStorageElementInfo) {
			if (numberOfArguments == 1) { 
				storageElementInfoServerWSDL = _args.argv(0); 
			} 
			else {
				if (version) {
					System.exit(1);
				}
				throw new IllegalArgumentException(
					"one and only one storage element info server " +
					"wsdl url should be  specified");
			}
		} 
		else if (reserveSpace) {
			srm_protocol_version =2;
                        protocols = readListOfOptions(protocols_list,",");
                        arrayOfClientNetworks = readListOfOptions(array_of_client_networks,",");
                        surls = new String[numberOfArguments];
                        for(int i=0;i<numberOfArguments;i++){surls[i]=_args.argv(i);}
		} 
		else if (releaseSpace) {
			srm_protocol_version =2;
                        surls = new String[numberOfArguments];
                        for(int i=0;i<numberOfArguments;i++){surls[i]=_args.argv(i);}
		} 
		else if (getSpaceMetaData) {
			srm_protocol_version =2;
			spaceTokensList=readListOfOptions(space_tokens_list,",");
			srmUrl = _args.argv(0); 
		} 
		else if(getSpaceTokens) {
			srm_protocol_version =2;
                        srmUrl = _args.argv(0);
		} 
		else if (copy||is_mv) {
			if (copy) {
				readCopyOptions();
			} 
			else if (is_mv) {
				srm_protocol_version =2;
			}
			if (copyjobfile == null) {
				if (numberOfArguments >= 2) { 
					if ( is_mv && numberOfArguments > 2 ) {
						throw new IllegalArgumentException(
							"one source and one destination " +
							"should be specified");
					}
					int number_of_sources = numberOfArguments - 1;
					from = new String[number_of_sources];
                                        for(int i=0;i<number_of_sources;i++){from[i]=_args.argv(i);}
                                        to  = _args.argv(number_of_sources);
                                }
				else {
					if (version) {
						System.exit(1);
					}
					if (is_mv) {
						throw new IllegalArgumentException(
							"one source and one destination " +
							"should be specified");
					} 
					else if (copy) {
						throw new IllegalArgumentException(
							"at least one source and one destination " +
							"should be specified");
					}
				}
			} 
			else {
				if (numberOfArguments > 0 ) {
					if(version) {
						System.exit(1);
					}
					throw new IllegalArgumentException(
						"no source or destination should be specified when " +
						"using copyjobfile");
				}
			}
		} 
		else if(bringOnline){
			srm_protocol_version =2;
                        protocols = readListOfOptions(protocols_list,",");
                        surls = new String[numberOfArguments];
                        for(int i=0;i<numberOfArguments;i++){surls[i]=_args.argv(i);}
		} 
		else if(ping) {
			srmUrl = _args.argv(0);
		} 
		else if(ping || ls || is_rm || is_rmdir || is_mkdir || stage || getPermission || checkPermission || extendFileLifetime ) { 
			srm_protocol_version =2;
                        surls = new String[numberOfArguments];
                        for(int i=0;i<numberOfArguments;i++){surls[i]=_args.argv(i);}
		} 
		else if (setPermission) {
			srm_protocol_version =2;
			setPermissionSurl = _args.argv(0);
		} 
		else {
			if (version) {
				System.exit(1);
			}
			throw new IllegalArgumentException("should not be here");
		}
	}
    
	private void readCopyOptions() throws Exception {
                protocols = readListOfOptions(protocols_list,",");
                if (spaceToken!=null) { 
                        srm_protocol_version = 2; 
                }
		readCksmOptions();
	}

	private String[] readListOfOptions(String option, 
					   String separator) throws Exception { 
		String[] listOfOptions = null;
                if (option != null) {
                        listOfOptions=option.split(separator);
		}
		return listOfOptions;
	}

	private void readCksmOptions()  throws Exception {
		if ( this.cksm_type == null && this.cksm_value != null )
			this.cksm_type = "adler32";
	}
    
	public void read(String file) throws Exception {
		DocumentBuilderFactory factory =
			DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = factory.newDocumentBuilder();
		Document document       = builder.parse(file);
		Node root               = document.getFirstChild();
		for (;root != null && !"srm-configuration".equals(root.getNodeName());
		     root = document.getNextSibling()) {
		}
		if (root == null) {
			System.err.println(" error, root element \"srm-configuration\" is not found");
			throw new IOException();
		}
		if (root != null && root.getNodeName().equals("srm-configuration")) {
			Node node = root.getFirstChild();
			for (;node != null; node = node.getNextSibling()) {
				if(node.getNodeType()!= Node.ELEMENT_NODE) {
					continue;
				}
				Node child = node.getFirstChild();
				for (;child != null; child = node.getNextSibling()) {
					if (child.getNodeType() == Node.TEXT_NODE) {
						break;
					}
				}
				if (child == null) {
					continue;
				}
				Text t            = (Text)child;
				String node_name  = node.getNodeName();
				String text_value = t.getData().trim();
                                for (Class c = getClass(); c != null; c = c.getSuperclass()) {
                                        for (Field field : c.getDeclaredFields()) {
                                                Option option = field.getAnnotation(Option.class);
                                                try {
                                                        if (option != null&&option.name().equals(node_name)&&option.save()) {
                                                                field.setAccessible(true);
                                                                Object value;
                                                                try {
                                                                        value = OptionParser.toType(text_value,
                                                                                                    field.getType());
                                                                        field.set(this, value);
                                                                }
                                                                catch (ClassCastException e) {
                                                                        throw new 
                                                                                IllegalArgumentException("Cannot convert '" + text_value
                                                                                                         + "' to " + field.getType(), e);
                                                                }
                                                        }
                                                }
                                                catch (SecurityException e) {
                                                        throw new 
                                                                RuntimeException("Bug detected while processing option " + 
                                                                                 option.name(), e);
                                                }
                                                catch (IllegalAccessException e) {
                                                        throw new 
                                                                RuntimeException("Bug detected while processing option " + 
                                                                                 option.name(), e);
                                                }
                                                catch (Exception e) {
                                                        e.printStackTrace();
                                                }
                                                
                                        }
                                }
                        }
                }
        }
        
	private static void put(Document document,
                                Node root,
                                String elem_name,
                                String value, 
                                String comment_str) {
		Text t = document.createTextNode("\n\n\t");
		root.appendChild(t);
		Comment comment = document.createComment(comment_str);
		root.appendChild(comment);
		t = document.createTextNode("\n\t");
		root.appendChild(t);
		Element element = document.createElement(elem_name);
		t               = document.createTextNode(" "+value+" ");
		element.appendChild(t);
		root.appendChild(element);
	}
    
	public void write(String file) throws Exception {
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		DocumentBuilder db         = dbf.newDocumentBuilder();
		Document document          = db.newDocument();
		Element root               = document.createElement("srm-configuration");
                for (Class c = getClass(); c != null; c = c.getSuperclass()) {
                        for (Field field : c.getDeclaredFields()) {
                                Option option = field.getAnnotation(Option.class);
                                try {
                                        if (option != null) {
                                                if (option.save()) {
                                                        field.setAccessible(true);
                                                        Object value = field.get(this);
                                                        String svalue="null";
                                                        if (value != null ) {
                                                                svalue = value.toString();
                                                        }
                                                        String description = option.description();
                                                        String unit = option.unit();
                                                        if (description.length() == 0)
                                                                description = option.name();
                                                        StringBuilder sb = new StringBuilder();
                                                        sb.append(description.replaceAll("\n"," "));
                                                        if (option.defaultValue().length()>0) { 
                                                                sb.append(", default is "+ option.defaultValue());
                                                        }
                                                        if (unit.length()>0) { 
                                                                sb.append(" ("+unit+")");
                                                        }
                                                        put(document,root,option.name(),svalue,sb.toString());
                                                }
                                        }
                                }
                                catch (SecurityException e) {
                                        throw new RuntimeException("Bug detected while processing option " + option.name(), e);
                                }
                                catch (IllegalAccessException e) {
                                        throw new RuntimeException("Bug detected while processing option " + option.name(), e);
                                }
                        }
                }
		Text t = document.createTextNode("\n");
		root.appendChild(t);
		document.appendChild(root);
		TransformerFactory tFactory = TransformerFactory.newInstance();
		Transformer transformer     = tFactory.newTransformer();
		DOMSource source            = new DOMSource(document);
		StreamResult result         = new StreamResult(new FileWriter(file));
		transformer.transform(source, result);
	}
    
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("SRM Configuration:");
                for (Class c = getClass(); c != null; c = c.getSuperclass()) {
                        for (Field field : c.getDeclaredFields()) {
                                Option option = field.getAnnotation(Option.class);
                                try {
                                        if (option != null) { 
                                                if (option.log()) {
                                                        field.setAccessible(true);
                                                        Object value = field.get(this);
                                                        sb.append("\n\t"+option.name()+"="+value);
                                                }
                                        }
                                }
                                catch (SecurityException e) {
                                        throw new RuntimeException("Bug detected while processing option " + option.name(), e);
                                } 
                                catch (IllegalAccessException e) {
                                        throw new RuntimeException("Bug detected while processing option " + option.name(), e);
                                }
                        }
                }
		if (getFileMetaData && surls != null) {
			sb.append("\n\taction is getFileMetaData");
			for (int i = 0; i<surls.length; ++i) {
				sb.append("\n\tsurl["+i+"]=").append(this.surls[i]);
			}
		}
		if (getSpaceTokens && srmUrl != null) {
			sb.append("\n\taction is getSpacetokens");
			sb.append("\n\tsurl=").append(this.srmUrl);
		}
		if (getPermission && surls != null) {
			sb.append("\n\taction is getPermissions");
			for (int i = 0; i<surls.length; ++i) {
				sb.append("\n\tsurl["+i+"]=").append(this.surls[i]);
			}
		}
		if (checkPermission &&  surls != null) {
			sb.append("\n\taction is checkPermissions");
			for (int i = 0; i<surls.length; ++i) {
				sb.append("\n\tsurl["+i+"]=").append(this.surls[i]);
			}
		}
		if (setPermission &&  setPermissionSurl != null) {
			sb.append("\n\taction is setPermissions");
			sb.append("\n\tsur=").append(this.setPermissionSurl);
		}
		if (extendFileLifetime && surls != null) {
			sb.append("\n\taction is extendFileLifetime");
			for (int i = 0; i<surls.length; ++i) {
				sb.append("\n\tsurl["+i+"]=").append(this.surls[i]);
			}
		}
		if (ls && surls != null) {
			sb.append("\n\taction is ls");
			for(int i = 0; i< surls.length; i++)
				sb.append("\n\tsurl[" + i + "]=").append(this.surls[i]);
		}
		if (is_rm && surls != null) {
			sb.append("\n\taction is rm");
			for(int i = 0; i< surls.length; i++)
				sb.append("\n\tsurl[" + i + "]=").append(this.surls[i]);
		}
		if (is_rmdir && surls != null) {
			sb.append("\n\taction is rmdir");
			for(int i = 0; i< surls.length; i++)
				sb.append("\n\tsurl[" + i + "]=").append(this.surls[i]);
		}
		if (is_mkdir && surls != null) {
			sb.append("\n\taction is mkdir");
			for(int i = 0; i< surls.length; i++)
				sb.append("\n\tsurl[" + i + "]=").append(this.surls[i]);
		}
		if (reserveSpace) {
			sb.append("\n\taction is reserveSpace");
			if ( surls != null ) {
				for(int i = 0; i< surls.length; i++) {
					sb.append("\n\tsrm url=").append(this.surls[i]);
				}
			}
			sb.append("\n\tprotocols");
			for(int i = 0; i< protocols.length; i++) {
				sb.append("\n\tprotocol[" + i + "]=").append(this.protocols[i]);
			}
			sb.append("\n\tarray of client networks:");
			if (arrayOfClientNetworks==null){
				sb.append("\n\tnull");
			} else {
				for(int i = 0; i<arrayOfClientNetworks.length; i++) {
					sb.append("\n\tnetwork["+i+"]=").append(arrayOfClientNetworks[i]);
				}
			}
		}
		if (releaseSpace) {
			sb.append("\n\taction is releaseSpace");
			if ( surls != null ) {
				for(int i = 0; i< surls.length; i++) {
					sb.append("\n\tsrm url=").append(this.surls[i]);
				}
			}
		}
		if (getSpaceMetaData) {
			sb.append("\n\taction is getSpaceMetaData");
			if ( getSpaceTokensList() != null ) {
				for(int i = 0; i< getSpaceTokensList().length; i++) {
					sb.append("\n\tgetSpaceMetaDataToken =").append(getSpaceTokensList()[i]);
				}
			}
            
		}
		if (from!= null) {
			for(int i = 0; i<from.length; ++i) {
				sb.append("\n\tfrom["+i+"]=").append(this.from[i]);
			}
		}
		if(to != null) {
			sb.append("\n\tto=").append(this.to).append('\n');
		}
		return sb.toString();
        }
    
	/** Getter for property from.
	 * @return Value of property from.
	 */
	public java.lang.String[] getFrom() {
		return this.from;
	}
    
	/** Setter for property from.
	 * @param from New value of property from.
	 */
	public void setFrom(java.lang.String[] from) {
		this.from = from;
	}
    
	/** Getter for property to.
	 * @return Value of property to.
	 */
	public java.lang.String getTo() {
		return to;
	}
    
	/** Setter for property to.
	 * @param to New value of property to.
	 */
	public void setTo(java.lang.String to) {
		this.to = to;
	}
    
	/** Getter for property logger.
	 * @return Value of property logger.
	 *
	 */
	public org.dcache.srm.Logger getLogger() {
		return logger;
	}
    
	/** Setter for property logger.
	 * @param logger New value of property logger.
	 *
	 */
	public void setLogger(org.dcache.srm.Logger logger) {
		this.logger = logger;
	}
    
    
	public String[] getLsURLs() {
		return surls;
	}

	public void setLsURLs(String[] inURLs) {
		surls = inURLs;
	}

	public String[] getSurls() {
		return surls;
	}

	public void setSurls(String[] inURLs) {
		surls = inURLs;
	}
    
	public String[] getArrayOfRequestTokens() { 
		return arrayOfRequestTokens;
	}
    
	public void setArrayOfRequestTokens(String[] tokens) {
		arrayOfRequestTokens = tokens;
	}
    
	public String[] getReserveSpaceURLs() {
		return surls;
	}
    
	public void setReserveSpaceURLs(String[] inURLs) {
		surls = inURLs;
	}
    
	public String[] getReleaseSpaceURLs() {
		return surls;
	}
    
	public void setReleaseSpaceURLs(String[] inURLs) {
		surls = inURLs;
	}
    
    
	public String[] getRmURLs() {
		return surls;
	}
    
	public String[] getRmdirURLs() {
		return surls;
	}
    
	public void setRmURLs(String[] inURLs) {
		surls = inURLs;
	}
    
	public String[] getMkDirURLs() {
		return surls;
	}
    
	public void setMkDirURLs(String[] inURLs) {
		surls = inURLs;
	}
    
	public void setRmdirURLs(String[] inURLs) {
		surls = inURLs;
	}
    
	/** Getter for property getFileMetaDataSurls.
	 * @return Value of property getFileMetaDataSurls.
	 *
	 */
	public java.lang.String[] getGetFileMetaDataSurls() {
		return this.surls;
	}
    
	/** Setter for property getFileMetaDataSurls.
	 * @param getFileMetaDataSurls New value of property getFileMetaDataSurls.
	 *
	 */
	public void setGetFileMetaDataSurls(java.lang.String[] getFileMetaDataSurls) {
		this.surls = getFileMetaDataSurls;
	}
    
	/** Getter for property getPermissionSurls.
	 * @return Value of property getPermissionSurls.
	 *
	 */
	public java.lang.String[] getGetPermissionSurls() {
		return this.surls;
	}

	/** Getter for property checkPermissionSurls.
	 * @return Value of property checkPermissionSurls.
	 *
	 */
	public java.lang.String[] getCheckPermissionSurls() {
		return this.surls;
	}

	public java.lang.String[] getExtendFileLifetimeSurls() { 
		return this.surls; 
	}

	/** Getter for property setPermissionSurls.
	 * @return Value of property setPermissionSurls.
	 *
	 */
	public java.lang.String getSetPermissionSurl() {
		return this.setPermissionSurl;
	}

	/** Setter for property getPermissionSurls.
	 * @param getPermissionSurls New value of property getPermissionSurls.
	 *
	 */
	public void setGetPermissionSurls(java.lang.String[] getPermissionSurls) {
		this.surls = getPermissionSurls;
	}

	/** Setter for property checkPermissionSurls.
	 * @param checkPermissionSurls New value of property checkPermissionSurls.
	 *
	 */
	public void setCheckPermissionSurls(java.lang.String[] checkPermissionSurls) {
		this.surls = checkPermissionSurls;
	}

	/** Setter for property setPermissionSurls.
	 * @param setPermissionSurls New value of property setPermissionSurls.
	 *
	 */
	public void setSetPermissionSurl(java.lang.String setPermissionSurls) {
		this.setPermissionSurl = setPermissionSurls;
	}

	public void setExtendFileLifetimeSurls(java.lang.String surls[]) {
		this.surls = surls;
	}
    
	/** Getter for property storageElementInfoServerWSDL.
	 * @return Value of property storageElementInfoServerWSDL.
	 *
	 */
	public java.lang.String getStorageElementInfoServerWSDL() {
		return storageElementInfoServerWSDL;
	}
    
	/** Setter for property storageElementInfoServerWSDL.
	 * @param storageElementInfoServerWSDL New value of property storageElementInfoServerWSDL.
	 *
	 */
	public void setStorageElementInfoServerWSDL(java.lang.String storageElementInfoServerWSDL) {
		this.storageElementInfoServerWSDL = storageElementInfoServerWSDL;
	}
    
    
	public String[] getArrayOfClientNetworks() {
		return arrayOfClientNetworks;
	}
    
	public void setArrayOfClientNetworks(String[] a) {
		arrayOfClientNetworks=a;
	}
    
    
	/** Getter for property advisoryDeleteSurls.
	 * @return Value of property advisoryDeleteSurls.
	 *
	 */
	public java.lang.String[] getAdvisoryDeleteSurls() {
		return this.surls;
	}
    
	/** Setter for property advisoryDeleteSurls.
	 * @param advisoryDeleteSurls New value of property advisoryDeleteSurls.
	 *
	 */
	public void setAdvisoryDeleteSurls(java.lang.String[] advisoryDeleteSurls) {
		this.surls = advisoryDeleteSurls;
	}

	/**
	 * Getter for property getRequestStatusSurl.
	 * @return Value of property getRequestStatusSurl.
	 */
	public java.lang.String getGetRequestStatusSurl() {
		return getRequestStatusSurl;
	}
    
	/**
	 * Setter for property getRequestStatusSurl.
	 * @param getRequestStatusSurl New value of property getRequestStatusSurl.
	 */
	public void setGetRequestStatusSurl(java.lang.String getRequestStatusSurl) {
		this.getRequestStatusSurl = getRequestStatusSurl;
	}
    
	public String[] getBringOnlineSurls() {
		return surls;
	}
    
	public void setBringOnlineSurls(String[] bringOnlineSurls) {
		this.surls = bringOnlineSurls;
	}
    
	public void setJobPriority(int p) {
		this.extraParameters.put("priority",Integer.toString(p));
	}
	public int getJobPriority() {
		return Integer.parseInt((String)extraParameters.get("priority"));
	}
    
	public Map getExtraParameters() {
		return this.extraParameters;
	}


	public String[] getSpaceTokensList() {
		return spaceTokensList;
	}

	public void setSpaceTokensList(String[] spaceTokensList) {
		this.spaceTokensList = spaceTokensList;
	}

        public String getSrmUrl() {
		return srmUrl;
	}

	public void setSrmUrl(String srmUrl) {
		this.srmUrl = srmUrl;
	}
}
