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

import org.w3c.dom.Comment;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.Text;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.dcache.srm.Logger;
import org.dcache.srm.client.Transport;
import org.dcache.util.Args;

/**
 *
 * @author  timur
 */
public class Configuration {

    public static final String SRMCPCONFIGNAMESPACE="srmcp.srm.fnal.gov";
    private static final String webservice_pathv1 = "srm/managerv1";
    private static final String webservice_pathv2 = "srm/managerv2";
    private static final String HOME_DIRECTORY=System.getProperty("user.home");
    private static final String PATH_SEPARATOR=System.getProperty("file.separator");
    private static final String CONFIGURATION_DIRECTORY=".srmconfig";
    private String DEFAULT_CONFIG_FILE=HOME_DIRECTORY+
    PATH_SEPARATOR+
    CONFIGURATION_DIRECTORY+
    PATH_SEPARATOR+"config.xml";

    @Option(
            name = "default_port",
            description = "default SRM port number",
            defaultValue = "8443",
            required=false,
            log=true,
            save=true
    )
    private int srmDefaultPortNumber;

    public int getDefaultSrmPortNumber() {
        return srmDefaultPortNumber;
    }

    public void setDefaultSrmPortNumber(int port) {
        this.srmDefaultPortNumber=port;
    }

    @Option(
            name = "debug",
            description = "enable debug output (including stack traces)",
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
            description = "path to srmcp product directory",
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
            description = "displays usage",
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
            description = "displays usage",
            defaultValue = "false",
            required=false,
            log=true
    )
    private boolean is_help;


    @Option(
            name = "mapfile",
            description = "path to the \"glue\" mapfile",
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
            description = "path to wsdl in web service url",
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
            name = "use_proxy",
            description = "use user proxy(true) or use certificates directly(false)",
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
            description = "comma separated globus tcp port range, like MIN,MAX",
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

    public String getProtocolsList() {
        return protocols_list;
    }

    public void setProtocolsList(String protocols_list) {
        this.protocols_list = protocols_list;
    }

    private String[] protocols = new String[]   {"gsiftp","dcap","http"};

    public String[] getProtocols() {
        return this.protocols;
    }

    public void setProtocols(String[] protocols) {
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
            description = "number of streams, nonnegative integer. Multi-stream extended block transfer require writes to be performed in server-passive and reads in server active mode. If client specified server_mode option conflicts with the multi-stream required mode, the transfer will be performed in a single stream mode, regardless of value specified for the streams_num option",
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
            name = "save_conf",
            description = "path to the file where new configuration will be saved",
            required=false,
            log=true
    )
    private String save_config_file;

    public String getSave_config_file() {
        return save_config_file;
    }

    public void setSave_config_file(String save_config_file) {
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
            defaultValue = "null",
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
            defaultValue = "null",
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
            defaultValue = "null",
            unit="bytes",
            required=false,
            log=true
    )
    private Long desiredReserveSpaceSize;

    public Long getDesiredReserveSpaceSize() {
        return desiredReserveSpaceSize;
    }

    public void setDesiredReserveSpaceSize(Long size) {
        desiredReserveSpaceSize=size;
    }

    @Option(
            name = "guaranteed_size",
            description = "guaranteed space reservation size",
            defaultValue = "null",
            unit="bytes",
            required=false,
            log=true
    )
    private Long guaranteedReserveSpaceSize;


    public Long getGuaranteedReserveSpaceSize() {
        return guaranteedReserveSpaceSize;
    }

    public void setGuaranteedReserveSpaceSize(Long size) {
        guaranteedReserveSpaceSize=size;
    }


    @Option(
            name = "lifetime",
            description = "desired lifetime in seconds",
            defaultValue = "null",
            unit="seconds",
            required=false,
            log=true
    )
    private Long desiredLifetime;

    public Long getDesiredLifetime() {
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

    public String getCopyjobfile() {
        return copyjobfile;
    }

    public void setCopyjobfile(String copyjobfile) {
        this.copyjobfile = copyjobfile;
    }

    @Option(
            name = "wsdl_url",
            description = "full URL to web service WSDL, overrides \"-webservice_path\" and \"-webservice_protocol\" options",
            required=false,
            log=true,
            save=true
    )
    private String wsdl_url;

    public String getWsdl_url() {
        return wsdl_url;
    }

    public void setWsdl_url(String wsdl_url) {
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
            description =  "number of retries before client gives up",
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
            description =  "connect to WSDL instead of connecting to the server directly",
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

    @Option(name = "transport",
            description = "the transport to use when connecting to server.",
            defaultValue = "GSI",
            required=false,
            log=true,
            save=true)
    private String transport;

    public Transport getTransport() {
        return Transport.transportFor(transport);
    }

    public void setTransport(String transport) {
        this.transport = Transport.transportFor(transport).name();
    }

    @Option(
            name = "delegate",
            description =  "enables delegation of user credenital to the server",
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
            description =  	"specifies type (full or limited) of delegation",
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

    public String getReport() {
        return report;
    }

    public void setReport(String report) {
        this.report = report;
    }

    @Option(
            name = "server_mode",
            description =  	"gridftp server mode for data transfer (\"passive\" or \"active\"). Needs to be explicitly specified to appropriate value if client or ftp server is behind a firewall",
            defaultValue = "null",
            required=false,
            log=true,
            save=true
    )
    private String server_mode;

    public String getServerMode() { return server_mode; }

    synchronized public void setServerMode(String x) {
        server_mode=x;
    }

    @Option(
            name = "storagetype",
            description =  	"<permanent|volatile|durable> to specify type of storage to use",
            defaultValue = "null",
            required=false,
            log=true,
            save=true
    )
    private String storagetype;

    public String getStorageType() {
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
            description = "long format mode",
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
            description = "directory tree depth level",
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
            description = "offset the number of elements to report",
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
            description = "number of elements to report",
            defaultValue = "null",
            required=false,
            log=true,
            save=true
    )
    private Integer lsCount;

    public Integer getLsCount() {
        return lsCount;
    }

    public void setLsCount(Integer count) {
        lsCount=count;
    }

    @Option(
            name = "srm_protocol_version",
            description = "srm protocol version",
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
            description = "srm protocol version 1",
            defaultValue = "false",
            required=false,
            log=false
    )
    private boolean isSrmv1;

    @Option(
            name = "2",
            description = "srm protocol version 2",
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
            name = "send_cksm",
            description = "send check sum to gridftp server",
            required=false,
            log=true,
            defaultValue="true",
            save=true
    )
    private boolean doSendCheckSum;

    public boolean getDoSendCheckSum() {
        return doSendCheckSum;
    }

    public void setDoSendCheckSum(boolean sendCheckSum ) {
        doSendCheckSum = sendCheckSum;
    }

    @Option(
            name = "cksm_type",
            description = "<type|negotiate> calculate and verify server and client checksum values using this type (adler32|MD4|MD5|....). If checksum value has been omitted, missing value will be computed over the local file. If negotiate is set - client will attempt to negotiate cksm type for the file checksum value avilable at the server. For gridftp transfers to/from servers that support checksumming features",
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

    private String arrayOfRequestTokens[];

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

    @Option(
            name = "print_performance",
            description = "if print_performance is set to a true, " +
            "print start and end time of each client run, " +
            "followed by the number of the specific run, "+
            "followed by the value of the perf string, ",
            defaultValue = "false",
            required=false,
            log=true
    )
    private boolean printPerformance;

    public boolean isPrintPerfomance() {
        return printPerformance ;
    }

    public void setPrintPerfomance(boolean printPerformance) {
        this.printPerformance = printPerformance;
    }

    @Option(
            name = "performance_test_name",
            description = "if performance_test_name is set to a true, " +
            "and performance_test_name is specified, this name will be printed in " +
            "each line with performance info ",
            defaultValue = "null",
            required=false,
            log=true
    )
    private String performanceTestName;

    public String getPerformanceTestName() {
        return performanceTestName ;
    }

    public void setPerformanceTestName(String performanceTestName) {
        this.performanceTestName = performanceTestName;
    }


    @Option(
            name = "repeat",
            description = "number of times to repeat a client run",
            defaultValue = "1",
            required=false,
            log=true,
            save=true
    )
    private int repeatCount;

    public int getRepeatCount() {
        return repeatCount;
    }

    public void setRepeatCount(int count) {
        repeatCount=count;
    }

    @Option(
            name = "dryrun",
            description =  	"performs srm \"get/put\", without actual data transfer for use as a diagnostics tool",
            defaultValue = "false",
            required=false,
            log=true
    )
    private boolean dryRun;

    public boolean isDryRun() {
        return dryRun;
    }

    public void setDryrun(boolean dryrun) {
        this.dryRun = dryrun;
    }


    @Option(
            name = "first_byte_timeout",
            description = "griftp client option, timeout before first byte sent/received in seconds",
            defaultValue = "3600",
            required=false,
            log=true,
            save=true
    )
    private int firstByteTimeout;

    public int getFirstByteTimeout() {
        return firstByteTimeout;
    }

    @Option(
            name = "next_byte_timeout",
            description = "griftp client option, timeout before next byte sent/received",
            defaultValue = "600",
            required=false,
            log=true,
            save=true
    )
    private int nextByteTimeout;

    public int getNextByteTimeout() {
        return nextByteTimeout;
    }


    private String srmUrl;
    private String surls[];

    /** Creates a new instance of Configuration */
    public Configuration() {
        extraParameters = new HashMap<>();
    }

    private String mkdir_options =
        " srmmkdir options : None\n"+
        "Examples: \n"+
        "\t\t srm -mkmdir srm://fledgling06.fnal.gov:8443/srm/managerv2?SFN=/dir/path/ \n";

    private String rm_options =
        " srmrm options : None \n"+
        "\t\t Applies to files only.\n";

    private String stage_options =
        " stage options: None \n";

    private String getPermission_options =
        " srm-get-permissions options: None \n";

    private String checkPermission_options =
        " srm-check-permissions options : None \n";

    public final String usage() {
        String general_options=
            " General Options :\n"+
            OptionParser.printOptions(this,"version",
                    "debug",
                    "srmcphome",
                    "gsissl",
                    "mapfile",
                    "wsdl_url",
                    "webservice_path",
                    "webservice_protocol",
                    "use_proxy",
                    "x509_user_proxy",
                    "x509_user_cert",
                    "x509_user_key",
                    "x509_user_trusted_certificates",
                    "globus_tcp_port_range",
                    "gss_expected_name",
                    "srm_protocol_version",
                    "1",
                    "2",
                    "conf",
                    "save_conf",
                    "retry_timeout",
                    "retry_num",
                    "connect_to_wsdl",
                    "delegate",
                    "full_delegation",
                    "transport",
                    "h",
            "help");
        if (getFileMetaData) {
            return
            "\nUsage:get-file-metadata [command line options]  srmurl\n\n"+
            "       default options can be set in configuration file \n"+
            "       or overriden by the command line options\n\n"+
            general_options;
        }
        if(getSpaceTokens) {
            String getSpaceTokensOptions="get-space-tokens options :\n"+
            OptionParser.printOptions(this,"space_desc");
            return
            "\nUsage:get-space-tokens [command line options]  srmurl\n\n"+
            "       default options can be set in configuration file \n"+
            "       or overriden by the command line options\n\n"+
            (isHelp()==true?general_options+getSpaceTokensOptions:getSpaceTokensOptions);
        }
        if (advisoryDelete) {
            return
            "Usage:advisory-delete [command line options]  srmurl(s)\n"+
            "     default options can be set in configuration file \n"+
            "     or overriden by the command line options\n\n"+
            general_options;
        }
        if (getRequestStatus) {
            String getRequestStatusOptions=" get-request-status options :\n"+
            OptionParser.printOptions(this,"request_id");
            return
            "\nUsage:get-request-status  [command line options]  srmurl \n\n"+
            "       where srmurl is one of the surl specified in the original request\n" +
            "       and is used for determening the srm endpoint location\n"+
            "       default options can be set in configuration file \n"+
            "       or overriden by the command line options\n\n"+
            (isHelp()==true?general_options+getRequestStatusOptions:getRequestStatusOptions);
        }
        if (copy) {
            String copy_options=" srmcp options :\n"+
            OptionParser.printOptions(this,"urlopy",
                    "buffer_size",
                    "tcp_buffer_size",
                    "streams_num",
                    "send_cksm",
                    "server_mode",
                    "storagetype",
                    "array_of_client_networks",
                    "protocols",
                    "space_token",
                    "retention_policy",
                    "access_latency",
                    "access_pattern",
                    "connection_type",
                    "overwrite_mode",
                    "pushmode",
                    "srmstage",
                    "use_urlcopy_script",
                    "srm_protocol_version",
                    "2",
                    "1",
                    "priority",
                    "request_lifetime",
                    "copyjobfile",
                    "report",
                    "cksm_type",
                    "cksm_value",
                    "first_byte_timeout",
                    "next_byte_timeout")+
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
            "\t\t srmcp gsiftp://ftphost.org//path/file srm://myhost.mydomain.edu:8443//dir1/dir2/file\n"+
            "port number is optional\n";
            return
            "\nUsage: srmcp [command line options] source(s) destination\n\n"+
            " or  : srmcp [command line options] -copyjobfile=<file>\n"+
            "       either source(s) or destination or both should be (an) srm url\n"+
            "       default options can be set in configuration file \n"+
            "       or overriden by the command line options\n\n"+
            (isHelp()==true?general_options+copy_options:copy_options);
        }
        if (bringOnline) {
            String bring_online_options=" srm-bring-online options: \n"+
            OptionParser.printOptions(this,
                    "storagetype",
                    "protocols",
                    "space_token",
                    "access_pattern",
                    "connection_type",
                    "array_of_client_networks",
                    "retention_policy",
                    "access_latency",
                    "srm_protocol_version",
                    "2","1",
                    "request_lifetime",
                    "lifetime",
                    "priority",
            "report");

            return
            "\nUsage: srm-bring-online [command line options] srmUrl(s)\n\n"+
            "       default options can be set in configuration file \n"+
            "       or overriden by the command line options\n\n"+
            (isHelp()==true?general_options+bring_online_options:bring_online_options);
        }

        if (reserveSpace) {
            String reserveSpace_options=" srm-reserve-space options : \n"+
            OptionParser.printOptions(this,
                    "space_desc",
                    "retention_policy",
                    "access_latency",
                    "access_pattern",
                    "array_of_client_networks",
                    "protocols",
                    "connection_type",
                    "desired_size",
                    "guaranteed_size",
            "lifetime")+
            printMandatoryOptions("retention_policy","guaranteed_size");
            return
            "\nUsage: srm-reserve-space [command line options]  srmUrl\n\n"+
            "       default options are read from configuration file\n"+
            "       or overriden by the command line options\n\n"+
            (isHelp()==true?general_options+reserveSpace_options:reserveSpace_options);
        }
        if(getSpaceMetaData) {
            String getSpaceMetaData_options = " srm-get-space-metadata options:\n"+
            OptionParser.printOptions(this,"space_tokens")+
            printMandatoryOptions("space_tokens");
            return
            "\nUsage: srm-get-space-metadata [commnad line options] srmUrl\n\n"+
            "       default options can be set in configuration file \n"+
            "       or overriden by the command line options\n\n"+
            (isHelp()==true?general_options+getSpaceMetaData_options:getSpaceMetaData_options);


        }
        if (releaseSpace) {
            String releaseSpace_options = " srm-release-space options :\n"+
            OptionParser.printOptions(this,"space_token","force")+
            printMandatoryOptions("space_token");

            return
            "\nUsage: srm-release-space [command line options]  srmUrl\n\n"+
            "     default options can be set in configuration file \n"+
            "     or overriden by the command line options\n\n"+
            (isHelp()==true?general_options+releaseSpace_options:releaseSpace_options);
        }
        if (ls) {
            String ls_options="srmls options :\n"+OptionParser.printOptions(this,"l","recursion_depth","count","offset");
            return
            "\nUsage: srmls [command line options] srmUrl [[srmUrl]...]\n\n" +
            (isHelp()==true?general_options+ls_options:ls_options);
        }
        if (is_rm) {
            return
            "\nUsage: srmrm [command line options] srmUrl [[srmUrl]...]\n\n" +
            "       default options can be set in configuration file \n"+
            "       or overriden by the command line options\n\n"+
            (isHelp()==true?general_options+rm_options:rm_options);
        }
        if (is_mv) {
            String move_options= "srmmv options :\n"+
            "Only moves within single storage system are allowed \n"+
            "(you can't mv from one SRM to another SRM \n"+
            "(or from/to remote/local filesystem, use copy and delete)).\n";
            return
            "\nUsage: srmmv [command line options] sourceSrmUrl destinationSrmUrl \n\n"+
            (isHelp()==true?general_options+move_options:move_options);
        }
        if (is_getRequestSummary) {
            String getRequestSummary_options=" srm-get-request-summary options:\n"+
            OptionParser.printOptions(this,"request_tokens");
            return
            "\nUsage: srm-get-request-summary [command line options] srmUrl \n\n"+
            "       default options can be set in configuration file \n"+
            "       or overriden by the command line options\n\n"+
            (isHelp()==true?general_options+getRequestSummary_options:getRequestSummary_options);
        }
        if (is_AbortRequest) {
            String abortRequest_options = " srm-abort-request options: \n"+
            OptionParser.printOptions(this,"request_tokens");
            return
            "\nUsage: srm-abort-request [command line options] srmUrl \n\n"+
            "       default options can be set in configuration file \n"+
            "       or overriden by the command line options\n"+
            "       the command line options are one or more of the following:\n"+
            (isHelp()==true?general_options+abortRequest_options:abortRequest_options);
        }
        if (is_AbortFiles) {
            String abortFiles_options=
                " srm-abort-files options : \n"+
                OptionParser.printOptions(this, "request_tokens");
            return
            "\nUsage: srm-abort-files [command line options] srmUrl [[srmUrl]...] \n\n"+
            "       default options can be set in configuration file \n"+
            "       or overriden by the command line options\n\n"+
            (isHelp()==true?general_options+abortFiles_options:abortFiles_options);
        }
        if (is_ReleaseFiles) {
            String releaseFiles_options=
                " srm-release-files options : \n"+
                OptionParser.printOptions(this,"request_tokens","do_remove");
            return
            "\nUsage: srm-relese-files [command line options] srmUrl [[srmUrl]...] \n\n"+
            "       default options can be set in configuration file \n"+
            "       or overriden by the command line options\n\n"+
            (isHelp()==true?general_options+releaseFiles_options:releaseFiles_options);
        }
        if (is_getRequestTokens) {
            String getRequestTokens_options=
                " srm-get-request-tokens options :\n"+
                OptionParser.printOptions(this,"request_desc");
            return
            "\nUsage: srm-get-request-tokens [command line options] srmUrl  \n\n"+
            "       default options can be set in configuration file \n"+
            "       or overriden by the command line options\n\n"+
            (isHelp()==true?general_options+getRequestTokens_options:getRequestTokens_options);
        }
        if (is_rmdir) {
            String rmdir_options =
                " srmrmdir options :\n"+
                OptionParser.printOptions(this,
                "recursive")+
                "\t\t -rmdir is defined in SRM specification as :\n"+
                "\t\t \"applies to dir doRecursiveRemove is false by edefault. To distinguish from \n"+
                "\t\t srmRm(), this function is for directories only. \"\n"+
                "\t\t so it is unclear id the directories must be empty. \n"+
                "\t\t We interpret \"rmdir\" as Unix \"rmdir\" which allows to remove only empty directories \n"+
                "\t\t extending it to have an ability to remove trees of empty directories. \n"+
                "\t\t Removal of multiple directories is not supported \n"+
                "\t\t Removal of files is not supported (use srmrm).\n";
            return
            "\nUsage: srmrmdir [command line options] srmUrl \n\n" +
            "       default options can be set in configuration file \n"+
            "       or overriden by the command line options\n\n"+
            (isHelp()==true?general_options+rmdir_options:rmdir_options);
        }
        if (is_mkdir) {
            return
            "\nUsage: srmmkdir [command line options] srmUrl \n\n" +
            "       default options can be set in configuration file \n"+
            "       or overriden by the command line options\n"+
            "       the command line options are one or more of the following:\n"+
            (isHelp()==true?general_options+mkdir_options:mkdir_options);
        }
        if(stage) {
            return
            "\nUsage: srmstage [command line options] srmUrl [[srmUrl]...] \n\n" +
            "       default options can be set in configuration file \n"+
            "       or overriden by the command line options\n\n"+
            (isHelp()==true?general_options+stage_options:stage_options);

        }
        if(getPermission) {
            return
            "\nUsage: srmgetpermission [command line options] srmUrl [[srmUrl]...] \n\n" +
            "       default options can be set in configuration file \n"+
            "       or overriden by the command line options\n\n"+
            (isHelp()==true?general_options+getPermission_options:getPermission_options);
        }
        if(checkPermission) {
            return
            "\nUsage: srmcheckpermission [command line options] srmUrl [[srmUrl]...] \n\n" +
            "       default options can be set in configuration file \n"+
            "       or overriden by the command line options\n\n"+
            (isHelp()==true?general_options+checkPermission_options:checkPermission_options);
        }
        if(setPermission) {
            String setPermission_options =
                " srm-set-permissions options : \n"+
                OptionParser.printOptions(this,
                        "type",
                        "owner",
                        "other",
                "group")+
                printMandatoryOptions("type");

            return
            "\nUsage: srm-set-permissions [command line options] srmUrl [[srmUrl]...] \n\n" +
            "       default options can be set in configuration file \n"+
            "       or overriden by the command line options \n\n"+
            (isHelp()==true?general_options+setPermission_options:setPermission_options);
        }
        if(extendFileLifetime) {
            String extendFileLifetime_options_string =
                " srm-extend-file-lifetime options :\n"+
                OptionParser.printOptions(this,"request_token",
                        "file_lifetime",
                "pin_lifetime");
            return
            "\nUsage: srm-extend-file-lifetime [command line options] srmUrl [[srmUrl]...] \n\n" +
            "       default options can be set in configuration file \n"+
            "       or overriden by the command line options\n\n"+
            (isHelp()==true?
                    general_options+
                    extendFileLifetime_options_string :
                        extendFileLifetime_options_string);
        }
        if(ping) {
            return
            "\nUsage: srmping  [command line options] srmUrl \n\n" +
            "       default options can be set in configuration file \n"+
            "       or overriden by the command line options\n\n"+
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

    public void parseArguments(String arguments[]) throws Exception {
        //
        // This is nasty kludge to make Jon Bakken happy,
        // namely handle case when option and option value
        // are separated by blank space. The kludge attempts
        // to insert "=" instead of " " and concatenates
        // input argument list into a String which is passed
        // to Args constructor. So nothing gets modified
        // elsewhere. In addition attempt has been made to
        // handle arbitrary space paddings between "="
        //
        Set<String> optionMap=OptionParser.getOptions(this);
        StringBuilder sb = new StringBuilder();
        {
            int i=0;
            while(i<arguments.length) {
                //
                // follow Gerd's suggestion and strip "-" to
                // expose possible option name
                //
                String name = arguments[i];
                while(name.startsWith("-")) {
                    name=name.substring(1);
                }
                if (optionMap.contains(name)) {
                    //
                    // we have space separated option value
                    // that is "-option "
                    //
                    sb.append(arguments[i]);
                    i++;
                    //
                    // if this was last string on command line we are done
                    // a case "-option" is already handled by OptionParser
                    // (it assumes boolean option with true value and
                    // checks if option is boolean, of not we will get
                    // exception later)
                    //
                    if (i>=arguments.length) {
                        break;
                    }
                    String value=arguments[i];
                    //
                    // Option value must be :
                    // 1) string that does not start with "-"
                    //    (otherwise this is another option)
                    //
                    // 2) string that does not start with "="
                    //
                    if(value.startsWith("-")) {
                        //
                        //"-1" and "-2" are magic numbers, nothing I can do
                        // here but:
                        //
                        if (value.equals("-1")||value.equals("-2")) {
                            sb.append(" ");
                            continue;
                        }
                        try {
                            //
                            // check that this is not negative request number,
                            // if it is, "replace" space with "=", so
                            // "-option -123" turns into "-option=-123"
                            // make sure we append space in the end,
                            // advance counter and continue to the next option
                            Integer.parseInt(value);
                            sb.append("=").append(value).append(" ");
                            i++;
                            continue;
                        }
                        catch (NumberFormatException nfe) {
                            // if we got here, we found another option
                            // so our input looked like "-option1 -option2"
                            // this is what we know how to process, just add
                            // space and continue to next option
                            sb.append(" ");
                            continue;
                        }
                    }
                    else if (!value.startsWith("=")) {
                        //
                        // if value does not start with "=" and
                        // it is not an argument, and here we
                        // rely on the fact that srm command line arguments
                        // are always SURLS, so we can tell what is option
                        // value and what is argument by checking this:
                        //
                        if (!value.startsWith("file:") &&
                                !value.startsWith("srm:") &&
                                !value.startsWith("gsiftp:") &&
                                !value.startsWith("http")) {
                            sb.append("=").append(value).append(" ");
                            i++;
                            continue;
                        }
                        else {
                            //
                            // we got here if we had "-option srm:/..."
                            // just append space and proceed to attach SURL at the next
                            // iteration
                            sb.append(" ");
                            continue;
                        }
                    }
                    else {
                        // we got here if
                        // value of the option looks like :
                        // "=123" "="
                        // do nothing, we will append it at the next iteration
                        continue;
                    }
                }
                else {
                    //
                    // we do not have space separator, or
                    // we got here on the next iteration, after
                    // we found space separated option.
                    // We have the following cases to handle here:
                    // 1) normal case "-option=value"
                    // 2) "-option="
                    // 3) "="
                    // 4) "=123"
                    // 5) any string , e.g. argument list
                    //
                    if (!arguments[i].endsWith("=")) {
                        //
                        // if we do not have "=" at the end, append space,
                        // this is "complete" case ((1) (4) (5))
                        //
                        sb.append(arguments[i]).append(" ");
                    }
                    else {
                        //
                        // if we have "exposed" "=" then we
                        // have incomplete case ((2) (3))
                        //
                        sb.append(arguments[i]);
                    }
                }
                i++;
            }
        }
        //
        // we concatenated argument list into String
        // having (hopefully) converted things like:
        // "-option 123 -option=123 -option= 123 -option =123 -option = 123"
        // into
        // "-option=123 -option=123 -option=123 -option=123 -option=123"
        // and use another convenient constructor
        // of Args to continue business as usual
        //
        Args args = new Args(sb.toString());
        //
        // Set all fields to default values
        //
        OptionParser.setDefaults(this);
        //
        // Need to parse "conf" option first, so we set the value
        // of config_file field. Make sure "conf" options is specified.
        //
        if (args.hasOption("conf")) {
            OptionParser.parseOption(this,"conf",args);
        }
        else {
            config_file=DEFAULT_CONFIG_FILE;
        }
        File f = new File(config_file);
        if (f.exists() && f.canRead()) {
            read(config_file);
        }
        else {
            if (args.hasOption("conf")) {
                throw new IOException("specified configuratioin file \""+config_file+"\" does not exist or can not be read");
            }
        }
        //
        // Now parse only specified options, so we achieve a situation where
        // class fields are set to defaults, then possibly overriden by
        // values from config file and then possibly overriden by command
        // line options.
        //
        OptionParser.parseSpecifiedOptions(this,args);
        extraParameters.put("priority",priority.toString());
        //
        // take care of normal people who tend to specify range as MIN:MAX
        //
        if (globus_tcp_port_range!=null) {
            globus_tcp_port_range=globus_tcp_port_range.replace(':',',');
        }
        if (is_help) {
            help=true;
        }

        if (retry_timeout <= 0) {
            throw new IllegalArgumentException("illegal retry timeout : "+
                    retry_timeout);
        }
        if(retry_num < 0) {
            throw new IllegalArgumentException("illegal number of retries : "+
                    retry_num);
        }
        if (isSrmv1&&isSrmv2) {
            throw new IllegalArgumentException(
                    "only one option of -srm_protocol_version, -1 or -2 should be specified");
        }
        if ((isSrmv1||isSrmv2) && args.hasOption("srm_protocol_version")) {
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
            srm_protocol_version=2;
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
                    "\n only one of the following options must be " +
                    "specified:\n\n" + usage());
        }

        int numberOfArguments = args.argc();

        if (numberOfArguments==0 && copyjobfile==null) {
            throw new IllegalArgumentException("Please specify command line arguments\n"+usage());
        }

        surls = args.getArguments().toArray(new String[numberOfArguments]);
        srmUrl=args.argv(0);

        //
        // take care of protocol version for srm v2.2 functions
        // (override whatever user has specified)
        //
        if (is_AbortRequest||
                reserveSpace ||
                checkPermission ||
                is_AbortFiles ||
                is_rm ||
                is_mv ||
                setPermission ||
                getSpaceTokens ||
                is_ReleaseFiles ||
                is_mkdir ||
                getSpaceMetaData ||
                ls ||
                bringOnline ||
                releaseSpace ||
                extendFileLifetime ||
                is_getRequestTokens ||
                getPermission ||
                is_getRequestSummary ||
                is_rmdir) {
            srm_protocol_version=2;
        }
        if (getRequestStatus) {
            if (numberOfArguments == 1) {
                getRequestStatusSurl = args.argv(0);
            }
            else {
                throw new IllegalArgumentException(
                        "one and only one storage element info server " +
                "wsdl url should be specified");
            }
        }
        else if (is_getRequestTokens) {
            getRequestStatusSurl =  args.argv(0);
        }
        else if (is_getRequestSummary) {
            getRequestStatusSurl =  args.argv(0);
            arrayOfRequestTokens=readListOfOptions(requestTokens,",");
        }
        else if (is_AbortRequest) {
            arrayOfRequestTokens=readListOfOptions(requestTokens,",");
        }
        else if (is_AbortFiles || is_ReleaseFiles ) {
            arrayOfRequestTokens=readListOfOptions(requestTokens,",");
        }
        else if (releaseSpace) {
            OptionParser.checkNullOptions(this,"space_token");
        }
        else if (reserveSpace) {
            protocols = readListOfOptions(protocols_list,",");
            arrayOfClientNetworks = readListOfOptions(array_of_client_networks,",");
            OptionParser.checkNullOptions(this,"retention_policy","guaranteed_size");
        }
        else if (getSpaceMetaData) {
            spaceTokensList=readListOfOptions(space_tokens_list,",");
            OptionParser.checkNullOptions(this,"space_tokens");
        }
        else if (copy||is_mv) {
            if (copy) {
                readCopyOptions();
            }
            if (copyjobfile == null) {
                if (numberOfArguments >= 2) {
                    if ( is_mv && numberOfArguments > 2 ) {
                        throw new IllegalArgumentException(
                                "one source and one destination " +
                        "should be specified");
                    }
                    int number_of_sources = numberOfArguments - 1;
                    from = args.getArguments().subList(0, number_of_sources).
                            toArray(new String[number_of_sources]);
                    to  = args.argv(number_of_sources);
                }
                else {
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
                    throw new IllegalArgumentException(
                            "no source or destination should be specified when " +
                    "using copyjobfile");
                }
            }
        }
        else if(bringOnline){
            protocols = readListOfOptions(protocols_list,",");
        }
        else if(ping) {
            srmUrl = args.argv(0);
        }
        else if (setPermission) {
            setPermissionSurl = args.argv(0);
            OptionParser.checkNullOptions(this,"type");
        }
        else {
        }
    }

    private void readCopyOptions()
    {
        protocols = readListOfOptions(protocols_list,",");
        arrayOfClientNetworks = readListOfOptions(array_of_client_networks,",");
        if (spaceToken!=null) {
            srm_protocol_version = 2;
        }
        readCksmOptions();
    }

    private String[] readListOfOptions(String option,
                                       String separator)
    {
        String[] listOfOptions = null;
        if (option != null) {
            listOfOptions=option.split(separator);
        }
        return listOfOptions;
    }

    private void readCksmOptions()
    {
        if ( this.cksm_type == null && this.cksm_value != null ) {
            this.cksm_type = "adler32";
        }
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
                for (Class<?> c = getClass(); c != null; c = c.getSuperclass()) {
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
                        catch (SecurityException | IllegalAccessException e) {
                            throw new
                            RuntimeException("Bug detected while processing option " +
                                    option.name(), e);
                        } catch (Exception e) {
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

    private String printMandatoryOptions(String ... names) {
        StringBuilder sb = new StringBuilder();
        sb.append("\nmandatory options : ");
        for(String s:names) {
            sb.append("\"").append(s).append("\" ");
        }
        return sb.toString();
    }

    public void write(String file) throws Exception {
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        DocumentBuilder db         = dbf.newDocumentBuilder();
        Document document          = db.newDocument();
        Element root               = document.createElement("srm-configuration");
        for (Class<?> c = getClass(); c != null; c = c.getSuperclass()) {
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
                            if (description.length() == 0) {
                                description = option.name();
                            }
                            StringBuilder sb = new StringBuilder();
                            sb.append(description.replaceAll("\n"," "));
                            if (option.defaultValue().length()>0) {
                                sb.append(", default is ")
                                        .append(option.defaultValue());
                            }
                            if (unit.length()>0) {
                                sb.append(" (").append(unit).append(")");
                            }
                            put(document,root,option.name(),svalue,sb.toString());
                        }
                    }
                }
                catch (SecurityException | IllegalAccessException e) {
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

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("SRM Configuration:");
        for (Class<?> c = getClass(); c != null; c = c.getSuperclass()) {
            for (Field field : c.getDeclaredFields()) {
                Option option = field.getAnnotation(Option.class);
                try {
                    if (option != null) {
                        if (option.log()) {
                            field.setAccessible(true);
                            Object value = field.get(this);
                            sb.append("\n\t").append(option.name()).append("=")
                                    .append(value);
                        }
                    }
                }
                catch (SecurityException | IllegalAccessException e) {
                    throw new RuntimeException("Bug detected while processing option " + option.name(), e);
                }
            }
        }
        if (getFileMetaData && surls != null) {
            sb.append("\n\taction is getFileMetaData");
            for (int i = 0; i<surls.length; ++i) {
                sb.append("\n\tsurl[").append(i).append("]=")
                        .append(this.surls[i]);
            }
        }
        if (getSpaceTokens && srmUrl != null) {
            sb.append("\n\taction is getSpacetokens");
            sb.append("\n\tsurl=").append(this.srmUrl);
        }
        if (getPermission && surls != null) {
            sb.append("\n\taction is getPermissions");
            for (int i = 0; i<surls.length; ++i) {
                sb.append("\n\tsurl[").append(i).append("]=")
                        .append(this.surls[i]);
            }
        }
        if (checkPermission &&  surls != null) {
            sb.append("\n\taction is checkPermissions");
            for (int i = 0; i<surls.length; ++i) {
                sb.append("\n\tsurl[").append(i).append("]=")
                        .append(this.surls[i]);
            }
        }
        if (setPermission &&  setPermissionSurl != null) {
            sb.append("\n\taction is setPermissions");
            sb.append("\n\tsur=").append(this.setPermissionSurl);
        }
        if (extendFileLifetime && surls != null) {
            sb.append("\n\taction is extendFileLifetime");
            for (int i = 0; i<surls.length; ++i) {
                sb.append("\n\tsurl[").append(i).append("]=")
                        .append(this.surls[i]);
            }
        }
        if (ls && surls != null) {
            sb.append("\n\taction is ls");
            for(int i = 0; i< surls.length; i++) {
                sb.append("\n\tsurl[").append(i).append("]=")
                        .append(this.surls[i]);
            }
        }
        if (is_rm && surls != null) {
            sb.append("\n\taction is rm");
            for(int i = 0; i< surls.length; i++) {
                sb.append("\n\tsurl[").append(i).append("]=")
                        .append(this.surls[i]);
            }
        }
        if (is_rmdir && surls != null) {
            sb.append("\n\taction is rmdir");
            for(int i = 0; i< surls.length; i++) {
                sb.append("\n\tsurl[").append(i).append("]=")
                        .append(this.surls[i]);
            }
        }
        if (is_mkdir && surls != null) {
            sb.append("\n\taction is mkdir");
            for(int i = 0; i< surls.length; i++) {
                sb.append("\n\tsurl[").append(i).append("]=")
                        .append(this.surls[i]);
            }
        }
        if (reserveSpace) {
            sb.append("\n\taction is reserveSpace");
            if ( surls != null ) {
                for (String surl : surls) {
                    sb.append("\n\tsrm url=").append(surl);
                }
            }
            sb.append("\n\tprotocols");
            for(int i = 0; i< protocols.length; i++) {
                sb.append("\n\tprotocol[").append(i).append("]=")
                        .append(this.protocols[i]);
            }
            sb.append("\n\tarray of client networks:");
            if (arrayOfClientNetworks==null){
                sb.append("\n\tnull");
            } else {
                for(int i = 0; i<arrayOfClientNetworks.length; i++) {
                    sb.append("\n\tnetwork[").append(i).append("]=")
                            .append(arrayOfClientNetworks[i]);
                }
            }
        }
        if (releaseSpace) {
            sb.append("\n\taction is releaseSpace");
            if ( surls != null ) {
                for (String surl : surls) {
                    sb.append("\n\tsrm url=").append(surl);
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
                sb.append("\n\tfrom[").append(i).append("]=")
                        .append(this.from[i]);
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
    public String[] getFrom() {
        return this.from;
    }

    /** Setter for property from.
     * @param from New value of property from.
     */
    public void setFrom(String[] from) {
        this.from = from;
    }

    /** Getter for property to.
     * @return Value of property to.
     */
    public String getTo() {
        return to;
    }

    /** Setter for property to.
     * @param to New value of property to.
     */
    public void setTo(String to) {
        this.to = to;
    }

    /** Getter for property logger.
     * @return Value of property logger.
     *
     */
    public Logger getLogger() {
        return logger;
    }

    /** Setter for property logger.
     * @param logger New value of property logger.
     *
     */
    public void setLogger(Logger logger) {
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
    public String[] getGetFileMetaDataSurls() {
        return this.surls;
    }

    /** Setter for property getFileMetaDataSurls.
     * @param getFileMetaDataSurls New value of property getFileMetaDataSurls.
     *
     */
    public void setGetFileMetaDataSurls(String[] getFileMetaDataSurls) {
        this.surls = getFileMetaDataSurls;
    }

    /** Getter for property getPermissionSurls.
     * @return Value of property getPermissionSurls.
     *
     */
    public String[] getGetPermissionSurls() {
        return this.surls;
    }

    /** Getter for property checkPermissionSurls.
     * @return Value of property checkPermissionSurls.
     *
     */
    public String[] getCheckPermissionSurls() {
        return this.surls;
    }

    public String[] getExtendFileLifetimeSurls() {
        return this.surls;
    }

    /** Getter for property setPermissionSurls.
     * @return Value of property setPermissionSurls.
     *
     */
    public String getSetPermissionSurl() {
        return this.setPermissionSurl;
    }

    /** Setter for property getPermissionSurls.
     * @param getPermissionSurls New value of property getPermissionSurls.
     *
     */
    public void setGetPermissionSurls(String[] getPermissionSurls) {
        this.surls = getPermissionSurls;
    }

    /** Setter for property checkPermissionSurls.
     * @param checkPermissionSurls New value of property checkPermissionSurls.
     *
     */
    public void setCheckPermissionSurls(String[] checkPermissionSurls) {
        this.surls = checkPermissionSurls;
    }

    /** Setter for property setPermissionSurls.
     * @param setPermissionSurls New value of property setPermissionSurls.
     *
     */
    public void setSetPermissionSurl(String setPermissionSurls) {
        this.setPermissionSurl = setPermissionSurls;
    }

    public void setExtendFileLifetimeSurls(String surls[]) {
        this.surls = surls;
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
    public String[] getAdvisoryDeleteSurls() {
        return this.surls;
    }

    /** Setter for property advisoryDeleteSurls.
     * @param advisoryDeleteSurls New value of property advisoryDeleteSurls.
     *
     */
    public void setAdvisoryDeleteSurls(String[] advisoryDeleteSurls) {
        this.surls = advisoryDeleteSurls;
    }

    /**
     * Getter for property getRequestStatusSurl.
     * @return Value of property getRequestStatusSurl.
     */
    public String getGetRequestStatusSurl() {
        return getRequestStatusSurl;
    }

    /**
     * Setter for property getRequestStatusSurl.
     * @param getRequestStatusSurl New value of property getRequestStatusSurl.
     */
    public void setGetRequestStatusSurl(String getRequestStatusSurl) {
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
        return Integer.parseInt(extraParameters.get("priority"));
    }

    public Map<String,String> getExtraParameters() {
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
