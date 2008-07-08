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
import java.util.*;


// for writing xml
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

/*
 *  apache xml specific
import org.apache.xml.serialize.OutputFormat;
import org.apache.xml.serialize.XMLSerializer;
import org.apache.xml.serialize.LineSeparator;
 
 */
/**
 *
 * @author  timur
 */
public class Configuration {
	public static final String SRMCPCONFIGNAMESPACE="srmcp.srm.fnal.gov";
	private boolean debug = false;
	private String srmcphome="..";
	private String urlcopy="sbin/urlcopy.sh";
	private String gsiftpclinet = "globus-url-copy";
	private boolean help=false;
	private boolean gsissl = true;
	private String glue_mapfile="conf/SRMServerV1.map";
	private String webservice_path =null;
	private String webservice_pathv1 = "srm/managerv1";
	private String webservice_pathv2 = "srm/managerv2";
	private String webservice_protocol="http";
	private boolean useproxy = true;
	private String x509_user_proxy="/home/timur/k5-ca-proxy.pem";
	private String x509_user_key="/home/timur/k5-ca-proxy.pem";
	private String x509_user_cert="/home/timur/k5-ca-proxy.pem";
	private String x509_user_trusted_certificates="/etc/grid-security/certificates";
	private String globus_tcp_port_range=null;
	private String gss_expected_name=null;
	private String protocols_list="http,gsiftp";
	private boolean pushmode=false;
	private int buffer_size=1024*128;
	private int tcp_buffer_size;
	private int streams_num=10;
	private String config_file = "config.xml";
	private String save_config_file;
	private Logger logger;
	private String[] protocols = new String[]   {"gsiftp","dcap","http"};
	private boolean doRemove=false;

	private boolean copy=false;
	private boolean bringOnline=false;
	private boolean ping=false;
	
	//
	// SrmReserveSpace parameters
	//
    
	private boolean reserveSpace=false;
	private String[] arrayOfClientNetworks;
	private String retentionPolicy=null;
	private String spaceTokenDescription;
	private String accessLatency=null;
	private String accessPattern="TRANSFER_MODE";
	private String connectionType="WAN";
	private long desiredReserveSpaceSize=0;
	private long guaranteedReserveSpaceSize=0;
	private long desiredLifetime=0;
    
	//
	// SrmReleaseSpace parameters
	//
    
	private boolean releaseSpace=false;
	private String spaceToken;
	private boolean forceFileRelease=false;
	//
	// SrmGetSpaceMetaData parameters
	private boolean getSpaceMetaData=false;

	private String[] spaceTokensList;
	private String[] from;
	private String to;
	private String copyjobfile;
	private String wsdl_url;
	private boolean use_urlcopy_script=false;
	private boolean getFileMetaData=false;
	private boolean ls=false;

	private boolean getSpaceTokens=false;
	private boolean is_rm=false;
	private boolean is_rmdir=false;
	private boolean is_mv=false;
	private boolean is_mkdir=false;
	private boolean getPermission=false;
	private boolean checkPermission=false;
	private boolean setPermission=false;
	private boolean is_getRequestSummary=false;
	private boolean is_getRequestTokens=false;
	private boolean is_AbortFiles=false;
	private boolean is_ReleaseFiles=false;
	private String  userRequestDescription=null;

	private String setPermissionType=null;
	private String setOwnerPermissionMode=null;
	private String setOtherPermissionMode=null;
	private String setGroupPermissionMode=null;
	
	private String setPermissionSurl;
	//
	// srmExtendFileLifeTime parameters
	//
	private String srmExtendFileLifetimeRequestToken=null;
	private Integer  newFileLifetime=null;
	private Integer  newPinLifetime=null;
	private boolean extendFileLifetime=false;
    
	private boolean advisoryDelete=false;
	private boolean getRequestStatus=false;
	private String getRequestStatusSurl;
	private int getRequestStatusId;
	private boolean getStorageElementInfo=false;
	private String storageElementInfoServerWSDL;
	private long retry_timeout=10000;
	private int retry_num=20;
	private boolean connect_to_wsdl = false;
	private boolean delegate = true;
	private boolean full_delegation = true;
	private boolean version = false;
	private String report=null;
	private boolean passive_server_mode=true;
	private String storagetype="permanent";
	private boolean stage = false;
	private boolean longLsFormat=false;
	private int recursionDepth=0;
	private boolean recursive=false;
	private int lsOffset=0;
	private int lsCount=0;
	private int srm_protocol_version=1;
	private long request_lifetime=60*60*24; //default request desiredLifetime in seconds
	private Map extraParameters;
	private String overwriteMode;

	// checksum options
	private String cksm_type = null;
	private String cksm_value = null;

	private String arrayOfRequestTokens[] = null;
	boolean is_AbortRequest=false;

	private String srmUrl;
	private String surls[];

	/** Creates a new instance of Configuration */
	public Configuration() {
		extraParameters = new HashMap();
		extraParameters.put("priority","0");
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

	
	public   final String usage() {
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
		ArgParser parser = new ArgParser(args);
		parser.addBooleanOption(null,"force","force space reservation release, default is false");
		parser.addStringOption(null,"space_token","space reservation token, default is null");
		parser.addStringOption(null,"space_tokens","space reservation tokens, default is null");
		parser.addStringOption(null,"request_tokens","comma separated list of request tokens, default is null");
		parser.addStringOption(null,"do_remove","remove files when executing srm-release-files");
		parser.addStringOption(null,"space_desc","space reservation description, default is null");
		parser.addStringOption(null,"retention_policy","retention policy, default is null");
		parser.addStringOption(null,"access_pattern","access pattern, default is \"TRANSFER_MODE\"");
		parser.addStringOption(null,"access_latency","access latency, default is null");
		parser.addStringOption(null,"overwrite_mode","overwrite mode, default is null");
		parser.addStringOption(null,"array_of_client_networks","array of client networks");
		parser.addStringOption(null,"connection_type","connection type, default is \"WAN\"");
		parser.addStringOption(null,"desired_size","desired space reservation size in Bytes");
		parser.addStringOption(null,"lifetime","desired lifetime or space reservation lifetime in seconds");
		parser.addStringOption(null,"guaranteed_size","guaranteed space reservation size in Bytes");
		parser.addBooleanOption(null,"version","print debug info if debug is true");
		parser.addStringOption(null,"srmcphome","path to the srmcp product directory");
		parser.addStringOption(null,"gsissl","perform srm soap invocations over httpg (http over gsi ssl)");
		parser.addVoidOption(null,"help","prints usage");
		parser.addVoidOption(null,"h","prints usage");
		parser.addBooleanOption(null,"debug","print debug info if debug is true");
		parser.addBooleanOption(null,"pushmode","sets srm in a \"push\" mode");
		parser.addStringOption(null,"mapfile","path to the soap map file");
		parser.addStringOption(null,"webservice_path","the path to the wsdl in the"+
				       " web service url");
		parser.addStringOption(null,"webservice_protocol",
				       "the protocol on which srm web service is published"+
				       "(usually http, https or httpg) ");
		parser.addStringOption(null,"protocols",
				       "the alternative supported TURL protocols list");
		parser.addStringOption(null,"urlcopy",
				       "the absolute path to the url_copy script");
		parser.addIntegerOption(null,"buffer_size",
					"set the buffer size to a value different then default(2048)",0,
					Integer.MAX_VALUE);
		parser.addIntegerOption(null,"streams_num",
					"set the number of streams used by gridftp",1,
					Integer.MAX_VALUE);
		parser.addIntegerOption(null,"tcp_buffer_size",
					"set the tcp buffer size to a value different then default",0,
					Integer.MAX_VALUE);
		parser.addBooleanOption(null,"use_proxy","true if srmcp should use proxy,"+
					" false if use certificate and key directly, defult value is true");
		parser.addStringOption(null,"x509_user_proxy","path to the user proxy");
		parser.addStringOption(null,"x509_user_key","path to the user private key");
		parser.addStringOption(null,"x509_user_cert","path to the user cert");
		parser.addStringOption(null,"x509_user_trusted_certificates","path to the trusted certificates directory");
		parser.addStringOption(null,"globus_tcp_port_range","range of ports open for tcp connection");
		parser.addStringOption(null,"gss_expected_name","gss expected name in the srm server DN");
		parser.addStringOption(null,"storagetype","permanent|durable|volatile;\"permanent\" by default");
		parser.addStringOption(null,"conf","path to the configuration file");
		parser.addStringOption(null,"save_conf",
				       "path to the file in which the new configuration will be saved\n"+
				       "no transfer will be performed if this option is specified");
		parser.addStringOption(null,"copyjobfile","path to the copy job file");
		parser.addStringOption(null,"wsdl_url","full url to web service wsdl");
		parser.addBooleanOption(null,"use_urlcopy_script", "true or false");
		parser.addVoidOption(null,"ls","performs srm ls");
		parser.addVoidOption(null,"stage","performs srm stage");
		parser.addVoidOption(null,"rm","performs srm rm");
		parser.addVoidOption(null,"rmdir","performs srm rmdir");
		parser.addVoidOption(null,"mv","performs srm mv");
		parser.addVoidOption(null,"mkdir","performs srm mkdir");
		parser.addVoidOption(null,"copy","performs srm \"get\", \"put\" or \"copy\"");
		parser.addVoidOption(null,"bringOnline","performs srmBringOnline");
		parser.addVoidOption(null,"reserveSpace","performs explicit space reservation");
		parser.addVoidOption(null,"releaseSpace","performs release of explicit space reservation");
		parser.addVoidOption(null,"getSpaceMetaData", "prints space metadata for space tokens");
		parser.addVoidOption(null,"getPermissions","gets file(s) permissions");
		parser.addVoidOption(null,"checkPermissions","checks file(s) permissions");
		parser.addVoidOption(null,"setPermissions","sets file(s) permissions");
		parser.addVoidOption(null,"extendFileLifetime","exend file(s) lifetimes");
		parser.addVoidOption(null,"getFileMetaData","gets FileMetaData");
		parser.addVoidOption(null,"getSpaceTokens","gets space tokens");
		parser.addVoidOption(null,"getStorageElementInfo", "gets StorageElementInfo");
		parser.addVoidOption(null,"advisoryDelete", "performs AdvisoryDelete");
		parser.addVoidOption(null,"getRequestStatus", "gets Request Status");
		parser.addVoidOption(null,"getRequestSummary", "retrieve a summary of the previously submitted request.");
		parser.addVoidOption(null,"abortRequest", "abort request.");
		parser.addVoidOption(null,"abortFiles", "abort files.");
		parser.addVoidOption(null,"releaseFiles", "unpin files.");
		parser.addVoidOption(null,"getRequestTokens", "retrieves request tokens for the client\u2019s requests, given client provided request description.");
		parser.addVoidOption(null,"ping", "pings srm");
		parser.addIntegerOption(null,"retry_timeout",
					"number of milliseconds to sleep after a failure before the client tries to retry", 1,
					Integer.MAX_VALUE);
		parser.addIntegerOption(null,"retry_num",
					"number of retries before client gives up",0,
					Integer.MAX_VALUE);
		parser.addStringOption(null,"connect_to_wsdl","connect to wsdl instead of "+
				       " connecting to the server directly");
		parser.addBooleanOption(null,"delegate",
					"enables or disables the delegation of the user credenital to the server");
		parser.addBooleanOption(null,"full_delegation",
					"specifies type (full or limited) of delegation delegation");
		parser.addStringOption(null,"report",
				       "specifies path to the repport file");
		parser.addStringOption(null, "server_mode", "active or passive");
		//
		// permission options
		//
		parser.addStringOption(null, "type", "permission type");
		parser.addStringOption(null, "owner", "owner permission mode");
		parser.addStringOption(null, "other", "world permission mode");
		parser.addStringOption(null, "group", "group permission mode");
		//
		// srmls options
		//
		parser.addBooleanOption(null, "l", "enables long format mode for srmls");
		parser.addIntegerOption(null,"recursion_depth",
					"controls how deep to descend into direcotory trees",
					0,Integer.MAX_VALUE);
		parser.addBooleanOption(null, "recursive", "makes srmrmdir recursive");
		parser.addIntegerOption(null,"offset",
					"controls from what element to start printing",
					0,Integer.MAX_VALUE);
		parser.addIntegerOption(null,"count",
					"controls how many elements to print starting from offset",
					0,Integer.MAX_VALUE);
		//
		// extendFileLifetime options 
		//
		parser.addIntegerOption(null,"pin_lifetime",
					"number of seconds to add to current time to extend pin lifetime of TURL(s)",0,
					Integer.MAX_VALUE);
	
		parser.addIntegerOption(null,"file_lifetime",
					"number of seconds to add to current time to extend file lifetime of SURL(s)",0,
					Integer.MAX_VALUE);
	
		parser.addStringOption(null, "request_token", "request token");
		parser.addIntegerOption(null,"srm_protocol_version","1 for srm 1.1 or 2 for srm 2.1.1, no other protocols are supported ",1,2);
		parser.addBooleanOption(null,"1", "srm version 1.1 is used if true");
		parser.addBooleanOption(null,"2", "srm version 2.2 is used if true");
		parser.addIntegerOption(null,"request_lifetime","request lifetime in seconds",-1,Integer.MAX_VALUE);
		parser.addIntegerOption(null,"priority","specify request priority, 0 is lowest",
					0,Integer.MAX_VALUE);
  
		// checksum options
		parser.addStringOption(null,"cksm_type", "instructs calculation and verification of the server and client checksum values using specific type. If value has not been provided , client side checksum will be used.");
		parser.addStringOption(null,"cksm_value", "instructs calculation and verification of the server and client checksums using provided value. If cksm_type has not been set adler32 is assumed");

		parser.parse();
		if (parser.isOptionSet(null,"conf") ) {
			this.config_file = parser.stringOptionValue(null,"conf");
		}
        
		File f = new File(config_file);
		if (f.exists() && f.canRead()) {
			read(config_file);
		}
        
		readGeneralOptions(parser);
        
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
        
		String [] arguments = parser.getCommandArguments();

		if (arguments==null && copyjobfile==null) { 
			usage();
			throw new IllegalArgumentException("Please specify command line arguments\n"+usage());
			 
		}
		if (arguments.length==0 && copyjobfile==null) { 
			usage();
			throw new IllegalArgumentException("Please specify command line arguments\n"+usage());
		}

		if (getFileMetaData) {
			surls = arguments;
		}
		else if (advisoryDelete) {
			surls = arguments;
		} 
		else if (getRequestStatus) {
			if (arguments != null && arguments.length == 2) {
				getRequestStatusSurl = arguments[0];
				getRequestStatusId = Integer.parseInt(arguments[1]);
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
			getRequestStatusSurl = arguments[0];
			if (parser.isOptionSet(null,"request_desc")) {
				userRequestDescription = parser.stringOptionValue(null,"request_desc");
			}
		}
		else if (is_getRequestSummary) {
			getRequestStatusSurl = arguments[0];
			arrayOfRequestTokens=readListOfOptions(parser,"request_tokens");
		}
		else if (is_AbortRequest) {
			srmUrl = arguments[0];
			arrayOfRequestTokens=readListOfOptions(parser,"request_tokens");
		}
		else if (is_AbortFiles) {
			srmUrl=arguments[0];
			if(arguments.length>1) { 
				surls=arguments;
			}
			else { 
				srmUrl = arguments[0];
			}
			arrayOfRequestTokens=readListOfOptions(parser,"request_tokens");
			if(arrayOfRequestTokens!=null&&surls!=null) { 
				throw new IllegalArgumentException(
					"specify request token or list of surls (exclusively)");
			}
		}
		else if (is_ReleaseFiles) {
			srmUrl=arguments[0];
			if(arguments.length>1) { 
				surls=arguments;
			}
			else { 
				srmUrl = arguments[0];
			}
			if (parser.isOptionSet(null,"do_remove")) {
				doRemove = parser.booleanOptionValue(null,"do_remove");
			}
			arrayOfRequestTokens=readListOfOptions(parser,"request_tokens");
			if(arrayOfRequestTokens!=null&&surls!=null) { 
				throw new IllegalArgumentException(
					"specify request token or list of surls (exclusively)");
			}
		}
		else if (getStorageElementInfo) {
			if (arguments != null && arguments.length == 1) {
				storageElementInfoServerWSDL = arguments[0];
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
			readReserveSpaceOptions(parser);
			surls = arguments;
		} 
		else if (releaseSpace) {
			srm_protocol_version =2;
			readReleaseSpaceOptions(parser);
			surls = arguments;
		} 
		else if (getSpaceMetaData) {
			srm_protocol_version =2;
			spaceTokensList=readListOfOptions(parser,"space_tokens");
			srmUrl = arguments[0];
		} 
		else if(getSpaceTokens) {
			srm_protocol_version =2;
			readGetSpaceTokensOptions(parser);
			srmUrl = arguments[0];
		} 
		else if (copy||is_mv) {
			if (copy) {
				if(parser.isOptionSet(null,"priority")) {
					int jobPriority = parser.intOptionValue(null,"priority");
					if(jobPriority < 0) {
						throw new IllegalArgumentException("illegal value for job priority"+
										   jobPriority);
					}
					extraParameters.put("priority",Integer.toString(jobPriority));
				}
				readCopyOptions(parser);
			} 
			else if (is_mv) {
				srm_protocol_version =2;
				if (parser.isOptionSet(null, "copyjobfile")) {
					copyjobfile=parser.stringOptionValue(null,"copyjobfile");
				}
			}
			if (copyjobfile == null) {
				if (arguments != null && arguments.length >= 2) {
					if ( is_mv && arguments.length > 2 ) {
						throw new IllegalArgumentException(
							"one source and one destination " +
							"should be specified");
					}
					int number_of_sources = arguments.length - 1;
					from = new String[number_of_sources];
					System.arraycopy(arguments,0,from,0,number_of_sources);
					to  = arguments[number_of_sources];
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
				if (arguments != null && arguments.length > 0) {
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
			readBringOnlineOptions(parser);
			surls = arguments;
			if(parser.isOptionSet(null,"priority")) {
				int jobPriority = parser.intOptionValue(null,"priority");
				if(jobPriority < 0) {
					throw new IllegalArgumentException("illegal value for job priority"+
									   jobPriority);
				}
				extraParameters.put("priority",Integer.toString(jobPriority));
			}
		} 
		else if(ping){
			readBringOnlineOptions(parser);
			srmUrl = arguments[0];
		} 
		else if (ls) {
			srm_protocol_version =2;
			readLsOptions(parser);
			surls = arguments;
		} 
		else if (is_rm ) {
			srm_protocol_version =2;
			readRmOptions(parser);
			surls = arguments;
		} 
		else if ( is_rmdir ) {
			srm_protocol_version =2;
			readRmdirOptions(parser);
			surls = arguments;
		} 
		else if ( is_mkdir ) {
			srm_protocol_version =2;
			surls = arguments;
		} 
		else if (stage) {
			srm_protocol_version =2;
			surls = arguments;
		} 
		else if (getPermission) {
			srm_protocol_version =2;
			surls = arguments;
		} 
		else if (checkPermission) {
			srm_protocol_version =2;
			surls = arguments;
		} 
		else if (setPermission) {
			srm_protocol_version =2;
			readSetPermissionOptions(parser);
			setPermissionSurl = arguments[0];
		} 
		else if (extendFileLifetime) { 
			srm_protocol_version =2;
			readExtendFileLifetimeOptions(parser);
			surls= arguments;
		} 
		else {
			if (version) {
				System.exit(1);
			}
			throw new IllegalArgumentException("should not be here");
		}
	}
    
	private void readGeneralOptions(ArgParser parser) throws Exception {
		if (parser.isOptionSet(null,"srmcphome") ) {
			this.srmcphome = parser.stringOptionValue(null,"srmcphome");
		}
		if (parser.isOptionSet(null,"save_conf") ) {
			this.save_config_file = parser.stringOptionValue(null,"save_conf");
		}
		if (parser.isOptionSet(null,"h") || parser.isOptionSet(null,"help")) {
			this.help = true;
		}
		if (parser.isOptionSet(null,"gsissl")) {
			this.gsissl = parser.booleanOptionValue(null,"gsissl");
		}
		if (gsissl) {
			this.webservice_protocol ="https";
		} else {
			this.webservice_protocol ="http";
		}
		if (parser.isOptionSet(null,"connect_to_wsdl")) {
			connect_to_wsdl = parser.booleanOptionValue(null,"connect_to_wsdl");
		}
		if (parser.isOptionSet(null,"delegate")) {
			delegate = parser.booleanOptionValue(null,"delegate");
		}
		if (parser.isOptionSet(null,"full_delegation")) {
			full_delegation = parser.booleanOptionValue(null,"full_delegation");
		}
		if (parser.isOptionSet(null,"mapfile")) {
			glue_mapfile = parser.stringOptionValue(null,"mapfile");
		}
		if (parser.isOptionSet(null,"wsdl_url")) {
			this.wsdl_url = parser.stringOptionValue(null,"wsdl_url");
		} else {
			if (parser.isOptionSet(null,"webservice_path")) {
				webservice_path = parser.stringOptionValue(null,"webservice_path");
			}
			if (parser.isOptionSet(null,"webservice_protocol")) {
				webservice_protocol =parser.stringOptionValue(null,"webservice_protocol");
			}
		}
		if (parser.isOptionSet(null,"use_proxy")) {
			useproxy = parser.booleanOptionValue(null,"use_proxy");
		}
		if (parser.isOptionSet(null,"x509_user_proxy")) {
			x509_user_proxy =parser.stringOptionValue(null,"x509_user_proxy");
		}
		if (parser.isOptionSet(null,"x509_user_cert")) {
			x509_user_cert =parser.stringOptionValue(null,"x509_user_cert");
		}
		if (parser.isOptionSet(null,"x509_user_key")) {
			x509_user_key =parser.stringOptionValue(null,"x509_user_key");
		}
		if (parser.isOptionSet(null,"x509_user_trusted_certificates")) {
			x509_user_trusted_certificates =parser.stringOptionValue(null,
										 "x509_user_trusted_certificates");
		}
		if (parser.isOptionSet(null,"globus_tcp_port_range")) {
			globus_tcp_port_range =parser.stringOptionValue(null,"globus_tcp_port_range");
		}
		if (parser.isOptionSet(null,"gss_expected_name")) {
			gss_expected_name =parser.stringOptionValue(null,"gss_expected_name");
		}
		if (parser.isOptionSet(null,"retry_timeout")) {
			retry_timeout = (long)parser.intOptionValue(null,"retry_timeout");
			if (retry_timeout <= 0) {
				throw new IllegalArgumentException("illegal retry timeout : "+
								   retry_timeout);
			}
		}
		if (parser.isOptionSet(null,"retry_num")) {
			retry_num = parser.intOptionValue(null,"retry_num");
			if(retry_num < 0) {
				throw new IllegalArgumentException("illegal number of retries : "+
								   retry_num);
			}
		}
		if(parser.isOptionSet(null,"version")) {
			version = parser.booleanOptionValue(null,"version");
		}
		if(parser.isOptionSet(null,"debug")) {
			debug = parser.booleanOptionValue(null,"debug");
		}
		int num_ver_set = (parser.isOptionSet( null,"srm_protocol_version")?1:0) +
			(parser.isOptionSet( null,"1")?1:0) +
			(parser.isOptionSet( null,"2")?1:0);
		if(num_ver_set >1) {
			throw new IllegalArgumentException(
				"only one option of -srm_protocol_version, -1 or -2 should be specified");
		}
        
        
		if(parser.isOptionSet(null,"srm_protocol_version") ) {
			srm_protocol_version = parser.intOptionValue(null,"srm_protocol_version");
			if(srm_protocol_version != 1 && srm_protocol_version != 2) {
				throw new IllegalArgumentException("illegal srm protocol version "+ srm_protocol_version);
			}
		}
		if(parser.isOptionSet(null,"1") ) {
			srm_protocol_version = parser.booleanOptionValue(null,"1")?1:2;
		}
		if(parser.isOptionSet(null,"2") ) {
			srm_protocol_version = parser.booleanOptionValue(null,"2")?2:1;
		}
		if(parser.isOptionSet(null,"request_lifetime")) {
			request_lifetime = (long)parser.intOptionValue(null,"request_lifetime");
			if(request_lifetime <= 0 && request_lifetime != -1) {
				throw new IllegalArgumentException("illegal value for request lifetime"+
								   request_lifetime);
			}
		}
		copy                  = parser.isOptionSet(null, "copy");
		bringOnline           = parser.isOptionSet(null, "bringOnline");
		getFileMetaData       = parser.isOptionSet(null, "getFileMetaData");
		getStorageElementInfo = parser.isOptionSet(null, "getStorageElementInfo");
		advisoryDelete        = parser.isOptionSet(null, "advisoryDelete");
		getRequestStatus      = parser.isOptionSet(null, "getRequestStatus");
		is_getRequestSummary  = parser.isOptionSet(null, "getRequestSummary");
		is_getRequestTokens   = parser.isOptionSet(null, "getRequestTokens");
		is_AbortRequest       = parser.isOptionSet(null, "abortRequest");
		is_AbortFiles         = parser.isOptionSet(null, "abortFiles");
		is_ReleaseFiles        = parser.isOptionSet(null, "releaseFiles");
		ls                    = parser.isOptionSet(null, "ls");
		is_rm                 = parser.isOptionSet(null, "rm");
		is_mv                 = parser.isOptionSet(null, "mv");
		is_rmdir              = parser.isOptionSet(null, "rmdir");
		is_mkdir              = parser.isOptionSet(null, "mkdir");
		stage                 = parser.isOptionSet(null, "stage");
		getPermission	      = parser.isOptionSet(null, "getPermissions");
		checkPermission	      = parser.isOptionSet(null, "checkPermissions");
		setPermission	      = parser.isOptionSet(null, "setPermissions");
		extendFileLifetime    = parser.isOptionSet(null, "extendFileLifetime");
		reserveSpace	      = parser.isOptionSet(null, "reserveSpace");
		releaseSpace	      = parser.isOptionSet(null, "releaseSpace");
		getSpaceMetaData      = parser.isOptionSet(null, "getSpaceMetaData");
		getSpaceTokens        = parser.isOptionSet(null, "getSpaceTokens");
		ping                  = parser.isOptionSet(null, "ping");

		if (parser.isOptionSet(null, "copyjobfile")) {
			copyjobfile=parser.stringOptionValue(null,"copyjobfile");
		}
	}
    
	private void readCopyOptions(ArgParser parser) throws Exception {
		if (parser.isOptionSet(null,"pushmode")) {
			pushmode = parser.booleanOptionValue(null,"pushmode");
		}
		if (parser.isOptionSet(null,"use_urlcopy_script")) {
			this.use_urlcopy_script = parser.booleanOptionValue(null,"use_urlcopy_script");
		}
		if (parser.isOptionSet(null,"urlcopy")) {
			urlcopy =parser.stringOptionValue(null,"urlcopy");
		}
		if (parser.isOptionSet(null,"buffer_size")) {
			buffer_size = parser.intOptionValue(null,"buffer_size");
			if (buffer_size <= 0) {
				throw new IllegalArgumentException("illegal buffer size: "+
								   buffer_size);
			}
		}
		if (parser.isOptionSet(null,"tcp_buffer_size")) {
			tcp_buffer_size = parser.intOptionValue(null,"tcp_buffer_size");
			if(tcp_buffer_size < 0) {
				throw new IllegalArgumentException("illegal buffer size: "+
								   tcp_buffer_size);
			}
		}
		if (parser.isOptionSet(null,"streams_num")) {
			streams_num = parser.intOptionValue(null,"streams_num");
			if (streams_num < 1) {
				throw new IllegalArgumentException("illegal number of streams: "+
								   streams_num);
			}
		}
		if (parser.isOptionSet(null,"protocols")) {
			this.protocols_list = parser.stringOptionValue(null,"protocols");
			java.util.StringTokenizer st = new java.util.StringTokenizer(
				protocols_list,",");
			protocols = new String[st.countTokens()];
			for (int i = 0;st.hasMoreTokens();++i) {
				protocols[i] = st.nextToken();
			}
		}
        
		if (parser.isOptionSet(null, "report")) {
			report=parser.stringOptionValue(null,"report");
		}
		if(parser.isOptionSet(null,"server_mode")) {
			passive_server_mode = parser.stringOptionValue(null,"server_mode").equalsIgnoreCase("passive");
		}
		if(parser.isOptionSet(null,"storagetype")) {
			storagetype=parser.stringOptionValue(null,"storagetype");
		}
		if (parser.isOptionSet(null,"space_token")) {
			spaceToken = parser.stringOptionValue(null,"space_token");
		}
		if (parser.isOptionSet(null,"retention_policy")) {
			retentionPolicy = parser.stringOptionValue(null,"retention_policy");
		}
		if (parser.isOptionSet(null,"access_latency")) {
			accessLatency = parser.stringOptionValue(null,"access_latency");
		}
		if (parser.isOptionSet(null,"overwrite_mode")) {
			overwriteMode = parser.stringOptionValue(null,"overwrite_mode");
		}
        
		readCksmOptions(parser);
        
	}
    
	private void readBringOnlineOptions(ArgParser parser) throws Exception {
		if (parser.isOptionSet(null,"protocols")) {
			this.protocols_list = parser.stringOptionValue(null,"protocols");
			java.util.StringTokenizer st = new java.util.StringTokenizer(
				protocols_list,",");
			protocols = new String[st.countTokens()];
			for (int i = 0;st.hasMoreTokens();++i) {
				protocols[i] = st.nextToken();
			}
		}
        
		if (parser.isOptionSet(null, "report")) {
			report=parser.stringOptionValue(null,"report");
		}
		if(parser.isOptionSet(null,"storagetype")) {
			storagetype=parser.stringOptionValue(null,"storagetype");
		}
		if (parser.isOptionSet(null,"space_token")) {
			spaceToken = parser.stringOptionValue(null,"space_token");
		}
		if (parser.isOptionSet(null,"retention_policy")) {
			retentionPolicy = parser.stringOptionValue(null,"retention_policy");
		}
		if (parser.isOptionSet(null,"access_latency")) {
			accessLatency = parser.stringOptionValue(null,"access_latency");
		}
		if (parser.isOptionSet(null,"lifetime")) {
			desiredLifetime = 
                            Long.parseLong(parser.stringOptionValue(null,"lifetime"));
		}
        
	}
	
	private void readSpaceTokes(ArgParser parser) throws Exception { 
		if (parser.isOptionSet(null,"space_tokens")) {
			String tokens =  parser.stringOptionValue(null,"space_tokens");
			java.util.StringTokenizer st = new java.util.StringTokenizer(tokens,",");
			spaceTokensList = new String[st.countTokens()];
			for (int i=0;st.hasMoreTokens();i++) { 
				spaceTokensList[i]=st.nextToken();
			}
		}
	}

	private String[] readListOfOptions(ArgParser parser,
					   String optionName) throws Exception { 
		return readListOfOptions(parser,optionName,",");
	}

	private String[] readListOfOptions(ArgParser parser,
					   String optionName, 
					   String separator) throws Exception { 
		String[] listOfOptions = null;
		if (parser.isOptionSet(null,optionName)) {
			String tokens =  parser.stringOptionValue(null,optionName);
			java.util.StringTokenizer st = new java.util.StringTokenizer(tokens,separator);
			listOfOptions = new String[st.countTokens()];
			for (int i=0;st.hasMoreTokens();i++) { 
				listOfOptions[i]=st.nextToken();
			}
		}
		return listOfOptions;
	}
    
	private void readReserveSpaceOptions(ArgParser parser) throws Exception {
		if (parser.isOptionSet(null,"protocols")) {
			this.protocols_list = parser.stringOptionValue(null,"protocols");
			java.util.StringTokenizer st = new java.util.StringTokenizer(
				protocols_list,",");
			protocols = new String[st.countTokens()];
			for (int i = 0;st.hasMoreTokens();++i) {
				protocols[i] = st.nextToken();
			}
		}
		if (parser.isOptionSet(null,"array_of_client_networks")) {
			String tmp = parser.stringOptionValue(null,"array_of_client_networks");
			java.util.StringTokenizer st = new java.util.StringTokenizer(tmp,",");
			arrayOfClientNetworks = new String[st.countTokens()];
			for (int i = 0;st.hasMoreTokens();++i) {
				arrayOfClientNetworks[i] = st.nextToken();
			}
		}
		if (parser.isOptionSet(null,"retention_policy")) {
			retentionPolicy = parser.stringOptionValue(null,"retention_policy");
		}
		if (parser.isOptionSet(null,"access_latency")) {
			accessLatency = parser.stringOptionValue(null,"access_latency");
		}
		if (parser.isOptionSet(null,"access_pattern")) {
			accessPattern = parser.stringOptionValue(null,"access_pattern");
		}
		if (parser.isOptionSet(null,"space_desc")) {
			spaceTokenDescription = parser.stringOptionValue(null,"space_desc");
		}
		if (parser.isOptionSet(null,"connection_type")) {
			connectionType = parser.stringOptionValue(null,"connection_type");
		}
		if (parser.isOptionSet(null,"desired_size")) {
			String string_size = parser.stringOptionValue(null,"desired_size");
			desiredReserveSpaceSize = Long.parseLong(string_size);
		}
		if (parser.isOptionSet(null,"guaranteed_size")) {
			String string_size = parser.stringOptionValue(null,"guaranteed_size");
			guaranteedReserveSpaceSize = Long.parseLong(string_size);
		}
		if (parser.isOptionSet(null,"lifetime")) {
			desiredLifetime = 
                            Long.parseLong(parser.stringOptionValue(null,"lifetime"));
		}
	}
    
	private void readReleaseSpaceOptions(ArgParser parser) throws Exception {
		if (parser.isOptionSet(null,"force")) {
			forceFileRelease = parser.booleanOptionValue(null,"force");
		}
		if (parser.isOptionSet(null,"space_token")) {
			spaceToken = parser.stringOptionValue(null,"space_token");
		}
	}
    
	private void readGetSpaceTokensOptions(ArgParser parser) throws Exception {
		if (parser.isOptionSet(null,"space_desc")) {
			spaceTokenDescription = parser.stringOptionValue(null,"space_desc");
		}
	}

	private void readMoveOptions(ArgParser parser) throws Exception {
		if (parser.isOptionSet(null,"protocols")) {
			this.protocols_list = parser.stringOptionValue(null,"protocols");
			java.util.StringTokenizer st = new java.util.StringTokenizer(
				protocols_list,",");
			protocols = new String[st.countTokens()];
			for (int i = 0;st.hasMoreTokens();++i) {
				protocols[i] = st.nextToken();
			}
		}
		if (parser.isOptionSet(null, "report")) {
			report=parser.stringOptionValue(null,"report");
		}
		if(parser.isOptionSet(null,"server_mode")) {
			passive_server_mode = parser.stringOptionValue(null,"server_mode").equalsIgnoreCase("passive");
		}
	}

	private void readSetPermissionOptions(ArgParser parser) throws Exception {
		if (parser.isOptionSet(null,"type")) {
			this.setPermissionType = parser.stringOptionValue(null,"type");
		}
		if (parser.isOptionSet(null,"owner")) {
			this.setOwnerPermissionMode = parser.stringOptionValue(null,"owner");
		}
		if (parser.isOptionSet(null,"group")) {
			this.setGroupPermissionMode = parser.stringOptionValue(null,"group");
		}
		if (parser.isOptionSet(null,"other")) {
			this.setOtherPermissionMode = parser.stringOptionValue(null,"other");
		}
	}


	private void readExtendFileLifetimeOptions(ArgParser parser) throws Exception {
		if (parser.isOptionSet(null,"request_token")) {
			this.srmExtendFileLifetimeRequestToken = parser.stringOptionValue(null,"request_token");
		}
		if (parser.isOptionSet(null,"file_lifetime")) {
			this.newFileLifetime = new Integer(parser.intOptionValue(null,"file_lifetime"));
		}
		if (parser.isOptionSet(null,"pin_lifetime")) {
			this.newPinLifetime = new Integer(parser.intOptionValue(null,"pin_lifetime"));
		}
	}
    
	private void readLsOptions(ArgParser parser) throws Exception {
		recursionDepth = 1;
		if(parser.isOptionSet(null,"recursion_depth")) {
			recursionDepth = parser.intOptionValue(null,"recursion_depth");
		}
		if(parser.isOptionSet(null,"offset")) {
			lsOffset = parser.intOptionValue(null,"offset");
		}
		if(parser.isOptionSet(null,"count")) {
			lsCount = parser.intOptionValue(null,"count");
		}
		if(parser.isOptionSet(null,"l")) {
			longLsFormat = parser.booleanOptionValue(null,"l");
		}
	}
    
	private void readRmdirOptions(ArgParser parser) throws Exception {
		if(parser.isOptionSet(null,"recursive")) {
			recursive = parser.booleanOptionValue(null,"recursive");
		}
		if(parser.isOptionSet(null,"l")) {
			longLsFormat = parser.booleanOptionValue(null,"l");
		}
	}
    
	private void readRmOptions(ArgParser parser) throws Exception {
        
		if(parser.isOptionSet(null,"l")) {
			longLsFormat = parser.booleanOptionValue(null,"l");
		}
	}
    
	private void readMetadataOptions(ArgParser parser) {
	}
    
	private void readStorageInfoOptions(ArgParser parser) {
	}

	private void readCksmOptions(ArgParser parser)  throws Exception {
		if (parser.isOptionSet(null,"cksm_type") ) {
			this.cksm_type = parser.stringOptionValue(null,"cksm_type");
		}
		if (parser.isOptionSet(null,"cksm_value") ) {
			this.cksm_value  = parser.stringOptionValue(null,"cksm_value");
		}

		if ( this.cksm_type == null && this.cksm_value != null )
			this.cksm_type = "adler32";

	}

	private void set(String name, String value) {
		name=name.trim();
		value=value.trim();
		if (value.equalsIgnoreCase("null")){
			value = null;
		}
		if (name.equals("debug")) {
			debug = Boolean.valueOf(value).booleanValue();
		} 
                else if (name.equals("srmcphome")) {
			srmcphome = value;
		} 
                else if (name.equals("gsissl")) {
			gsissl = Boolean.valueOf(value).booleanValue();
		} 
                else if (name.equals("gsiftpclient")) {
			gsiftpclinet = value;
		} 
                else if (name.equals("mapfile")) {
			glue_mapfile = value;
		} 
                else if (name.equals("webservice_protocol")) {
			webservice_protocol = value;
		} 
                else if (name.equals("urlcopy")) {
			urlcopy = value;
		} 
                else if (name.equals("buffer_size")) {
			buffer_size = Integer.parseInt(value);
		} 
                else if (name.equals("tcp_buffer_size")) {
			tcp_buffer_size = Integer.parseInt(value);
		} 
                else if (name.equals("streams_num")) {
			streams_num = Integer.parseInt(value);
		} 
                else if (name.equals("protocols")) {
			protocols_list = value;
		} 
                else if (name.equals("pushmode")) {
			pushmode = Boolean.valueOf(value).booleanValue();
		} 
                else if (name.equals("useproxy")) {
			useproxy = Boolean.valueOf(value).booleanValue();
		} 
                else if (name.equals("x509_user_proxy")) {
			x509_user_proxy = value;
		} 
                else if (name.equals("x509_user_key")) {
			x509_user_key = value;
		} 
                else if (name.equals("x509_user_cert")) {
			x509_user_cert = value;
		} 
                else if (name.equals("x509_user_trusted_certificates")) {
			x509_user_trusted_certificates = value;
		} 
                else if (name.equals("globus_tcp_port_range")) {
			globus_tcp_port_range = value;
		} 
                else if (name.equals("gss_expected_name")) {
			gss_expected_name = value;
		} 
                else if (name.equals("storagetype")) {
			storagetype = value;
		} 
                else if (name.equals("use_urlcopy_script")) {
			use_urlcopy_script = Boolean.valueOf(value).booleanValue();
		}  
                else if (name.equals("retry_num")) {
			retry_num = Integer.parseInt(value);
		} 
                else if (name.equals("retry_timeout")) {
			retry_timeout = Long.parseLong(value);
		} 
                else if (name.equals("connect_to_wsdl")) {
			connect_to_wsdl = Boolean.valueOf(value).booleanValue();
		} 
                else if (name.equals("delegate")) {
			delegate = Boolean.valueOf(value).booleanValue();
		} 
                else if (name.equals("full_delegation")) {
			full_delegation = Boolean.valueOf(value).booleanValue();
		} 
                else if(name.equals("server_mode")) {
			passive_server_mode = value.equalsIgnoreCase("passive");
		} 
                else if(name.equals("long_ls_format")) {
			longLsFormat =  Boolean.valueOf(value).booleanValue();
		} 
                else if(name.equals("recursion_depth")) {
			recursionDepth =  Integer.parseInt(value);
		} 
                else if(name.equals("recursive")) {
			recursive =  Boolean.valueOf(value).booleanValue();
		} 
                else if(name.equals("offset")) {
			lsOffset =  Integer.parseInt(value);
		} 
                else if(name.equals("count")) {
			lsCount =  Integer.parseInt(value);
		} 
                else if(name.equals("srm_protocol_version")) {
			srm_protocol_version =  Integer.parseInt(value);
		} 
                else if(name.equals("request_lifetime")) {
			request_lifetime =  Long.parseLong(value);
		} 
                else if(name.equals("priority")) {
			int jobPriority =  Integer.parseInt(value);
			extraParameters.put("priority",Integer.toString(jobPriority));
		} 
                else if(name.equals("type")) {
			setPermissionType = value;
		} 
                else if(name.equals("owner")) {
			setOwnerPermissionMode = value;
		} 
                else if(name.equals("group")) {
			setGroupPermissionMode = value;
		} 
                else if(name.equals("other")) {
			setOtherPermissionMode = value;
		} 
                else if(name.equals("file_lifetime")) {
			newFileLifetime = Integer.parseInt(value);
		} 
                else if(name.equals("pin_lifetime")) {
			newPinLifetime = Integer.parseInt(value);
		} 
                else if(name.equals("access_latency")) {
			accessLatency = value;
		} 
                else if(name.equals("retention_policy")) {
			retentionPolicy = value;
		} 
                else if(name.equals("do_remove")) {
			doRemove = Boolean.valueOf(value).booleanValue();
		} 
                else if(name.equals("overwrite_mode")) {
			overwriteMode = value;
		} 
                else if(name.equals("recursive")) {
                        recursive = Boolean.valueOf(value).booleanValue();
		} 
                else if(name.equals("access_pattern")) {
                        accessPattern = value;
		} 
                else if(name.equals("connection_type")) {
                        connectionType = value;
		} 
                else { 
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
				String text_value = t.getData();
				if(text_value.equals("null")) {
					text_value = null;
				}
				set(node_name,text_value);
			}
		}
	}
    
	private static void put(Document document,Node root,String elem_name,String value, String comment_str) {
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
		put(document,root,"debug",new Boolean(debug).toString()," true or false");
		put(document,root,"srmcphome",srmcphome," path to srmcp home directory ");
		put(document,root,"gsiftpclient",gsiftpclinet," \"globus-url-copy\" or \"kftp\"");
		put(document,root,"gsissl",new Boolean(gsissl).toString(),"true if use http over gsi over ssl for SOAP invocations \n\t"+
		    "or false to use plain http (no authentication or encryption)");
		put(document,root,"mapfile",glue_mapfile," path to the \"glue\" mapfile");
		put(document,root,"webservice_protocol",webservice_protocol," this could be http or https");
		put(document,root,"use_urlcopy_script",new Boolean(use_urlcopy_script).toString(),
		    " if true, use urlcopy script,\n\t otherwise use java native copiers");
		put(document,root,"urlcopy",urlcopy," path to the urlcopy script ");
		put(document,root,"buffer_size",Integer.toString(buffer_size),
		    "nonnegative integer, 2048 by default");
		put(document,root,"tcp_buffer_size",Integer.toString(tcp_buffer_size),
		    "integer, 0 by default (which means do not set tcp_buffer_size at all)");
		put(document,root,"streams_num",Integer.toString(streams_num),
		    "integer, 10 by default");
		put(document,root,"protocols",protocols_list,
		    " comma separated list of protocol names, \"http,gridftp\" by default");
		put(document,root,"pushmode",new Boolean(pushmode).toString(),
		    " true for pushmode and false for pullmode, false by default");
		put(document,root,"useproxy",new Boolean(useproxy).toString()," true to use user proxy or false to use\n\t"+
		    " certificates directly, true by default");
		put(document,root,"x509_user_proxy",x509_user_proxy,"absolute path to user proxy");
		put(document,root,"x509_user_key",x509_user_key,"absolute path to user (or host) private key");
		put(document,root,"x509_user_cert",x509_user_cert,"absolute path to user (or host) certificate");
		put(document,root,"x509_user_trusted_certificates",
		    x509_user_trusted_certificates,
		    "absolute path to the trusted certificates directory");
		put(document,root,"globus_tcp_port_range",globus_tcp_port_range,"range of ports open for tcp connection");
		put(document,root,"gss_expected_name",gss_expected_name,"gss expected name in the srm server DN.\"host\" by default");
		put(document,root,"storagetype",storagetype,"permanent|durable|volatile;\"permanent\" by default");
		put(document,root,"retry_timeout",
		    Long.toString(retry_timeout),
		    "number of miliseconds to pause for before retrying after the failure");
		put(document,root,"retry_num",
		    Integer.toString(retry_num),
		    "max number of times to retry after failure before giving up");
		put(document,root,"connect_to_wsdl",new Boolean(connect_to_wsdl).toString()," true or false");
		put(document,root,"delegate",new Boolean(delegate).toString()," true or false");
		put(document,root,"full_delegation",new Boolean(full_delegation).toString()," true or false");
		put(document,root,"server_mode", passive_server_mode?"passive":"active",
		    " \'active\' for active mode and \'passive\' for passive mode, \"active\" by default");
		put(document,root,"long_ls_format", new Boolean(longLsFormat).toString(),
		    "true or false");
		put(document,root,"recursion_depth", Integer.toString(recursionDepth),
		    " 0 for no recursion (like unix ls -d),1 for one level and so on");
		put(document,root,"recursive", new Boolean(recursive).toString(),
		    "true or false");
		put(document,root,"offset", Integer.toString(lsOffset),
		    " ls offset ");
		put(document,root,"count", Integer.toString(lsCount),
		    " ls count ");
		put(document,root,"srm_protocol_version", Integer.toString(srm_protocol_version),
		    " 1 for srm 1.1 or 2 for srm 2.1.1, no other protocols are supported ");
		put(document,root,"request_lifetime", Long.toString(request_lifetime),
		    " request lifetime in seconds ");
		put(document,root,"access_latency", accessLatency, 
		    " access latency ");
		put(document,root,"retention_policy", retentionPolicy, 
		    " retention policy ");
		put(document,root,"access_pattern", accessPattern, 
		    " access pattern,  TRANSFER_MODE|PROCESSING_MODE  ");
		put(document,root,"connection_type", connectionType, 
		    " connection type: WAN|LAN ");
		put(document,root,"do_remove",  new Boolean(doRemove).toString(),
		    " true or false ");
		put(document,root,"overwrite_mode",  overwriteMode, 
		    " ALWAYS|NEVER|WHEN_FILES_ARE_DIFFERENT ");
		put(document,root,"recursive",  new Boolean(recursive).toString(),
		    " true or false ");
		for (Iterator i = extraParameters.keySet().iterator(); i.hasNext();) {
			Object key = i.next();
			put(document,root,(String)key,(String)extraParameters.get(key),(String)key);
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
    
	public static final void main( String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.parseArguments(args);
		if (conf.debug) {
			System.out.println(conf.toString());
		}
		if (conf.help) {
			System.out.println(conf.usage());
		}
		if (conf.save_config_file != null) {
			System.out.println("Saving configuration in " +
					   conf.save_config_file);
			conf.write(conf.save_config_file);
			System.out.println("Exiting, no transfer will be performed.");
			System.exit(0);
		}
		if (conf.getFileMetaData) {
			if (conf.surls == null) {
				System.err.println("surls are not specified");
				System.exit(1);
			}
		} else if (conf.getSpaceTokens) {
			if (conf.srmUrl == null) {
				System.err.println("surl is not specified");
				System.exit(1);
			}
		} else if (conf.getPermission) {
			if (conf.surls == null) {
				System.err.println("surls are not specified");
				System.exit(1);
			}
		} else if (conf.checkPermission) {
			if (conf.surls == null) {
				System.err.println("surls are not specified");
				System.exit(1);
			}
		} else if (conf.extendFileLifetime) {
			if (conf.surls == null) {
				System.err.println("surls are not specified");
				System.exit(1);
			}
		} else if (conf.setPermission) {
			if (conf.setPermissionSurl == null) {
				System.err.println("surl is not specified");
				System.exit(1);
			}
			if (conf.setPermissionType==null) { 
				System.err.println("permission type is not specified");
				System.exit(1);
			}
			String types[] = {"ADD","REMOVE","CHANGE"};
			boolean ok=false;
			for (int i = 0; i< types.length; ++ i) { 
				String p = types[i];
				if ( conf.setPermissionType.equalsIgnoreCase(p) ) { 
					ok=true;
					break;
				}
			}
			if ( ok == false ) { 
				StringBuffer sb=new StringBuffer();
				sb.append("Incorrect permission type specified "+conf.setPermissionType+"\n");
				sb.append("supported permission types :\n");
				sb.append("\t");
				for (int i = 0; i< types.length; ++ i) { 
					String p = types[i];
					sb.append(p+" ");
				}
				sb.append("\n");
				System.err.println(sb.toString());
				System.exit(1);
 
			}
			String modes[] = {"NONE","X","W","WR","R","RX","RW","RWX"};
			ok=false;
			if ( conf.setOwnerPermissionMode != null ) { 
				for (int i = 0; i< modes.length; ++ i) { 
					String m= modes[i];
					if ( conf.setOwnerPermissionMode.equalsIgnoreCase(m) ) {
						ok=true;
						break;
				    
					}
				}
				if ( ok == false ) { 
					StringBuffer sb=new StringBuffer();
					sb.append("Incorrect owner permission mode specified "+conf.setOwnerPermissionMode+"\n");
					sb.append("supported owner permission modes :\n");
					sb.append("\t");
					for (int i = 0; i< modes.length; ++ i) { 
						String m= modes[i];
						sb.append(m+" ");
					}
					sb.append("\n");
					System.err.println(sb.toString());
					System.exit(1);
				}
			}
			ok=false;
			if ( conf.setGroupPermissionMode != null ) { 
				for (int i = 0; i< modes.length; ++ i) { 
					String m= modes[i];
					if ( conf.setGroupPermissionMode.equalsIgnoreCase(m) ) {
						ok=true;
						break;
				    
					}
				}
				if ( ok == false ) { 
					StringBuffer sb=new StringBuffer();
					sb.append("Incorrect group permission mode specified "+conf.setGroupPermissionMode+"\n");
					sb.append("supported group permission modes :\n");
					sb.append("\t");
					for (int i = 0; i< modes.length; ++ i) { 
						String m= modes[i];
						sb.append(m+" ");
					}
					sb.append("\n");
					System.err.println(sb.toString());
					System.exit(1);
				}
			}
			ok=false;
			if ( conf.setOtherPermissionMode != null ) { 
				for (int i = 0; i< modes.length; ++ i) { 
					String m= modes[i];
					if ( conf.setOtherPermissionMode.equalsIgnoreCase(m) ) {
						ok=true;
						break;
				    
					}
				}
				if ( ok == false ) { 
					StringBuffer sb=new StringBuffer();
					sb.append("Incorrect other permission mode specified "+conf.setOtherPermissionMode+"\n");
					sb.append("supported other permission modes :\n");
					sb.append("\t");
					for (int i = 0; i< modes.length; ++ i) { 
						String m= modes[i];
						sb.append(m+" ");
					}
					sb.append("\n");
					System.err.println(sb.toString());
					System.exit(1);
				}
			}
		} 
		else if(conf.from == null || conf.to == null) {
			System.err.println("source(s) and/or destination are not specified");
			System.exit(1);
		}
	}
    
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("SRM Configuration:");
		sb.append("\n\tdebug=").append(this.debug);
		sb.append("\n\tgsissl=").append(this.gsissl);
		sb.append("\n\thelp=").append(this.help);
		sb.append("\n\tpushmode=").append(this.pushmode);
		sb.append("\n\tuserproxy=").append(this.useproxy);
		sb.append("\n\tbuffer_size=").append(this.buffer_size);
		sb.append("\n\ttcp_buffer_size=").append(this.tcp_buffer_size);
		sb.append("\n\tstreams_num=").append(this.streams_num);
		sb.append("\n\tconfig_file=").append(this.config_file);
		sb.append("\n\tglue_mapfile=").append(this.glue_mapfile);
		sb.append("\n\twebservice_path=").append(getWebservice_path());
		sb.append("\n\twebservice_protocol=").append(this.webservice_protocol);
		sb.append("\n\tgsiftpclinet=").append(this.gsiftpclinet);
		sb.append("\n\tprotocols_list=").append(this.protocols_list);
		sb.append("\n\tsave_config_file=").append(this.save_config_file);
		sb.append("\n\tsrmcphome=").append(this.srmcphome);
		sb.append("\n\turlcopy=").append(this.urlcopy);
		sb.append("\n\tx509_user_cert=").append(this.x509_user_cert);
		sb.append("\n\tx509_user_key=").append(this.x509_user_key);
		sb.append("\n\tx509_user_proxy=").append(this.x509_user_proxy);
		sb.append("\n\tx509_user_trusted_certificates=").append(this.x509_user_trusted_certificates);
		sb.append("\n\tglobus_tcp_port_range=").append(this.globus_tcp_port_range);
		sb.append("\n\tgss_expected_name=").append(this.gss_expected_name);
		sb.append("\n\tstoragetype=").append(this.storagetype);
		sb.append("\n\tretry_num=").append(this.retry_num);
		sb.append("\n\tretry_timeout=").append(this.retry_timeout);
		sb.append("\n\twsdl_url=").append(this.wsdl_url);
		sb.append("\n\tuse_urlcopy_script=").append(this.use_urlcopy_script);
		sb.append("\n\tconnect_to_wsdl=").append(this.connect_to_wsdl);
		sb.append("\n\tdelegate=").append(this.delegate);
		sb.append("\n\tfull_delegation=").append(this.full_delegation);
		sb.append("\n\tserver_mode=").append(this.passive_server_mode?"passive":"active");
		sb.append("\n\tsrm_protocol_version=").append(this.srm_protocol_version);
		sb.append("\n\trequest_lifetime=").append(this.request_lifetime);
		if(retentionPolicy != null) {
			sb.append("\n\tretention policy="+retentionPolicy);
		}
		sb.append("\n\taccess latency="+accessLatency);
		sb.append("\n\toverwrite mode="+overwriteMode);
		for (Iterator i = extraParameters.keySet().iterator(); i.hasNext();) {
			Object key = i.next();
			sb.append("\n\t"+key+"="+extraParameters.get(key));
		}
		if (getFileMetaData && surls != null) {
			for (int i = 0; i<surls.length; ++i) {
				sb.append("\n\tsurl["+i+"]=").append(this.surls[i]);
			}
		}
		if (getSpaceTokens && srmUrl != null) {
			sb.append("\n\tsurl=").append(this.srmUrl);
			sb.append("\n\tspace token description="+spaceTokenDescription);
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
			sb.append("\n\trecursion depth=" + recursionDepth);
			sb.append("\n\toffset=" + lsOffset);
			sb.append("\n\tcount=" + lsCount);
			sb.append("\n\tis long listing mode=" + longLsFormat);
			for(int i = 0; i< surls.length; i++)
				sb.append("\n\tsurl[" + i + "]=").append(this.surls[i]);
		}
		if (is_rm && surls != null) {
			sb.append("\n\taction is rm");
			sb.append("\n\trecursion level=" + recursionDepth);
			for(int i = 0; i< surls.length; i++)
				sb.append("\n\tsurl[" + i + "]=").append(this.surls[i]);
		}
		if (is_rmdir && surls != null) {
			sb.append("\n\taction is rmdir");
			sb.append("\n\trecursive =" + recursive);
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
			sb.append("\n\tdesired size="+desiredReserveSpaceSize);
			sb.append("\n\tguaranteed size="+guaranteedReserveSpaceSize);
			sb.append("\n\tlifetime="+desiredLifetime);
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
			sb.append("\n\tspace token description="+spaceTokenDescription);
			sb.append("\n\taccess pattern="+accessPattern);
		}
		if (releaseSpace) {
			sb.append("\n\taction is releaseSpace");
			if ( surls != null ) {
				for(int i = 0; i< surls.length; i++) {
					sb.append("\n\tsrm url=").append(this.surls[i]);
				}
			}
			sb.append("\n\tspace token="+spaceToken);
			sb.append("\n\tforce="+forceFileRelease);
		}
		if (getSpaceMetaData) {
			sb.append("\n\taction is getSpaceMetaData");
			sb.append("\n\tsrm url=").append(srmUrl);
			if ( getSpaceTokensList() != null ) {
				for(int i = 0; i< getSpaceTokensList().length; i++) {
					sb.append("\n\tgetSpaceMetaDataToken =").append(getSpaceTokensList()[i]);
				}
			}
            
		}
		if (copyjobfile != null) {
			sb.append("\n\tcopy job file =\"").append(this.copyjobfile).append('"');
		}
		if (spaceToken != null) {
			sb.append("\n\tspace token="+spaceToken);
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
    
	/** Getter for property srmcphome.
	 * @return Value of property srmcphome.
	 */
	public String getSrmcphome() {
		return srmcphome;
	}
    
	/** Setter for property srmcphome.
	 * @param srmcphome New value of property srmcphome.
	 */
	public void setSrmcphome(String srmcphome) {
		this.srmcphome = srmcphome;
	}
    
	/** Getter for property urlcopy.
	 * @return Value of property urlcopy.
	 */
	public String getUrlcopy() {
		return urlcopy;
	}
    
	/** Setter for property urlcopy.
	 * @param urlcopy New value of property urlcopy.
	 */
	public void setUrlcopy(String urlcopy) {
		this.urlcopy = urlcopy;
	}
    
	/** Getter for property gsiftpclinet.
	 * @return Value of property gsiftpclinet.
	 */
	public String getGsiftpclinet() {
		return gsiftpclinet;
	}
    
	/** Setter for property gsiftpclinet.
	 * @param gsiftpclinet New value of property gsiftpclinet.
	 */
	public void setGsiftpclinet(String gsiftpclinet) {
		this.gsiftpclinet = gsiftpclinet;
	}
    
	/** Getter for property gsissl.
	 * @return Value of property gsissl.
	 */
	public boolean isGsissl() {
		return gsissl;
	}
    
	/** Setter for property gsissl.
	 * @param gsissl New value of property gsissl.
	 */
	public void setGsissl(boolean gsissl) {
		this.gsissl = gsissl;
	}
    
	/** Getter for property glue_mapfile.
	 * @return Value of property glue_mapfile.
	 */
	public String getGlue_mapfile() {
		return glue_mapfile;
	}
    
	/** Setter for property glue_mapfile.
	 * @param glue_mapfile New value of property glue_mapfile.
	 */
	public void setGlue_mapfile(String glue_mapfile) {
		this.glue_mapfile = glue_mapfile;
	}
    
	/** Getter for property webservice_path.
	 * @return Value of property webservice_path.
	 */
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
    
	/** Setter for property webservice_path.
	 * @param webservice_path New value of property webservice_path.
	 */
	public void setWebservice_path(String webservice_path) {
		this.webservice_path = webservice_path;
	}
    
	/** Getter for property useproxy.
	 * @return Value of property useproxy.
	 */
	public boolean isUseproxy() {
		return useproxy;
	}
    
	/** Setter for property useproxy.
	 * @param useproxy New value of property useproxy.
	 */
	public void setUseproxy(boolean useproxy) {
		this.useproxy = useproxy;
	}
    
	/** Getter for property x509_user_proxy.
	 * @return Value of property x509_user_proxy.
	 */
	public String getX509_user_proxy() {
		return x509_user_proxy;
	}
    
	/** Setter for property x509_user_proxy.
	 * @param x509_user_proxy New value of property x509_user_proxy.
	 */
	public void setX509_user_proxy(String x509_user_proxy) {
		this.x509_user_proxy = x509_user_proxy;
	}
    
	/** Getter for property x509_user_key.
	 * @return Value of property x509_user_key.
	 */
	public String getX509_user_key() {
		return x509_user_key;
	}
    
	/** Setter for property x509_user_key.
	 * @param x509_user_key New value of property x509_user_key.
	 */
	public void setX509_user_key(String x509_user_key) {
		this.x509_user_key = x509_user_key;
	}
    
	/** Getter for property x509_user_cert.
	 * @return Value of property x509_user_cert.
	 */
	public String getX509_user_cert() {
		return x509_user_cert;
	}
    
	/** Setter for property x509_user_cert.
	 * @param x509_user_cert New value of property x509_user_cert.
	 */
	public void setX509_user_cert(String x509_user_cert) {
		this.x509_user_cert = x509_user_cert;
	}
    
	/** Getter for property x509_user_trusted_certificates.
	 * @return Value of property x509_user_trusted_certificates.
	 */
	public String getX509_user_trusted_certificates() {
		return x509_user_trusted_certificates;
	}
    
	/** Setter for property x509_user_trusted_certificates.
	 * @param x509_user_trusted_certificates New value of property x509_user_trusted_certificates.
	 */
	public void setX509_user_trusted_certificates(String x509_user_trusted_certificates) {
		this.x509_user_trusted_certificates = x509_user_trusted_certificates;
	}
    
	/** Getter for property globus_tcp_port_range.
	 * @return Value of property globus_tcp_port_range.
	 */
	public String getGlobus_tcp_port_range() {
		return globus_tcp_port_range;
	}
    
	/** Setter for property globus_tcp_port_range.
	 * @param globus_tcp_port_range New value of property globus_tcp_port_range.
	 */
	public void setGlobus_tcp_port_range(String globus_tcp_port_range) {
		this.globus_tcp_port_range = globus_tcp_port_range;
	}
	/** Getter for property gss_expected_name.
	 * @return Value of property gss_expected_name.
	 */
	public String getGss_expected_name() {
		if (gss_expected_name == null){
			gss_expected_name = "host";
		}
		//System.out.println("ExpectedName in Configuration: "+gss_expected_name);
		return gss_expected_name;
	}
    
	/** Setter for property gss_expected_name.
	 * @param gss_expected_name New value of property gss_expected_name.
	 */
	public void setGss_expected_name(String gss_expected_name) {
		this.gss_expected_name = gss_expected_name;
	}
	/** Getter for property storagetype.
	 * @return Value of property storagetype.
	 */
	public String getStorageType() {
		if (storagetype == null){
			storagetype = "permanent";
		}
		return storagetype;
	}
    
	/** Setter for property storagetype.
	 * @param storagetype New value of property storagetype.
	 */
	public void setStorageType(String storage_type) {
		this.storagetype = storage_type;
	}
	/** Getter for property debug.
	 * @return Value of property debug.
	 */
	public boolean isDebug() {
		return debug;
	}
    
	/** Setter for property debug.
	 * @param debug New value of property debug.
	 */
	public void setDebug(boolean debug) {
		this.debug = debug;
	}
    
	/** Getter for property stage.
	 * @return Value of property stage.
	 */
	public boolean isStage() {
		return stage;
	}
    
	/** Setter for property stage.
	 * @param stage New value of property stage.
	 */
	public void setStage(boolean stage) {
		this.stage = stage;
	}
	/** Getter for property connect_to_wsdl.
	 * @return Value of property connect_to_wsdl.
	 */
	public boolean isConnect_to_wsdl() {
		return connect_to_wsdl;
	}
    
	/** Setter for property connect_to_wsdl.
	 * @param connect_to_wsdl New value of property connect_to_wsdl.
	 */
	public void setConnect_to_wsdl(boolean connect_to_wsdl) {
		this.connect_to_wsdl = connect_to_wsdl;
	}
    
	/** Getter for property webservice_protocol.
	 * @return Value of property webservice_protocol.
	 */
	public java.lang.String getwebservice_protocol() {
		return webservice_protocol;
	}
    
	/** Setter for property webservice_protocol.
	 * @param webservice_protocol New value of property webservice_protocol.
	 */
	public void setwebservice_protocol(java.lang.String webservice_protocol) {
		this.webservice_protocol = webservice_protocol;
	}
    
	/** Getter for property protocols.
	 * @return Value of property protocols.
	 */
	public java.lang.String getProtocolsList() {
		return protocols_list;
	}
    
	/** Setter for property protocols.
	 * @param protocols New value of property protocols.
	 */
	public void setProtocolsList(java.lang.String protocols_list) {
		this.protocols_list = protocols_list;
	}
    
	/** Getter for property pushmode.
	 * @return Value of property pushmode.
	 */
	public boolean isPushmode() {
		return pushmode;
	}
    
	/** Setter for property pushmode.
	 * @param pushmode New value of property pushmode.
	 */
	public void setPushmode(boolean pushmode) {
		this.pushmode = pushmode;
	}
    
	/** Getter for property buffer_size.
	 * @return Value of property buffer_size.
	 */
	public int getBuffer_size() {
		return buffer_size;
	}
    
	/** Setter for property buffer_size.
	 * @param buffer_size New value of property buffer_size.
	 */
	public void setBuffer_size(int buffer_size) {
		this.buffer_size = buffer_size;
	}
    
	/** Getter for property tcp_buffer_size.
	 * @return Value of property tcp_buffer_size.
	 */
	public int getTcp_buffer_size() {
		return tcp_buffer_size;
	}
    
	/** Setter for property tcp_buffer_size.
	 * @param tcp_buffer_size New value of property tcp_buffer_size.
	 */
	public void setTcp_buffer_size(int tcp_buffer_size) {
		this.tcp_buffer_size = tcp_buffer_size;
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
    
	/** Getter for property help.
	 * @return Value of property help.
	 */
	public boolean isHelp() {
		return help;
	}
    
	/** Setter for property help.
	 * @param help New value of property help.
	 */
	public void setHelp(boolean help) {
		this.help = help;
	}
    
	/** Getter for property save_config_file.
	 * @return Value of property save_config_file.
	 */
	public java.lang.String getSave_config_file() {
		return save_config_file;
	}
    
	/** Setter for property save_config_file.
	 * @param save_config_file New value of property save_config_file.
	 */
	public void setSave_config_file(java.lang.String save_config_file) {
		this.save_config_file = save_config_file;
	}
    
	/** Getter for property webservice_protocol.
	 * @return Value of property webservice_protocol.
	 */
	public java.lang.String getWebservice_protocol() {
		return webservice_protocol;
	}
    
	/** Setter for property webservice_protocol.
	 * @param webservice_protocol New value of property webservice_protocol.
	 */
	public void setWebservice_protocol(java.lang.String webservice_protocol) {
		this.webservice_protocol = webservice_protocol;
	}
    
	/** Getter for property protocols.
	 * @return Value of property protocols.
	 */
	public java.lang.String[] getProtocols() {
		return this.protocols;
	}
    
	/** Setter for property protocols.
	 * @param protocols New value of property protocols.
	 */
	public void setProtocols(java.lang.String[] protocols) {
		this.protocols = protocols;
	}
    
	/** Getter for property copyjobfile.
	 * @return Value of property copyjobfile.
	 *
	 */
	public java.lang.String getCopyjobfile() {
		return copyjobfile;
	}
    
	/** Setter for property copyjobfile.
	 * @param copyjobfile New value of property copyjobfile.
	 *
	 */
	public void setCopyjobfile(java.lang.String copyjobfile) {
		this.copyjobfile = copyjobfile;
	}
    
	/** Getter for property wsdl_url.
	 * @return Value of property wsdl_url.
	 *
	 */
	public java.lang.String getWsdl_url() {
		return wsdl_url;
	}
    
	/** Setter for property wsdl_url.
	 * @param wsdl_url New value of property wsdl_url.
	 *
	 */
	public void setWsdl_url(java.lang.String wsdl_url) {
		this.wsdl_url = wsdl_url;
	}
    
	/** Getter for property use_urlcopy_script.
	 * @return Value of property use_urlcopy_script.
	 *
	 */
	public boolean isUse_urlcopy_script() {
		return use_urlcopy_script;
	}
    
	/** Setter for property use_urlcopy_script.
	 * @param use_urlcopy_script New value of property use_urlcopy_script.
	 *
	 */
	public void setUse_urlcopy_script(boolean use_urlcopy_script) {
		this.use_urlcopy_script = use_urlcopy_script;
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
    
	public boolean isGetFileMetaData() { return getFileMetaData; }
    
	public boolean isls() { return ls; }
    
	public boolean isRm() { return is_rm; }
    
	public boolean isRmdir() { return is_rmdir; }
    
	public boolean isMkdir() { return is_mkdir; }
    
	public boolean isGetPermission() { return getPermission;}

	public boolean isCheckPermission() { return checkPermission;}

	public boolean isSetPermission() { return setPermission;}
    
	public boolean isExtendFileLifetime() { return extendFileLifetime; } 
    
	public void setGetFileMetaData(boolean getFileMetaData) {
		this.getFileMetaData = getFileMetaData;
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

	public int getRecursionDepth() {
		return recursionDepth;
	}

	public int getLsOffset() {
		return lsOffset;
	}

	public int getLsCount() {
		return lsCount;
	}
    
	public boolean isLongLsFormat() {
		return longLsFormat;
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

	public Integer getNewFileLifetime() { return newFileLifetime; } 
	public Integer getNewPinLifetime()  { return newPinLifetime; } 
	public String getExtendFileLifetimeRequestToken() { 
		return srmExtendFileLifetimeRequestToken;
	}
	public void setNewFileLifetime(Integer lt) { newFileLifetime=lt; } 
	public void setNewPinLifetime(Integer lt) { newPinLifetime=lt; }
	public void setExtendFileLifetimeRequestToken(String token) { 
		srmExtendFileLifetimeRequestToken=token;
	}

	/** Getter for property setPermissionSurls.
	 * @return Value of property setPermissionSurls.
	 *
	 */
	public java.lang.String getSetPermissionSurl() {
		return this.setPermissionSurl;
	}
	public String getSetPermissionType() { 
		return this.setPermissionType;
	}
    
	public String getSetOwnerPermissionMode() { 
		return this.setOwnerPermissionMode;
	}
    
	public String getSetGroupPermissionMode() { 
		return this.setGroupPermissionMode;
	}
    
	public String getSetOtherPermissionMode() { 
		return this.setOtherPermissionMode;
	}
    
	public void setSetPermissionType(String x) { 
		this.setPermissionType=x;
	}
    
	public void setSetOwnerPermissionMode(String x) { 
		this.setOwnerPermissionMode=x;
	}
    
	public void setSetGroupPermissionMode(String x) { 
		this.setGroupPermissionMode=x;
	}
    
	public void setSetOtherPermissionMode(String x) { 
		this.setOtherPermissionMode=x;
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
    
	/** Getter for property getStorageElementInfo.
	 * @return Value of property getStorageElementInfo.
	 *
	 */
	public boolean isGetStorageElementInfo() {
		return getStorageElementInfo;
	}
    
	/** Setter for property getStorageElementInfo.
	 * @param getStorageElementInfo New value of property getStorageElementInfo.
	 *
	 */
	public void setGetStorageElementInfo(boolean getStorageElementInfo) {
		this.getStorageElementInfo = getStorageElementInfo;
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
    
	/** Getter for property copy.
	 * @return Value of property copy.
	 *
	 */
	public boolean isCopy() {
		return copy;
	}
    
	/** Setter for property copy.
	 * @param copy New value of property copy.
	 *
	 */
	public void setCopy(boolean copy) {
		this.copy = copy;
	}
    
	/** Getter for property bringOnline.
	 * @return Value of property bringOnline.
	 *
	 */
	public boolean isBringOnline() {
		return bringOnline;
	}
    
	/** Setter for property bringOnline.
	 * @param bringOnline New value of property bringOnline.
	 *
	 */
	public void setBringOnline(boolean bringOnline) {
		this.bringOnline = bringOnline;
	}
    
	/** Getter for property ping.
	 * @return Value of property ping.
	 *
	 */
	public boolean isPing() {
		return ping;
	}
    
	/** Setter for property ping.
	 * @param ping New value of property ping.
	 *
	 */
	public void setPing(boolean ping) {
		this.ping = ping;
	}
    
	/** Getter for property reserveSpace.
	 * @return Value of property reserveSpace.
	 *
	 */
	public boolean isReserveSpace() {
		return reserveSpace;
	}
    
	public long getDesiredLifetime() {
		return desiredLifetime;
	}
    
	public boolean isReleaseSpace() {
		return releaseSpace;
	}
    
	public String getSpaceTokenDescription() {
		return spaceTokenDescription;
	}
    
	public void setSpaceTokenDescription(String s) {
		spaceTokenDescription=s;
	}
    
	public String getSpaceToken() {
		return spaceToken;
	}
    
	public void setSpaceToken(String s) {
		spaceToken=s;
	}
    
	public boolean getForceFileRelease() {
		return forceFileRelease;
	}
    
	public void setForceFileRelease(boolean yes) {
		forceFileRelease=yes;
	}
    
	public String getAccessLatency() {
		return accessLatency;
	}
    
	public void setAccessLatency(String s) {
		accessLatency=s;
	}
    
	public String getAccessPattern() {
		return accessPattern;
	}
    
	public void setAccessPattern(String s) {
		accessPattern=s;
	}
    
	public String getRetentionPolicy() {
		return retentionPolicy;
	}
    
	public void setRetentionPolicy(String s) {
		retentionPolicy=s;
	}
    
	public String getConnectionType() {
		return connectionType;
	}
    
	public void setConnectionType(String s) {
		connectionType=s;
	}
    
	public long getDesiredReserveSpaceSize() {
		return desiredReserveSpaceSize;
	}
    
	public void setDesiredReserveSpaceSize(long size) {
		desiredReserveSpaceSize=size;
	}
    
	public String[] getArrayOfClientNetworks() {
		return arrayOfClientNetworks;
	}
    
	public void setArrayOfClientNetworks(String[] a) {
		arrayOfClientNetworks=a;
	}
    
	public void setReserveSpaceLifetime(long life) {
		desiredLifetime=life;
	}
    
	public long getGuaranteedReserveSpaceSize() {
		return guaranteedReserveSpaceSize;
	}
    
	public void setGuaranteedReserveSpaceSize(long size) {
		guaranteedReserveSpaceSize=size;
	}
    
	/** Setter for property reserveSpace.
	 * @param reserveSpace New value of property reserveSpace.
	 *
	 */
	public void setReserveSpace(boolean reserveSpace) {
		this.reserveSpace = reserveSpace;
	}
    
	/** Getter for property move.
	 * @return Value of property move.
	 *
	 */
	public boolean isMove() {
		return is_mv;
	}
    
	/** Setter for property move.
	 * @param move New value of property move.
	 *
	 */
	public void setMove(boolean move) {
		this.is_mv = move;
	}
    
	/** Getter for property advisoryDelete.
	 * @return Value of property advisoryDelete.
	 *
	 */
	public boolean isAdvisoryDelete() {
		return advisoryDelete;
	}
    
	/** Setter for property advisoryDelete.
	 * @param advisoryDelete New value of property advisoryDelete.
	 *
	 */
	public void setAdvisoryDelete(boolean advisoryDelete) {
		this.advisoryDelete = advisoryDelete;
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
	 * Getter for property retry_timeout.
	 * @return Value of property retry_timeout.
	 */
	public long getRetry_timeout() {
		return retry_timeout;
	}
    
	/**
	 * Setter for property retry_timeout.
	 * @param retry_timeout New value of property retry_timeout.
	 */
	public void setRetry_timeout(long retry_timeout) {
		this.retry_timeout = retry_timeout;
	}
    
	/**
	 * Getter for property retry_num.
	 * @return Value of property retry_num.
	 */
	public int getRetry_num() {
		return retry_num;
	}
    
	/**
	 * Setter for property retry_num.
	 * @param retry_num New value of property retry_num.
	 */
	public void setRetry_num(int retry_num) {
		this.retry_num = retry_num;
	}
    
	/**
	 * Getter for property getRequestStatus.
	 * @return Value of property getRequestStatus.
	 */
	public boolean isGetRequestStatus() {
		return getRequestStatus;
	}
    
	/**
	 * Setter for property getRequestStatus.
	 * @param getRequestStatus New value of property getRequestStatus.
	 */
	public void setGetRequestStatus(boolean getRequestStatus) {
		this.getRequestStatus = getRequestStatus;
	}

	/**
	 * Getter for property getRequestStatus.
	 * @return Value of property getRequestStatus.
	 */
	public boolean isGetRequestSummary() {
		return is_getRequestSummary;
	}
    
	/**
	 * Setter for property getRequestSummary.
	 * @param getRequestSummary New value of property getRequestSummary.
	 */
	public void setGetRequestSummary(boolean getRequestSummary) {
		this.is_getRequestSummary = getRequestSummary;
	}


	public boolean isAbortRequest() {
		return is_AbortRequest;
	}
	public void setAbortRequest(boolean abortRequest) { 
		this.is_AbortRequest = abortRequest;
	}


	public boolean isAbortFiles() {
		return is_AbortFiles;
	}
	public void setAbortFiles(boolean yes) { 
		this.is_AbortFiles = yes;
	}


	public boolean isReleaseFiles() {
		return is_ReleaseFiles;
	}
	public void setReleaseFiles(boolean yes) { 
		this.is_ReleaseFiles = yes;
	}


	public boolean isGetRequestTokens() {
		return is_getRequestTokens;
	}
    
	/**
	 * Setter for property getRequestSummary.
	 * @param getRequestSummary New value of property getRequestSummary.
	 */
	public void setGetRequestTokens(boolean getRequestTokens) {
		this.is_getRequestTokens = getRequestTokens;
	}

	public String getUserRequestDescription() { 
		return userRequestDescription;
	}
    
	public void setUserRequestDescription(String desc) { 
		this.userRequestDescription=desc;
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
    
	/**
	 * Getter for property getRequestStatusId.
	 * @return Value of property getRequestStatusId.
	 */
	public int getGetRequestStatusId() {
		return getRequestStatusId;
	}
    
	/**
	 * Setter for property getRequestStatusId.
	 * @param getRequestStatusId New value of property getRequestStatusId.
	 */
	public void setGetRequestStatusId(int getRequestStatusId) {
		this.getRequestStatusId = getRequestStatusId;
	}
    
	/**
	 * Getter for property full_delegation.
	 * @return Value of property full_delegation.
	 */
	public boolean isFull_delegation() {
		return full_delegation;
	}
    
	/**
	 * Setter for property full_delegation.
	 * @param full_delegation New value of property full_delegation.
	 */
	public void setFull_delegation(boolean full_delegation) {
		this.full_delegation = full_delegation;
	}
    
	/**
	 * Getter for property delegate.
	 * @return Value of property delegate.
	 */
	public boolean isDelegate() {
		return delegate;
	}
    
	/**
	 * Setter for property delegate.
	 * @param delegate New value of property delegate.
	 */
	public void setDelegate(boolean delegate) {
		this.delegate = delegate;
	}
    
	/**
	 * Getter for property streams_num.
	 * @return Value of property streams_num.
	 */
	public int getStreams_num() {
		return streams_num;
	}
    
	/**
	 * Setter for property streams_num.
	 * @param streams_num New value of property streams_num.
	 */
	public void setStreams_num(int streams_num) {
		this.streams_num = streams_num;
	}
    
	/**
	 * Getter for property report.
	 * @return Value of property report.
	 */
	public java.lang.String getReport() {
		return report;
	}
    
	/**
	 * Setter for property report.
	 * @param report New value of property report.
	 */
	public void setReport(java.lang.String report) {
		this.report = report;
	}
    
	/**
	 * Getter for property server_mode.
	 * @return Value of property server_mode.
	 */
	public boolean isPassiveServerMode() {
		return passive_server_mode;
	}
    
	/**
	 * Setter for property server_mode.
	 * @param server_mode New value of property server_mode.
	 */
	public void setPassiveServerMode(boolean passive_server_mode) {
		this.passive_server_mode = passive_server_mode;
	}
    
	/**
	 * Getter for property srm_protocol_version.
	 * @return Value of property srm_protocol_version.
	 */
	public int getSrmProtocolVersion() {
		return srm_protocol_version;
	}
    
	/**
	 * Setter for property srm_protocol_version.
	 * @param srmProtocolVersion New value of property srm_protocol_version.
	 */
	public void setSrmProtocolVersion(int srmProtocolVersion) {
		this.srm_protocol_version = srmProtocolVersion;
	}
    
	/**
	 * Getter for property request_lifetime.
	 * @return Value of property request_lifetime.
	 */
	public long getRequestLifetime() {
		return request_lifetime;
	}
    
	/**
	 * Setter for property request_lifetime.
	 * @param requestLifetime New value of property request_lifetime.
	 */
	public void setRequestLifetime(long requestLifetime) {
		this.request_lifetime = requestLifetime;
	}
    
	public String[] getBringOnlineSurls() {
		return surls;
	}
    
	public void setBringOnlineSurls(String[] bringOnlineSurls) {
		this.surls = bringOnlineSurls;
	}
    
	public String getSrmUrl() {
		return srmUrl;
	}
    
	public void setSrmUrl(String srmUrl) {
		this.srmUrl = srmUrl;
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

	public boolean isGetSpaceMetaData() {
		return getSpaceMetaData;
	}

	public void setGetSpaceMetaData(boolean getSpaceMetaData) {
		this.getSpaceMetaData = getSpaceMetaData;
	}

	public String[] getSpaceTokensList() {
		return spaceTokensList;
	}

	public void setSpaceTokensList(String[] spaceTokensList) {
		this.spaceTokensList = spaceTokensList;
	}

	public boolean isGetSpaceTokens() {
		return getSpaceTokens;
	}

	public void setGetSpaceTokens(boolean getSpaceTokens) {
		this.getSpaceTokens = getSpaceTokens;
	}

	public String getOverwriteMode() {
		return overwriteMode;
	}

	public void setOverwriteMode(String overwriteMode) {
		this.overwriteMode = overwriteMode;
	}

	public boolean isRecursive() {
		return recursive;
	}

	public void setRecursive(boolean recursive) {
		this.recursive = recursive;
	}

	/**
	 * Setter for property checksum type.
	 * @param checksum type String value of the property checksum type.
	 */

	public String getCksmType(){ 
		return this.cksm_type;
	}

	public void setCksmType(String type){
		this.cksm_type = type;
	}

	public String getCksmValue(){
		return this.cksm_value;
	}

	public void setCksmValue(String cksm_value){
		this.cksm_value = cksm_value;
	}
	
	public boolean getDoRemove() { 
		return this.doRemove;
	}

	public void setDoRemove(boolean yes) { 
		this.doRemove=yes;
	}

}
