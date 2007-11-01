// $Id: Configuration.java,v 1.50.2.2 2007-04-17 17:02:52 timur Exp $
// $Author: timur $ 
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
	private boolean help;
	private boolean gsissl = true;
	private String glue_mapfile="conf/SRMServerV1.map";
	private String webservice_path = "srm/managerv1";
	private String webservice_protocol="http";
	private boolean useproxy = true;
	private String x509_user_proxy="/home/timur/k5-ca-proxy.pem";
	private String x509_user_key="/home/timur/k5-ca-proxy.pem";
	private String x509_user_cert="/home/timur/k5-ca-proxy.pem";
	private String x509_user_trusted_certificates="/home/timur/.globus/certificates";
	private String globus_tcp_port_range=null;
	private String gss_expected_name=null;
	private String protocols_list="gsiftp,http";
	private boolean pushmode=false;
	private int buffer_size=1024*128;
	private int tcp_buffer_size;
	private int streams_num=10;
	private String config_file = "config.xml";
	private String save_config_file;
	private Logger logger;
	private String[] protocols = new String[]  {"gsiftp","dcap","http"};
	private boolean copy;
	private String[] from;
	private String to;
	private String copyjobfile;
	private String wsdl_url;
	private boolean use_urlcopy_script;
	private boolean getFileMetaData;
	private String[] getFileMetaDataSurls;
	private boolean ls;
	//
	// I am using lsURLs for all directory functions (litvinse@fnal.gov)
	//
	private String[] lsURLs;
	private boolean is_rm;
	private boolean is_rmdir;
	private boolean is_mv;
	private boolean is_mkdir;
	private boolean getPermission;
	private String[] getPermissionSurls;
	private boolean advisoryDelete;
	private String[] advisoryDeleteSurls;
	private boolean getRequestStatus;
	private String getRequestStatusSurl;
	private int getRequestStatusId;
	private boolean getStorageElementInfo;
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
        private int srm_protocol_version=1;
        private long request_lifetime=60*60*24; //default request lifetime in seconds
	/** Creates a new instance of Configuration */
	public Configuration() { }

	private String general_options =
	"\t\t General Options :\n"+
	"-version enables printing version number\n"+
	"-debug=<true or false> true enables debug output, default is false \n"+
	"-srmcphome=<path to srmcp product dir>\n"+
	"-gsissl=<true or false> true uses gsi https, default is false\n"+
	"-mapfile=<mapfile> to specify glue mapfile\n"+
	"-wsdl_url=<full url to web service wsdl> this options overrides\n"+
	"\t -webservice_path and -webservice_protocol options\n"+
	"-webservice_path=<webservice_path> to specify web service path component\n"+
	"\t of web service URL (for example \"srm/managerv1.wsdl\")\n"+
	"-webservice_protocol=<webservice_protocol> to specify the\n"+
	"\t webservice protocol ( for example \"http\",\"https\" or \"httpg\")\n"+
	"-use_proxy=<true or false> true if srmcp should use grid proxy\n"+
	"\t false if it should use certificate and key directly,\n"+
	"\t defult value is true\n"+
	"-x509_user_proxy=<path to user grid proxy>\n"+
	"-x509_user_cert=<path to user grid certificate>\n"+
	"-x509_user_key=<path to user private key>\n"+
	"-x509_user_trusted_certificates=<path to the directory with cerificates\n"+
	"\t of trusted Certificate Authorities (CAs)>\n"+
	"-globus_tcp_port_range=<min value>:<max value>,\n"+"" +
	"\t a range of ports open for tcp connections specified as\n"+
	"\t a pair of positive integers separated by \":\",\n" +
	"\t not set by default \n"+
	"-gss_expected_name=<gss expected name in the srm server DN> default value is host\n"+
	"-srm_protocol_version=<srm protocol version\n"+
	"-conf=<path to the configuration file> default value is config.xml\n"+
	"-save_conf=<path to the file in which the new configuration will be saved>\n"+
	"\t no transfer will be performed if this option is specified\n"+
	"-retry_timeout=<number of miliseconds to sleep after a failure\n"+
	"\t before the client tries to retry>\n"+
	"-retry_num=<number of retries before client gives up>\n"+
	"-connect_to_wsdl=<true or false, false by default> \n"+
	"\t srm client now connects directly to the service without reading\n" +
	"\t the wsdl first but for the compatibility with the old implementation,\n"+
	"\t especially if srm urls available point to the location of the wsdl,\n"+
	"\t we make the old way of connecting to the wsdl location first available\n"+
	"-delegate=<true or false, true by default> \n"+
	"\t enables or disables the delegation of the user credenital to the server\n"+
	"-full_delegation=<true or false, true by default> \n"+
	"\t if delegation is enabled, it specifies the type of delegation\n"+
	"\t if this option is set to true the delegation is full, otherwise limited\n"+
	"-h or -help for this help \n";

	private String storage_info_options =
	"\t\t Storage Info options :\n"+
	"Example:\n" +
	"      srm-storage-element-info  https://fndca.fnal.gov:8443/srm/infoProvider1_0.wsdl\n";

	private String copy_options =
	"\t\t copy options :\n"+
	"-urlopy=<urlcopy path> to specify the path to  universal url_copy script\n"+
	"\t see $SRM_PATH/bin/url-copy.sh for example\n"+
	"-buffer_size=<integer> to set the buffer size to a value \n"+
	"\t different then default(2048)\n"+
	"-tcp_buffer_size=<integer> to set the tcp buffer size to a value \n"+
	"\t if option is not specified or set to 0,\n"+
	"\t then the default tcp buffer size is used\n"+
	"-streams_num=<integer> to set the number of streams used by gridftp \n"+
        "\t if number of stream is set to 1, then stream mode is used, otherwise\"+" +
        "\t extended block mode is used\n"+
        "-server_mode=<active or passive> to set (gridftp) server mode for data transfer, passive by default\n"+
        "\t this option will have affect only if transfer is performed in a stream mode (see -streams_num)\n"+
	"-storagetype=<permanent|volatile|durable> to specify kind of storage to use,\"permanent\" by default\n"+ 
	"-protocols=protocol1[,protocol2[...]] \n"+
	"\t the comma separated list of supported TURL protocols\n"+
	"-pushmode=<true or false>  true to use the push mode in case\n"+
	"\t of srm Mass Storage Systems (MSS) to MSS copy, \n"+
	"\t false to use the pull mode, the default mode is pull mode (false)\n"+
	"-srmstage=<true or false, false by default> \n"+
	"\t if set to true - the source files are staged only onto disk cache\n"+
	"\t and not transferred to client right away> \n"+
	"\t if set to false - the source files are transferred to the client\n"+
	"-use_urlcopy_script=<true or false> use java native copiers of use urcopy script\n"+
	"-srm_protocol_version=<1 or 2> 1 for srm 1.1 or 2 for srm 2.1.1, no other protocols are supported\n"+
	"-request_lifetime=<num of seconds> request lifetime in seconds\n"+
	"-copyjobfile=<file> where <file> is the path to the text file containing \n"+
	"\t the list of sources and destination\n"+
	"\t each line has a format : <sorce-url> <destination-url>\n"+
	"-report=<report_file> where <report_file> is the path to the report file\n"+
	"\t if specified, it will contain the resutls of the execution srmcp \n"+
	"\t the each line in the file will have the following format:\n"+
	"\t<src url> <dst url> <return code> [<error>]\n"+
	"the following return codes are supported:\n"+
	"\t 0 - success\n"+
	"\t 1 - general error\n"+
	"\t 2 - file exists, can not overwrite\n"+
	"\t 3 - user permission error\n" +
	"Example of srm put:\n" +
	"\t srmcp file:////bin/sh srm://myhost.mydomain.edu:8443//dir1/dir2/sh-copy\n"+
	"Example of srm get:\n" +
	"\t srmcp srm://myhost.mydomain.edu:8443//dir1/dir2/sh-copy file:///localdir/sh\n"+
	"Example of srm copy (srm to srm):\n" +
	"\t srmcp srm://myhost.mydomain.edu:8443//dir1/dir2/sh-copy srm://anotherhost.org:8443/newdir/sh-copy\n"+
	"Example of srm copy (gsiftp to srm):\n" +
	"\t srmcp gsiftp://ftphost.org//path/file srm://myhost.mydomain.edu:8443//dir1/dir2/file\n";


	private String move_options =
	"\t\t mv options :\n"+
	"-copyjobfile=<file> where <file> is the path to the text file containing \n"+
	"\t the list of sources and destination\n"+
	"\t each line has a format : <sorce-url> <destination-url>\n"+
        "New directory can include path, as long as all sub directories exist.\n"+
        "Moves within single storage system are allowed (you can't mv from one SRM to another SRM \n"+
	" (or from/to remote/local filesystem, use copy and delete)).\n"+
	"Examples: \n"+
	"\t srm -mv srm://fledgling06.fnal.gov:8443/srm/managerv2?SFN=/from_path/ \n"+
	"\t\t srm://fledgling06.fnal.gov:8443/srm/managerv2?SFN=/to_path/ \n";

	private String rmdir_options =
	"\t\t rmdir options :\n"+
        "-recursion_depths=<integer> recursive empty directory deletion (any number not euqal to 0).\n"+
	"\t -rmdir is defined in SRM specification as :\n"+
	"\t \"applies to dir doRecursiveRemove is false by edefault. To distinguish from \n"+
	"\t srmRm(), this function is for directories only. \"\n"+
	"\t so it is unclear id the directories must be empty. \n"+
	"\t We interpret \"rmdir\" as Unix \"rmdir\" which allows to remove only empty directories \n"+
	"\t extending it to have an ability to remove trees of empty directories. \n"+
	"\t Removal of multiple directories is not supported \n"+
	"\t Removal of files is not supported (use -rm).\n"+
	"Examples: \n"+
	"\t srmrmdir srm://fledgling06.fnal.gov:8443/srm/managerv2?SFN=/dir \n"+ 
	"\t srm -rmdir  srm://fledgling06.fnal.gov:8443/srm/managerv2?SFN=/dir \n"+ 
	"\t srm -rmdir -recursion_depth=1 srm://fledgling06.fnal.gov:8443/srm/managerv2?SFN=/dir \n";

	private String mkdir_options =
	"\t\t mkdir options :\n"+
        "No options are defined for \"mkdir \". New directory  can include path,\n"+
	"as long as all sub directories exist.\n"+
	"Examples: \n"+
	"\t srm -mkmdir srm://fledgling06.fnal.gov:8443/srm/managerv2?SFN=/dir/path/ \n";
	
	private String rm_options =
	"\t\t rm options :\n"+
	"\t no additional options are suported for \"-rm\". \n"+
	"\t Applies to files only.\n"+
	"Examples: \n"+
	"\t srmrm srm://fledgling06.fnal.gov:8443/srm/managerv2?SFN=/dir/file  \n"+ 
	"\t srm -rm  srm://fledling06.fnal.gov:8443/srm/managerv2?SFN=/dir/file  \n";
	
        private String ls_options =
	"\t\t ls options :\n"+
        "-l changes srm ls to long format mode, may result in dramatic increase in execution time\n"+
        "-recursion_depth=<integer> controls how deep to descend into direcotory trees\n"+
        "\t 0 means do not descend at all, equivalent to unix ls -d option, 0 is the default value\n";
        
        private String stage_options =
        "\t\t stage options:\n";

        private String getPermission_options =
        "\t\t getpermission options:\n";

	public   final String usage() {
		if (getStorageElementInfo) {
			return
				"Usage: get-storage-element-info [command line options] endpoint-wsdl-url\n"+
				" default options will be read from configuration file \n"+
				" but can be overridden by the command line options\n"+
				" the command line options are one or more of the following:\n"+
				general_options+storage_info_options;
		}
		if (getFileMetaData) {
			return
				"Usage:get-file-metadata [command line options]  srmurl(s)\n"+
				" default options will be read from configuration file \n"+
				" but can be overridden by the command line options\n"+
				" the command line options are one or more of the following:\n"+
				general_options;
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
				" either source(s) or destination or both should be (an) srm url\n"+
				" default options will be read from configuration file \n"+
				" but can be overridden by the command line options\n"+
				" the command line options are one or more of the following:\n"+
				general_options+copy_options;
		}
		if (ls) {
			return
				"Usage: srmls [srm url] [[srm url]...]\n" +
				general_options+ls_options;
		}
		if (is_rm) {
			return
				"Usage: srmrm [srm url] [[srm url]...]\n" +
				general_options+rm_options;
		}
		if (is_mv) {
			return
				"Usage: srmmv [srm url(source)] [srm url(destination)] \n"+
				general_options+move_options;
		}
		if (is_rmdir) {
			return
				"Usage: srmrmdir [srm url] \n" +
				general_options+rmdir_options;
		}
		if (is_mkdir) {
			return
				"Usage: srmmkdir [srm url] \n" +
				general_options+mkdir_options;
		}
                if(stage) {
			return
				"Usage: srmstage  [srm url] [[srm url]...] \n" +
				general_options+stage_options;
                    
                }
                if(getPermission) {
			return
				"Usage: srmgetpermission  [srm url] [[srm url]...] \n" +
				general_options+getPermission_options;
		}
		return
			"Usage: srm [command option] [command line options] arguments\n"+
			"where command option is one of the following :\n"+
			" -copy                   performs srm \"get\", \"put\", or \"copy\" depending \n"+
			"                         on arguments \n"+
                        "-stage                   performs srm \"get\", without actual copy of the files \n"+
                        "                         with the hope that this will trigger staging in srm managed\n"+
                        "                         hierarchical storage system\n"+
			" -mv                     performs srm \"mv\" of files and directories \n"+
			" -ls                     list content of directory \n"+
			" -rm                     remove file(s) \n"+
			" -mkdir                  create directory \n"+
			" -rmdir                  remove empty directory tree \n"+
			" -getPermission	  get file(s) permission \n"+
			" -advisoryDelete         performs srm advisory delete \n"+
			" -getRequestStatus       obtains and prints srm request status \n"+
			" -getFileMetaData        gets srm meta data for specified sulrs\n"+
			" -getStorageElementInfo  prints storage element info, in particular\n"+
			"                         it prints total, availabale and used space amount\n"+
			" -h,-help                prints this help\n"+
			" type srm [command option] -h for more info about a particular option";
	}
	
	public void parseArguments(String args[]) throws Exception {
		ArgParser parser = new ArgParser(args);
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
		parser.addVoidOption(null,"getPermission","gets file(s) permissions");
		parser.addVoidOption(null,"getFileMetaData","gets FileMetaData");
		parser.addVoidOption(null,"getStorageElementInfo", "gets StorageElementInfo");
		parser.addVoidOption(null,"advisoryDelete", "performs AdvisoryDelete");
		parser.addVoidOption(null,"getRequestStatus", "gets Request Status");
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
                parser.addBooleanOption(null, "l", "enables lonf format mode for srmls");
                parser.addIntegerOption(null,"recursion_depth",
                                        "controls how deep to descend into direcotory trees", 
                                        0,Integer.MAX_VALUE);
                parser.addIntegerOption(null,"srm_protocol_version","1 for srm 1.1 or 2 for srm 2.1.1, no other protocols are supported ",1,2);
                parser.addIntegerOption(null,"request_lifetime","request lifetime in seconds",1,Integer.MAX_VALUE);
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

		String [] arguments = parser.getCommandArguments();

		if( isHelp()) {
			return;
		}
      
		if (!( is_mv ^ is_mkdir ^ is_rmdir ^ is_rm ^ ls ^ copy ^ getFileMetaData ^ advisoryDelete ^ getRequestStatus ^ getStorageElementInfo ^ stage ^ getPermission)) {
			if(version) System.exit(1);
			throw new IllegalArgumentException(
				"one and only one of the following options must be " +
				"specified:\n\n" + usage());
		}
		if (getFileMetaData) {
			getFileMetaDataSurls = arguments;
		}
		else if (advisoryDelete) {
			advisoryDeleteSurls = arguments;
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
		else if (copy||is_mv) {
			if (copy) {
				readCopyOptions(parser);
			}
			else if (is_mv) { 
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
		else if (ls) {
			readLsOptions(parser);
			lsURLs = arguments;
		} 		
		else if (is_rm ) {
			readRmOptions(parser);
			lsURLs = arguments;
		} 		
		else if ( is_rmdir ) {
			readRmdirOptions(parser);
			lsURLs = arguments;
		}
		else if ( is_mkdir ) {
			lsURLs = arguments;
		}
                else if (stage) {
			lsURLs = arguments;
                }
		else if (getPermission) {
			getPermissionSurls = arguments;
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
		} 
		else {
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
		} 
		else {
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
		if(parser.isOptionSet(null,"srm_protocol_version")) {
			srm_protocol_version = parser.intOptionValue(null,"srm_protocol_version");
			if(srm_protocol_version != 1 && srm_protocol_version != 2) {
				throw new IllegalArgumentException("illegal srm protocol version "+ srm_protocol_version);
                        }
		}
		if(parser.isOptionSet(null,"request_lifetime")) {
			request_lifetime = (long)parser.intOptionValue(null,"request_lifetime");
			if(request_lifetime <= 0) {
				throw new IllegalArgumentException("illegal value for request lifetime"+
								   request_lifetime);
                        }
                }
		copy                  = parser.isOptionSet(null, "copy");
		getFileMetaData       = parser.isOptionSet(null, "getFileMetaData");
                getStorageElementInfo = parser.isOptionSet(null, "getStorageElementInfo");
		advisoryDelete        = parser.isOptionSet(null, "advisoryDelete");
		getRequestStatus      = parser.isOptionSet(null, "getRequestStatus");
		ls                    = parser.isOptionSet(null, "ls");
		is_rm                 = parser.isOptionSet(null, "rm");
		is_mv                 = parser.isOptionSet(null, "mv");
		is_rmdir              = parser.isOptionSet(null, "rmdir");
		is_mkdir              = parser.isOptionSet(null, "mkdir");
                stage                 = parser.isOptionSet(null, "stage");
		getPermission	      = parser.isOptionSet(null, "getPermission");
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
		if (parser.isOptionSet(null, "copyjobfile")) {
			copyjobfile=parser.stringOptionValue(null,"copyjobfile");
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

	private void readLsOptions(ArgParser parser) throws Exception {
                recursionDepth = 1;
		if(parser.isOptionSet(null,"recursion_depth")) {
			recursionDepth = parser.intOptionValue(null,"recursion_depth");
                }
		if(parser.isOptionSet(null,"l")) {
			longLsFormat = parser.booleanOptionValue(null,"l");
                }
	}
        
	private void readRmdirOptions(ArgParser parser) throws Exception {
                recursionDepth = 0;
		if(parser.isOptionSet(null,"recursion_depth")) {
			recursionDepth = parser.intOptionValue(null,"recursion_depth");
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
	private void set(String name, String value) {
		name=name.trim();
		value=value.trim();
		if (value.equalsIgnoreCase("null")){
			value = null;
		}
		if (name.equals("debug")) {
			debug = Boolean.valueOf(value).booleanValue();
		} else if (name.equals("srmcphome")) {
			srmcphome = value;
		} else if (name.equals("gsissl")) {
			gsissl = Boolean.valueOf(value).booleanValue();
		} else if (name.equals("gsiftpclient")) {
			gsiftpclinet = value;
		} else if (name.equals("mapfile")) {
			glue_mapfile = value;
		} else if (name.equals("webservice_protocol")) {
			webservice_protocol = value;
		} else if (name.equals("urlcopy")) {
			urlcopy = value;
		} else if (name.equals("buffer_size")) {
			buffer_size = Integer.parseInt(value);
		} else if (name.equals("tcp_buffer_size")) {
			tcp_buffer_size = Integer.parseInt(value);
		} else if (name.equals("streams_num")) {
			streams_num = Integer.parseInt(value);
		} else if (name.equals("protocols")) {
			protocols_list = value;
		} else if (name.equals("pushmode")) {
			pushmode = Boolean.valueOf(value).booleanValue();
		} else if (name.equals("useproxy")) {
			useproxy = Boolean.valueOf(value).booleanValue();
		} else if (name.equals("x509_user_proxy")) {
			x509_user_proxy = value;
		} else if (name.equals("x509_user_key")) {
			x509_user_key = value;
		} else if (name.equals("x509_user_cert")) {
			x509_user_cert = value;
		} else if (name.equals("x509_user_trusted_certificates")) {
			x509_user_trusted_certificates = value;
		} else if (name.equals("globus_tcp_port_range")) {
			globus_tcp_port_range = value;
		} else if (name.equals("gss_expected_name")) {
			gss_expected_name = value;
		} else if (name.equals("storagetype")) {
			storagetype = value;
		} else if (name.equals("use_urlcopy_script")) {
			use_urlcopy_script = Boolean.valueOf(value).booleanValue();
		} else if (name.equals("retry_num")) {
			retry_num = Integer.parseInt(value);
		} else if (name.equals("retry_timeout")) {
			retry_timeout = Long.parseLong(value);
		} else if (name.equals("connect_to_wsdl")) {
			connect_to_wsdl = Boolean.valueOf(value).booleanValue();
		} else if (name.equals("delegate")) {
			delegate = Boolean.valueOf(value).booleanValue();
		} else if (name.equals("full_delegation")) {
			full_delegation = Boolean.valueOf(value).booleanValue();
                } else if(name.equals("server_mode")) {
                        passive_server_mode = value.equalsIgnoreCase("passive");
                } else if(name.equals("long_ls_format")) {
                        longLsFormat =  Boolean.valueOf(value).booleanValue();
		} else if(name.equals("recursion_depth")) {
                        recursionDepth =  Integer.parseInt(value);
                } else if(name.equals("srm_protocol_version")) {
                        srm_protocol_version =  Integer.parseInt(value);
                } else if(name.equals("request_lifetime")) {
                        request_lifetime =  Long.parseLong(value);
		} else {
			//System.err.println("set::trying to set unknown name \""+ name+ "\" to \""+ value+"\"");
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
                put(document,root,"srm_protocol_version", Integer.toString(srm_protocol_version),
                  " 1 for srm 1.1 or 2 for srm 2.1.1, no other protocols are supported ");
                put(document,root,"request_lifetime", Long.toString(request_lifetime),
                  " request lifetime in seconds ");
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
			if (conf.getFileMetaDataSurls == null) {
				System.err.println("surls are not specified");
				System.exit(1);
			}
		} else if (conf.getPermission) {
			if (conf.getPermissionSurls == null) {
				System.err.println("surls are not specified");
				System.exit(1);
			}		
		} else if(conf.from == null || conf.to == null) {
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
		sb.append("\n\twebservice_path=").append(this.webservice_path);
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
		if (getFileMetaData && getFileMetaDataSurls != null) {
			for (int i = 0; i<getFileMetaDataSurls.length; ++i) {
				sb.append("\n\tsurl["+i+"]=").append(this.getFileMetaDataSurls[i]);
			}
		}
		if (getPermission && lsURLs != null) {
			sb.append("\n\taction is getPermission");
			for (int i = 0; i<getPermissionSurls.length; ++i) {
				sb.append("\n\tsurl["+i+"]=").append(this.getPermissionSurls[i]);
			}
		}
		if (ls && lsURLs != null) {
			sb.append("\n\taction is ls");
			sb.append("\n\trecursion depth=" + recursionDepth);
			sb.append("\n\tis long listing mode=" + longLsFormat);
			for(int i = 0; i< lsURLs.length; i++) 
				sb.append("\n\tsurl[" + i + "]=").append(this.lsURLs[i]);
		}
		if (is_rm && lsURLs != null) {
			sb.append("\n\taction is rm");
			sb.append("\n\trecursion level=" + recursionDepth);
			for(int i = 0; i< lsURLs.length; i++) 
				sb.append("\n\tsurl[" + i + "]=").append(this.lsURLs[i]);
		}
		if (is_rmdir && lsURLs != null) {
			sb.append("\n\taction is rmdir");
			sb.append("\n\trecursion level=" + recursionDepth);
			for(int i = 0; i< lsURLs.length; i++) 
				sb.append("\n\tsurl[" + i + "]=").append(this.lsURLs[i]);
		}
		if (is_mkdir && lsURLs != null) {
			sb.append("\n\taction is mkdir");
			for(int i = 0; i< lsURLs.length; i++) 
				sb.append("\n\tsurl[" + i + "]=").append(this.lsURLs[i]);
		}
		else if (copyjobfile != null) {
			sb.append("\n\tcopy job file =\"").append(this.copyjobfile).append('"');
		} 
		else {
			if (from!= null) {
				for(int i = 0; i<from.length; ++i) {
					sb.append("\n\tfrom["+i+"]=").append(this.from[i]);
				}
			} 
			else {
				sb.append("\n\tfrom=null");
			}
		}
		sb.append("\n\tto=").append(this.to).append('\n');
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
		return webservice_path;
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
   
	public void setGetFileMetaData(boolean getFileMetaData) {
		this.getFileMetaData = getFileMetaData;
	}
   	
	public String[] getLsURLs() {
		return lsURLs;
	}
	
	public void setLsURLs(String[] inURLs) {
		lsURLs = inURLs;
	}

	public String[] getRmURLs() {
		return lsURLs;
	}

	public String[] getRmdirURLs() {
		return lsURLs;
	}
	
	public void setRmURLs(String[] inURLs) {
		lsURLs = inURLs;
	}

	public String[] getMkDirURLs() {
		return lsURLs;
	}
	
	public void setMkDirURLs(String[] inURLs) {
		lsURLs = inURLs;
	}

	public void setRmdirURLs(String[] inURLs) {
		lsURLs = inURLs;
	}
	
	public int getRecursionDepth() {
            return recursionDepth;
        }

	public boolean isLongLsFormat() {
            return longLsFormat;
        }
        
	/** Getter for property getFileMetaDataSurls.
	 * @return Value of property getFileMetaDataSurls.
	 *
	 */
	public java.lang.String[] getGetFileMetaDataSurls() {
		return this.getFileMetaDataSurls;
	}
	
	/** Setter for property getFileMetaDataSurls.
	 * @param getFileMetaDataSurls New value of property getFileMetaDataSurls.
	 *
	 */
	public void setGetFileMetaDataSurls(java.lang.String[] getFileMetaDataSurls) {
		this.getFileMetaDataSurls = getFileMetaDataSurls;
	}

	/** Getter for property getPermissionSurls.
	 * @return Value of property getPermissionSurls.
	 *
	 */
	public java.lang.String[] getGetPermissionSurls() {
		return this.getPermissionSurls;
	}
	
	/** Setter for property getPermissionSurls.
	 * @param getPermissionSurls New value of property getPermissionSurls.
	 *
	 */
	public void setGetPermissionSurls(java.lang.String[] getPermissionSurls) {
		this.getPermissionSurls = getPermissionSurls;
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
		return this.advisoryDeleteSurls;
	}
	
	/** Setter for property advisoryDeleteSurls.
	 * @param advisoryDeleteSurls New value of property advisoryDeleteSurls.
	 *
	 */
	public void setAdvisoryDeleteSurls(java.lang.String[] advisoryDeleteSurls) {
		this.advisoryDeleteSurls = advisoryDeleteSurls;
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
          
}
