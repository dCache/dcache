#include <xercesc/parsers/XercesDOMParser.hpp> 
#include <xercesc/dom/DOM.hpp> 
#include <xercesc/sax/HandlerBase.hpp> 
#include <xercesc/util/XMLString.hpp> 
#include <xercesc/util/PlatformUtils.hpp>

#include <xercesc/dom/deprecated/DOM.hpp>

#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <stdlib.h>
#include <stdio.h>
#include <getopt.h>

#include "SrmWsClientConfig.hpp"

extern int optind; // for long_options();
//---------------------------------------------------------------------------------

namespace srm {

  // Keep track of the file CVS version in the binary file
  static char cvsid[] = "$Id: SrmWsClientConfig.cpp,v 1.2 2003-09-26 16:24:45 cvs Exp $";

  //  static std::string srmDummyStr("");

//=================================================================================

  //----------------------------
  // Default constructor:
  // - set default values

  SrmWsClientConfig::SrmWsClientConfig()
    : srmDebug(       false ),
      useGsiSsl(      true ),
      urlCopyScript( "bin/url-copy.sh" ), // path is relative to SRM_PATH - to fix.
      bufferSize(     2048 ),
      pushMode(       false ),
      useProxy(       true ),
      configFile(    "config.xml" ),
      gsiFtpClient(  "globus-url-copy" ),
      tcpBufferSize( 0 ),                 // '0' means do not change TCP Buffer Size

      _saveConfigOnly(false),
      _printHelpOnly(false)
      
  {
    srmProtocols.push_back( "gsissl" );
  }

//---------------------------------------------------------------------------------

  void   SrmWsClientConfig::printConfig()
  {
    cout << "Configuration:"   << endl;

    cout << "\tdebug\t\t"                 << srmDebug                     << endl;
//     cout << "\tgsissl\t\t"                << useGsiSsl                    << endl;
//     cout << "\tpushmode\t"                << pushMode                     << endl;
//     cout << "\tuse-proxy\t"               << useProxy                     << endl;

//     cout << "\tbuffer-size\t"             << bufferSize                   << endl;
//     cout << "\ttcp_buffer_size\t"         << tcpBufferSize                << endl;

//      cout << "\tsrmcphome\t"        << '"' << srmCpHome             << '"' << endl;
//     cout << "\tmapfile\t\t"        << '"' << glueMapFile           << '"' << endl;
    cout << "\twebservice_path\t"  << '"' << webServicePath        << '"' << endl;
//     cout << "\twebservice_protocol\t" << '"' << webServiceProtocol << '"' << endl;
     cout << "\turlcopy\t\t"        << '"' << urlCopyScript         << '"' << endl;

     cout << "\tprotocols["                << srmProtocols.size()   << ']' << endl;
     for( int j=0; j<srmProtocols.size(); j++ )
       cout << "\t\t"     << '"' << srmProtocols[j] << '"' << endl;

//      cout << "\tgsiftpclient\t"     << '"' << gsiFtpClient          << '"' << endl;

     cout << "\tx509_user_proxy\t"  << '"' << x509_userProxy        << '"' << endl;
     cout << "\tx509_user_cert\t"   << '"' << x509_userCert         << '"' << endl;
     cout << "\tx509_user_key\t"    << '"' << x509_userKey          << '"' << endl;
     cout << "\tx509_user_CA\t"     << '"' << x509_userTrustedCertificates << '"' << endl;

     cout << "\tconf\t\t"           << '"' << configFile            << '"' << endl;
//     cout << "\tsave_conf\t"        << '"' << saveConfigFile        << '"' << endl;
    cout << endl;
  }

  //---------------------------------------------------------------------------------
  // convert from string and assign to boolean

  void SrmWsClientConfig::_setBool( bool *pb, string& s, string& n  )
  {

    if ( pb ) {
      istringstream is( s );
      string log, end;
      bool ok(false);
    
      is >> log >> end;

      ok = true;
      if( !is && end == "" 
	  && (log == "true" || log == "false" )
	  ){
	if ( log == "true" )
	  *pb = true;
	else if ( log == "false" )
	  *pb = false;
      }else{
	ok = false;
	cerr << "Configuration file: expected 'true' or 'false' for '" << n 
	     << "', got '" << s << "'" << endl;
      }
    }
    return;
  }

  //---------------------------------------------------------------------------------
  // convert from string and assign to integer

  void SrmWsClientConfig::_setInt( int *pi, string& s, string& n  )
  {

    if ( pi ) {

      istringstream is( s );
      int    ival;
      string end;
      bool ok = false;

      is >> ival;

      if ( is ){
	ok = true;
	is >> end;
      }

      if( ok && !is && end == "" ) {
	*pi = ival;

      }else{
	ok = false;
	cerr << "Configuration file: expected int value for '" << n 
	     << "', got '" << s << "'" << endl;
      }
    }
    return;
  }

  //---------------------------------------------------------------------------------
  // copy non-empty string to string
  // leading and trailing whitespaces ingnored

  void SrmWsClientConfig::_setStr(  string *dest, string& s, string& n  )
  {
    if ( dest && !s.empty() ) {

      istringstream is( s );
      string end;
      bool ok(false);
    
      is >> s >> end;
      if( !is && end == "" ){
	ok = true;
	*dest = s;
      }else{
	ok = false;
	cerr << "Configuration file: expected 'true' or 'false' for '" << n 
	     << "', got '" << s << "'" << endl;
      }
    }
    return;
  }

  //------------------------------
  // Get configuration from file

  XERCES_CPP_NAMESPACE_USE

  int SrmWsClientConfig::readConfig()
  {
    try { 
      XMLPlatformUtils::Initialize();
    } 
    catch (const XMLException& toCatch) {
      char* message = XMLString::transcode(toCatch.getMessage());
      cout << "Error during initialization! :\n" 
	   << message << "\n"; 
      XMLString::release(&message); 
      return 1; 
    }

    XercesDOMParser* parser = new XercesDOMParser();

    parser->setValidationScheme(XercesDOMParser::Val_Always);     // optional. 
    parser->setDoNamespaces(true);                                // optional 

    ErrorHandler* errHandler = (ErrorHandler*) new HandlerBase();
    parser->setErrorHandler(errHandler);

    //---------------------------------
    // Open configuration file
    //
    const char* xmlFile = configFile.c_str();
    ifstream fin;

    // Try to open file:
    fin.open(xmlFile);

    if (fin.fail()) {
      cerr <<"Can not open the xml file: " << xmlFile << endl;
      return 2;
    }
    fin.close();

    //---------------------------------
    // Parse file
    bool gotProtocols = false;
    string protocol;

    try { 
      parser->parse(xmlFile);
    }
    catch (const XMLException& toCatch) { 
      char* message = XMLString::transcode(toCatch.getMessage()); 
      cerr << "Exception message is: \n" 
	   << message << "\n"; 
      XMLString::release(&message); 
      return -1;
    } 
    catch (const DOMException& toCatch) {
      char* message = XMLString::transcode(toCatch.msg);
      cerr << "Exception message is: \n"
	   << message << "\n";
      XMLString::release(&message); 
      return -1; 
    }
    catch (...) {
      cerr << "SrmWsClientConfig::readConfig() :" << endl;
      cerr << " unexpected Exception durung parsing '" << xmlFile <<"' file\n" ;
      return -1; 
    } 

    //Extract the document info...    
    DOMDocument *pDoc  = parser->getDocument();
    DOMElement  *pRoot = pDoc->getDocumentElement();

    //--------------------------------------------
    // Traverse XML document to get info out

    // create a walker to visit all text nodes.
    DOMNode* pCurrent = NULL;
    DOMTreeWalker* walker = pDoc->createTreeWalker(pRoot, 
						   DOMNodeFilter::SHOW_ELEMENT, NULL, true);

    int j = 0;
    for ( pCurrent = walker->nextNode(); 
	  pCurrent != 0; pCurrent = walker->nextNode() ) {

      char *strName  = XMLString::transcode( pCurrent->getNodeName() );
      char *strValue = XMLString::transcode( pCurrent->getNodeValue() );
      {
	DOMNode *child = pCurrent->getFirstChild();
	for( ; child != NULL; child = pCurrent->getNextSibling()) {
	  if(child->getNodeType() == DOMNode::TEXT_NODE)	  
	    break;   
	}

	if(child == NULL){
	  cerr << "No value found for XML element " << strName << endl; 
	  XMLString::release(&strName);
	  XMLString::release(&strValue);
	  continue;
	}

	char *strNameC  = XMLString::transcode( child->getNodeName() );
	char *strValueC = XMLString::transcode( child->getNodeValue() );

	//       std::cout << j++ 
	// 		<< " FOUND: " << strName 
	// 		<< " = [" << strValueC << "] "<<endl;

	std::string name(  strName   ); // Name  - from element
	std::string value( strValueC ); // Value - from its child

	// boolean

	if ( name == "debug" )
	  _setBool( &srmDebug,  value, name  );
	else if ( name == "gsissl" )
	  _setBool( &useGsiSsl, value, name  );
	else if ( name == "pushmode" )
	  _setBool( &pushMode,  value, name  );
	else if ( name == "useproxy" )
	  _setBool( &useProxy,  value, name  );

	// integer
 
	else if ( name == "buffer_size" )
	  _setInt( &bufferSize,  value, name  );
	else if ( name == "tcp_buffer_size" )
	  _setInt( &tcpBufferSize, value, name  );

	// string

	else if ( name == "srmcphome" )
	  _setStr( &srmCpHome, value, name  );
	else if ( name == "mapfile" )
	  _setStr( &glueMapFile, value, name  );
	else if ( name == "webservice_path" )
	  _setStr( &webServicePath, value, name  );
	else if ( name == "webservice_protocol" )
	  _setStr( &webServiceProtocol, value, name  );
	else if ( name == "urlcopy" )
	  _setStr( &urlCopyScript, value, name  );
	else if ( name == "protocols" ) {
	  _setStr( &protocol, value, name  );
	  if ( ! gotProtocols ) { // if this is first entry, erase list and then add entries one-by-one 
	    gotProtocols = true;
	    srmProtocols.erase( srmProtocols.begin(), srmProtocols.end() );
	  }
	  if ( protocol != "" )
	    srmProtocols.push_back( protocol );
	}
	else if ( name == "x509_user_proxy" )
	  _setStr( &x509_userProxy, value, name  );
	else if ( name == "x509_user_cert" )
	  _setStr( &x509_userCert, value, name  );
	else if ( name == "x509_user_key" )
	  _setStr( &x509_userKey, value, name  );
	else if ( name == "x509_user_trusted_certificates" )
	  _setStr( &x509_userTrustedCertificates, value, name  );

	else if ( name == "gsiftpclient" )
	  _setStr( &gsiFtpClient, value, name  );

	XMLString::release(&strNameC);
	XMLString::release(&strValueC);
      }
      XMLString::release(&strName);
      XMLString::release(&strValue);
    }

    //   std::cout<< '\n';

    delete parser; 
    delete errHandler; 

    return 0; 
  }

  //------------------------------------------------------------------------------
void SrmWsClientConfig::usage( std::string progname )
{
  std::cerr << "Usage: " << progname << " [OPTIONS] <source> <destination>\n";
  std::cerr << " <source> <destination> are in form of local file URLs or remote site URLs (SURLs).\n"
    "    SURLs have a form     srm://host:port/path\n"
    "    file  have a form     file:///path\n"
    "    either source(s) or destination must be srm URL\n"
    "    currently one of URLs shall be file URL - no remote SURL to SURL transfers\n";

  std::cerr << endl << "Command line OPTIONS:\n";
 
  std::cerr << "  --debug[=<true|false>]\n";
//   std::cerr << "  --srmcphome=<path>            set path to 'srmcp' product directory\n";
//   std::cerr << "  --gsissl[=<true|false>]       use gsi https, default is true\n";
//   //-mapfile=<mapfile> to specify glue mapfile
  std::cerr << "  --webservice_path=<webservice_path>   specify web service path component\n";
  std::cerr << "                                    of web service URL (e.g. ""srm/managerv1.wsdl"")\n";
//   std::cerr << "  --webservice_protocol=<webservice_protocol>  specify the webservice protocol\n";
//   std::cerr << "                                    e.g. ""http,""https"" or ""httpg""\n";
  std::cerr << "  --urlcopy=<urlcopy_script>    specify the path/scriptname for the universal 'url_copy' script\n";
  std::cerr << "                                    default is $SRM_PATH/bin/url-copy.sh\n";
//   std::cerr << "  --buffer_size=<integer>       set the buffer size different from default(2048)\n";
  std::cerr << "  --protocols=<protocol1> [--protocols=<protocol2] \n";
  std::cerr << "                                add protocol(s) to the list of supported TURL protocols\n";
  std::cerr << "  --protocols=                  to reset list of supported TURL protocols in configuration\n";

//   std::cerr << "  --pushmode[=<true|false>]     use push mode for srm Mass Storage Systems (MSS) to MSS copy (true) or\n";
//   std::cerr << "                                    use the pull mode (false is default)\n";
//   std::cerr << "  --use_proxy[=<true|false>]    use grid proxy (default),\n"
//                "                                    otherwise  use certificate and key directly\n";
//   std::cerr << "\n"; 

  std::cerr << "  --x509_user_proxy=<path>      set path to user grid proxy>\n";
  std::cerr << "  --x509_user_cert=<path>       set path to user grid certificate>\n";
  std::cerr << "  --x509_user_key=<path>        set path to user private key>\n";
  std::cerr << "  --x509_user_CA=<path>         set path to the directory with cerificates\n"
                "                                    of trusted Certificate Authorities (CAs)\n";

  std::cerr << "  --conf=<path>                 set path to the configuration file, default is 'config.xml'\n";
//   std::cerr << "  --save_conf=<path>            set path to the file in which the new configuration will be saved\n"
//                "                                    no transfer will be performed if this option is specified\n";
  std::cerr << "  --help                        print this help \n";
}
  //------------------------------------------------------------------------------
  // Get optional boolean 'true' or 'false'
  // if boolean not present, returns default value 'dflt'
  //
  bool SrmWsClientConfig::_optGetBool( char* arg, bool dflt )
  {
    bool ret = dflt;
    if ( arg == NULL )
      return dflt;
    {
      string s( arg );

      if ( s == "true" )
	ret = true;
      else if ( s == "false" )
	ret = false;
      else if ( s == "" )
	ret = dflt;
      else{
	usage( progname );
	exit( 1 );
      }
    }
    return ret;
  }
  //------------------------------------------------------------------------------
  // Do initial scan of arguments before configuration file has been read
  //
  int SrmWsClientConfig::parseCLOptions0( int argc, char **argv ) 
  {
    const int optHelp            =  1;
    const int optDebug           =  2;
    const int optConfigFile      =  3;
    const int optSkip            = 99;

    static struct option long_options0[] =
      {
	{"help",            no_argument,        0,  optHelp},
	{"conf",            required_argument,  0,  optConfigFile },
       	{"debug",           optional_argument,  0,  optDebug},
	//---
	{"srmcphome",       required_argument,  0,  optSkip },
	{"gsissl",          optional_argument,  0,  optSkip },
	{"mapfile",         required_argument,  0,  optSkip },
	{"webservice_path", required_argument,  0,  optSkip },
	{"webservice_protocol", required_argument,  0,  optSkip },
	{"urlcopy",         required_argument,  0,  optSkip },

	{"buffer-size",     required_argument,  0,  optSkip },
	{"protocols",       required_argument,  0,  optSkip },
	{"pushmode",        optional_argument,  0,  optSkip },
	{"use-proxy",       optional_argument,  0,  optSkip },

	{"x509_user_proxy", required_argument,  0,  optSkip },
	{"x509_user_cert",  required_argument,  0,  optSkip },
	{"x509_user_key",   required_argument,  0,  optSkip },
	{"x509_user_CA",    required_argument,  0,  optSkip },

	{"save_conf",       required_argument,  0,  optSkip },

	{ 0, 0, 0, 0}
      };

    int option_index = 0;
    int c;

    progname = argv[0];

    while ((c = getopt_long (argc, argv, "",
			     long_options0, &option_index)) >= 0)
      {
	std::string option = long_options0[option_index].name;

// 	if( srmDebug )
// 	  cout << "DEBUG:: getopt_long ret = " << c << endl;

	switch (c) {
	case 0:
	  /* If this options set a flag in a 'struct option', do nothing else now. */
	  if ( long_options0[option_index].flag != 0 ) 
	    break;

	  cout << "SrmGetOptions() case 0 option " << option;
	  if ( optarg )
	    cout << " with arg " << optarg;
	  cout << endl;
	  break;

	case optHelp:
	  _printHelpOnly = true;
	  usage( progname );
	  exit( 0 );                 // Print Usage and exit ( forget destructors, huh ? )
	  break;

	case optConfigFile:
	  configFile =  optarg;
	  break;

	case optDebug:
	  srmDebug = _optGetBool( optarg, true );
	  break;

	case optSkip:
	  break;                     // skip all other arguments for the first pass

	default:
	  break;                     // skip all other arguments for the first pass
	}
      }
    return optind;
  }

  //------------------------------------------------------------------------------
  int SrmWsClientConfig::parseCLOptions( int argc, char **argv ) 
  {
    const int optHelp            =  1;
    const int optCpHome          =  2;
    const int optUseGsiSsl       =  3;
    const int optGlueMapFile     =  4;
    const int optWebSrvPath      =  5;
    const int optWebSrvProtocol  =  6;
    const int optUrlCopyScript   =  7;

    const int optBufferSize      =  8;
    const int optSrmProtocols    =  9;
    const int optPushMode        = 10;
    const int optUseProxy        = 11;

    const int optX509Proxy       = 20;
    const int optX509Cert        = 21;
    const int optX509Key         = 22;
    const int optX509CA          = 23;

    const int optConfigFile      = 30;
    const int optSaveConfig      = 31;

    const int optDebug           = 99;

    static struct option long_options[] =
      {
	{"help",            no_argument,        0,  optHelp},

	{"srmcphome",       required_argument,  0,  optCpHome },
	{"gsissl",          optional_argument,  0,  optUseGsiSsl },
	{"mapfile",         required_argument,  0,  optGlueMapFile },
	{"webservice_path", required_argument,  0,  optWebSrvPath },
	{"webservice_protocol", required_argument,  0,  optWebSrvProtocol },
	{"urlcopy",         required_argument,  0,  optUrlCopyScript },

	{"buffer-size",     required_argument,  0,  optBufferSize },
	{"protocols",       optional_argument,  0,  optSrmProtocols },
	{"pushmode",        optional_argument,  0,  optPushMode },
	{"use-proxy",       optional_argument,  0,  optUseProxy },

	{"x509_user_proxy", required_argument,  0,  optX509Proxy },
	{"x509_user_cert",  required_argument,  0,  optX509Cert },
	{"x509_user_key",   required_argument,  0,  optX509Key },
	{"x509_user_CA",    required_argument,  0,  optX509CA },

	{"conf",            required_argument,  0,  optConfigFile },
	{"save_conf",       required_argument,  0,  optSaveConfig },

	{"debug",           optional_argument,  0,  optDebug},
	{ 0, 0, 0, 0}
      };
    
    int option_index = 0;
    int c;

    bool gotProtocols = false;
    progname = argv[0];

    optind = 0; // Start over; 'optind' is extern in common namespace. 

    while ((c = getopt_long (argc, argv, "",
			     long_options, &option_index)) >= 0)
      {
	std::string option = long_options[option_index].name;

// 	if( srmDebug )
// 	  cout << "DEBUG:: getopt_long ret = " << c << endl;

	switch (c) {
	case 0:
	  /* If this options set a flag in a 'struct option', do nothing else now. */
	  if ( long_options[option_index].flag != 0 ) 
	    break;

	  cout << "SrmGetOptions() case 0 option " << option;
	  if ( optarg )
	    cout << " with arg " << optarg;
	  cout << endl;

	  usage( progname ); // currently there are no such entries in the table - then it's a bug
	  exit( 0 );
	  break;

	case optHelp:
	  _printHelpOnly = true;
	  usage( progname );
	  exit( 0 );
	  break;

	case optCpHome:
	  srmCpHome =  optarg;
	  break;
	case optUseGsiSsl:
	  useGsiSsl = _optGetBool( optarg, true );
	  break;
	case optGlueMapFile:
	  glueMapFile =  optarg;
	  break;
	case optWebSrvPath:
	  webServicePath =  optarg;
	  break;
	case optWebSrvProtocol:
	  webServiceProtocol = optarg;
	  break;
	case optUrlCopyScript:
	  urlCopyScript  =  optarg;
	  break;

	case optBufferSize:
	  bufferSize = std::atoi (optarg);  // to do: check bufferSize is not 0, so it was converted correctly 
	  break;
	case optSrmProtocols:
	  if ( ! gotProtocols ) { // if this is first entry, erase list and then add entries one-by-one 
	    gotProtocols = true;
	    srmProtocols.erase( srmProtocols.begin(), srmProtocols.end() );
	  }
	  if ( optarg && *optarg  )
	    srmProtocols.push_back( optarg );
	  else                                     // got "--protocols="  - start over
	    srmProtocols.erase( srmProtocols.begin(), srmProtocols.end() );

	  break;
	case optPushMode:
	  pushMode = _optGetBool( optarg, true );
	  break;
	case optUseProxy:
	  useProxy = _optGetBool( optarg, true );
	  break;

	case optX509Proxy :
	  x509_userProxy =  optarg;
	  break;
	case optX509Cert :
	  x509_userCert  =  optarg;
	  break;
	case optX509Key :
	  x509_userKey   =  optarg;
	  break;
	case optX509CA :
	  x509_userTrustedCertificates =  optarg;
	  break;

	case optConfigFile:
	  configFile =  optarg;
	  break;
	case optSaveConfig:
	  saveConfigFile  =  optarg;
	  _saveConfigOnly =  true;
	  break;

	case optDebug:
	  srmDebug = _optGetBool( optarg, true );
	  break;

	default:
	  if( srmDebug )
	    cout << "DEBUG: default case - option " << option;
	  if ( optarg )
	    cout << " with arg " << optarg;
	  cout << endl;

	  usage( progname );
	  exit(1);
	  break;
	}
      }
    return optind;
  }

} // namespace srm

