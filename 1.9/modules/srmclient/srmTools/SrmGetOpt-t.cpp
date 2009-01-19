//
// File: SrmGetOpt.cpp
// Purpose: Browse Command Line Options for SRM Copy.
//          Usage() for srmcp.cpp
//
// Created: Sep, 2003, aik
//

#include <iostream>
#include <string>

#include <stdio.h>
#include <stdlib.h>
#include <getopt.h>

namespace srm {
//============================================

  // Keep track of version in the binary file

  static char cvsid[] = "$Id: SrmGetOpt-t.cpp,v 1.1 2003-09-24 19:58:24 cvs Exp $";

  static std::string progname;

  static bool        srmDebug  =false;
  static std::string srmCpHome("");
  static bool        useGsiSsl =true;
  static std::string glueMapFile("");;
  static std::string webServicePath("");
  static std::string webServiceProtocol("");
  static std::string urlCopyScript("bin/url-copy.sh"); // path relative to SRM_PATH - to fix.
  static int         bufferSize(2048);
  static std::string srmProtocols("gsissl");
  static bool        pushMode =false;
  static bool        useProxy =true;

  static std::string x509_userProxy("");
  static std::string x509_userCert("");
  static std::string x509_userKey("");
  static std::string x509_userTrustedCertificates("");

  static std::string configFile("config.xml");
  static std::string saveConfigFile("");
  static bool        saveConfigOnly =false;
  static bool        printHelpOnly  =false;

//--------------------------------------------------------------------
  static bool optGetBool( char* arg, bool dflt );
//--------------------------------------------------------------------

void usage (std::string progname, int ret)
{
  std::cerr << "Usage: " << progname << " [OPTIONS] [<source>...] <source> <destination>\n";
  std::cerr << " <source> ... <destination> - list of sources and one destination in form of \n"
    "    local file URLs or remote site URLs (SURLs).\n"
    "    SURLs have a form \n"
    "    \tsrm://host:port/path\n"
    "    either source(s) or destination or both must be srm URL\n";

  std::cerr << endl << "Command line OPTIONS:\n";
 
  std::cerr << "  --debug[=<true|false>]\n";
  std::cerr << "  --srmcphome=<path>            set path to 'srmcp' product directory\n";
  std::cerr << "  --gsissl[=<true|false>]       use gsi https, default is true\n";
  //-mapfile=<mapfile> to specify glue mapfile
  std::cerr << "  --webservice_path=<webservice_path>   specify web service path component\n";
  std::cerr << "                                    of web service URL (e.g. ""srm/managerv1.wsdl"")\n";
  std::cerr << "  --webservice_protocol=<webservice_protocol>  specify the webservice protocol\n";
  std::cerr << "                                    e.g. ""http,""https"" or ""httpg""\n";
  std::cerr << "  --urlcopy=<urlcopy_script>    specify the path/scriptname for the universal 'url_copy' script\n";
  std::cerr << "                                    default is $SRM_PATH/bin/url-copy.sh\n";
  std::cerr << "  --buffer_size=<integer>       set the buffer size different from default(2048)\n";
  std::cerr << "  --protocols=protocol1[,protocol2[...]] \n";
  std::cerr << "                                comma separated list of supported TURL protocols\n";

  std::cerr << "  --pushmode[=<true|false>]     use push mode for srm Mass Storage Systems (MSS) to MSS copy (true) or\n";
  std::cerr << "                                    use the pull mode (false is default)\n";
  std::cerr << "  --use_proxy[=<true|false>]    use grid proxy (default),\n"
               "                                    otherwise  use certificate and key directly\n";
  std::cerr << "\n"; 

  std::cerr << "  --x509_user_proxy=<path>      set path to user grid proxy>\n";
  std::cerr << "  --x509_user_cert=<path>       set path to user grid certificate>\n";
  std::cerr << "  --x509_user_key=<path>        set path to user private key>\n";
  std::cerr << "  --x509_user_CA=<path>         set path to the directory with cerificates\n"
               "                                    of trusted Certificate Authorities (CAs)\n";

  std::cerr << "  --conf=<path>                 set path to the configuration file, default is 'config.xml'\n";
  std::cerr << "  --save_conf=<path>            set path to the file in which the new configuration will be saved\n"
               "                                    no transfer will be performed if this option is specified\n";
  std::cerr << "  --help                        print this help \n";
  //  std::cerr << "  --help or -h                  print this help \n";

  std::exit (ret);
}

int SrmGetOptions( int argc, char **argv ) 
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
    {"protocols",       required_argument,  0,  optSrmProtocols },
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

  progname = argv[0];

  while ((c = getopt_long (argc, argv, "",
			   long_options, &option_index)) >= 0)
  {
    std::string option = long_options[option_index].name;

    cout << "DEBUG:: getopt_long ret = " << c << endl;

    switch (c) {
    case 0:
      /* If this options set a flag in a 'struct option', do nothing else now. */
      if ( long_options[option_index].flag != 0 ) 
	break;

      cout << "SrmGetOptions() case 0 option " << option;
      if ( optarg )
	cout << " with arg " << optarg;
      cout << endl;

      usage (progname, 0); // currently there are no such entries in the table - then it's a bug
      break;

    case optHelp:
      printHelpOnly = true;
      usage (progname, 0);
      break;

    case optCpHome:
      srmCpHome =  optarg;
      break;
    case optUseGsiSsl:
      useGsiSsl = optGetBool( optarg, true );
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
      srmProtocols   = optarg;
      break;
    case optPushMode:
      pushMode = optGetBool( optarg, true );
      break;
    case optUseProxy:
      useProxy = optGetBool( optarg, true );
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
      saveConfigOnly  =  true;
      break;

    case optDebug:
      srmDebug = optGetBool( optarg, true );
      break;

    default:
      cout << "DEBUG: default case - option " << option;
      if ( optarg )
	cout << " with arg " << optarg;
      cout << endl;

      usage (progname, 1);
      break; // just in case - usage() does exit()
    }
  }
  return optind;
}

// Get optional boolean 'true' or 'false'
// if boolean not present, returns default value 'dflt'
//
bool optGetBool( char* arg, bool dflt )
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
      usage( progname, 1 );
      // it exits in 'usage()'
    }
  }
  return ret;
}

void SrmPrintOptions()
{
  cout << "Configuration:"   << endl;

  cout << "\tdebug\t\t"                 << srmDebug                     << endl;
  cout << "\tgsissl\t\t"                << useGsiSsl                    << endl;
  cout << "\tpushmode\t"                << pushMode                     << endl;
  cout << "\tuse-proxy\t"               << useProxy                     << endl;
  cout << "\tbuffer-size\t"             << bufferSize                   << endl;

  cout << "\tsrmcphome\t"        << '"' << srmCpHome             << '"' << endl;
  cout << "\tmapfile\t\t"        << '"' << glueMapFile           << '"' << endl;
  cout << "\twebservice_path\t"  << '"' << webServicePath        << '"' << endl;
  cout << "\twebservice_protocol\t" << '"' << webServiceProtocol << '"' << endl;
  cout << "\turlcopy\t\t"        << '"' << urlCopyScript         << '"' << endl;
  cout << "\tprotocols\t"        << '"' << srmProtocols          << '"' << endl;

  cout << "\tx509_user_proxy\t"  << '"' << x509_userProxy        << '"' << endl;
  cout << "\tx509_user_cert\t"   << '"' << x509_userCert         << '"' << endl;
  cout << "\tx509_user_key\t"    << '"' << x509_userKey          << '"' << endl;
  cout << "\tx509_user_CA\t"     << '"' << x509_userTrustedCertificates << '"' << endl;

  cout << "\tconf\t\t"           << '"' << configFile            << '"' << endl;
  cout << "\tsave_conf\t"        << '"' << saveConfigFile        << '"' << endl;
  cout << endl;
}

} // namespace srm

int
main( int argc, char **argv ) 
{
  int ind;

  ind = srm::SrmGetOptions( argc, argv );
  srm::SrmPrintOptions();

  if ( ind < argc ){
    cout << endl;
    cout << "Arguments:" << endl;
    while( ind < argc )
      cout << "\t" << argv[ind++] << endl;
    cout << endl;
  }
  return 0;
}
