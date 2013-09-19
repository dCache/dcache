// $Id: SrmWsClientConfig.hpp,v 1.2 2003-09-26 16:24:45 cvs Exp $
//
#ifndef _SrmWsClientConfig_hpp_defined_
#define _SrmWsClientConfig_hpp_defined_

#include <string>
#include <vector>

namespace srm {

  class SrmWsClientConfig{
  public:
    SrmWsClientConfig();

    void printConfig();
    int  readConfig();

    void usage( std::string progname ); // prints usage. It does NOT do exit !!!
    int  parseCLOptions0( int argc, char **argv ); // First pass in parsing options to detect opt. before conf file
    int  parseCLOptions(  int argc, char **argv ); // Full blown second pass in parsing command line options

    std::string progname;

    bool        srmDebug;
    std::string srmCpHome;
    bool        useGsiSsl;
    std::string glueMapFile;
    std::string webServicePath;
    std::string webServiceProtocol;
    std::string urlCopyScript; 

    int         bufferSize;
    vector<std::string> srmProtocols;
    bool        pushMode;
    bool        useProxy;

    std::string x509_userProxy;
    std::string x509_userCert;
    std::string x509_userKey;
    std::string x509_userTrustedCertificates;

    std::string configFile;
    std::string saveConfigFile;
    std::string gsiFtpClient;
    int         tcpBufferSize;
  private:
    // Get Command Line Options helpers
    bool _optGetBool( char* arg, bool dflt );

    // Read XML configuration file  helpers
    void _setBool( bool   *pb,   string& s, string& n  );
    void _setInt(  int    *pi,   string& s, string& n  );
    void _setStr(  string *dest, string& s, string& n  );

    bool _saveConfigOnly;
    bool _printHelpOnly;
  };

} // namespace srm

#endif // _SrmWsClientConfig_hpp_defined_
