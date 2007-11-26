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

//---------------------------------------------------------------------------------

namespace srm {

  // Keep track of version in the binary file
  static char cvsid[] = "$Id: XMLParser-t.cpp,v 1.1 2003-09-24 19:58:24 cvs Exp $";

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

  static std::string gsiFtpClient("globus-url-copy");
  static int         tcpBufferSize(0);  // '0' meands do not change

  static std::string srmDummyStr("");

//---------------------------------------------------------------------------------

void SrmPrintOptions()
{
  cout << "Configuration:"   << endl;

  cout << "\tdebug\t\t"                 << srmDebug                     << endl;
  cout << "\tgsissl\t\t"                << useGsiSsl                    << endl;
  cout << "\tpushmode\t"                << pushMode                     << endl;
  cout << "\tuse-proxy\t"               << useProxy                     << endl;

  cout << "\tbuffer-size\t"             << bufferSize                   << endl;
  cout << "\ttcp_buffer_size\t"         << tcpBufferSize                << endl;

  cout << "\tsrmcphome\t"        << '"' << srmCpHome             << '"' << endl;
  cout << "\tmapfile\t\t"        << '"' << glueMapFile           << '"' << endl;
  cout << "\twebservice_path\t"  << '"' << webServicePath        << '"' << endl;
  cout << "\twebservice_protocol\t" << '"' << webServiceProtocol << '"' << endl;
  cout << "\turlcopy\t\t"        << '"' << urlCopyScript         << '"' << endl;

  cout << "\tprotocols\t"        << '"' << srmProtocols          << '"' << endl;
  cout << "\tgsiftpclient\t"     << '"' << gsiFtpClient          << '"' << endl;

  cout << "\tx509_user_proxy\t"  << '"' << x509_userProxy        << '"' << endl;
  cout << "\tx509_user_cert\t"   << '"' << x509_userCert         << '"' << endl;
  cout << "\tx509_user_key\t"    << '"' << x509_userKey          << '"' << endl;
  cout << "\tx509_user_CA\t"     << '"' << x509_userTrustedCertificates << '"' << endl;

  cout << "\tconf\t\t"           << '"' << configFile            << '"' << endl;
  cout << "\tsave_conf\t"        << '"' << saveConfigFile        << '"' << endl;
  cout << endl;
}

} // namespace srm

//---------------------------------------------------------------------------------
// convert from string and assign to boolean

void setBool( bool *pb, string& s, string& n  )
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

void setInt( int *pi, string& s, string& n  )
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

void setStr(  string *dest, string& s, string& n  )
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

//---------------------------------------------------------------------------------

using namespace srm;

XERCES_CPP_NAMESPACE_USE

int main (int argc, char* argv[]) 
{ 
  srm::SrmPrintOptions();
  //---------------------
  // Now get configuration from file
  
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

  parser->setValidationScheme(XercesDOMParser::Val_Always); // optional. 
  parser->setDoNamespaces(true); // optional 

  ErrorHandler* errHandler = (ErrorHandler*) new HandlerBase();
  parser->setErrorHandler(errHandler);

  char* xmlFile = "config.xml";

  ifstream fin;

  // Try to open file:
  fin.open(xmlFile);

  if (fin.fail()) {
    cerr <<"Cannot open the xml file: " << xmlFile << endl;
    return 2;
  }
  fin.close();


  try { 
    parser->parse(xmlFile);
  }
  catch (const XMLException& toCatch) { 
    char* message = XMLString::transcode(toCatch.getMessage()); 
    cout << "Exception message is: \n" 
	 << message << "\n"; 
    XMLString::release(&message); 
    return -1;
  } 
  catch (const DOMException& toCatch) {
    char* message = XMLString::transcode(toCatch.msg);
    cout << "Exception message is: \n"
	 << message << "\n";
    XMLString::release(&message); 
    return -1; 
  }
  catch (...) {
    cout << argv[0] << " : ";
    cout << "Unexpected Exception durung parsing '" << xmlFile <<"' file\n" ;
    return -1; 
  } 

  
  //Extract the document info...    
  DOMDocument *pDoc  = parser->getDocument();
  DOMElement  *pRoot = pDoc->getDocumentElement();

  // create a walker to visit all text nodes.
  DOMNode* pCurrent = NULL;
  DOMTreeWalker* walker = pDoc->createTreeWalker(pRoot, 
						 DOMNodeFilter::SHOW_ELEMENT, NULL, true);
  //						 DOMNodeFilter::SHOW_TEXT, NULL, true);

  // use the tree walker to print out the text nodes.
  std::cout<< "Parse configuration file:\n";
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
	cout << "No value found for XML element " << strName << endl; 
	XMLString::release(&strName);
	XMLString::release(&strValue);
	continue;
      }

      char *strNameC  = XMLString::transcode( child->getNodeName() );
      char *strValueC = XMLString::transcode( child->getNodeValue() );

      std::cout << j++ 
		<< " FOUND: " << strName 
		<< " = [" << strValueC << "] "<<endl;

      std::string name(  strName   ); // Name  - from element
      std::string value( strValueC ); // Value - from its child
//        long int
//        strtol(const char *nptr, char **endptr, int base);


      if ( name == "debug" )
        setBool( &srmDebug,  value, name  );
      else if ( name == "gsissl" )
        setBool( &useGsiSsl, value, name  );
      else if ( name == "pushmode" )
        setBool( &pushMode,  value, name  );
      else if ( name == "useproxy" )
        setBool( &useProxy,  value, name  );

      // to do - convert to int with
 
      else if ( name == "buffer_size" )
        setInt( &bufferSize,  value, name  );
      else if ( name == "tcp_buffer_size" )
        setInt( &tcpBufferSize, value, name  );

      else if ( name == "srmcphome" )
        setStr( &srmCpHome, value, name  );
      else if ( name == "mapfile" )
	setStr( &glueMapFile, value, name  );
      else if ( name == "webservice_path" )
	setStr( &webServicePath, value, name  );
      else if ( name == "webservice_protocol" )
	setStr( &webServiceProtocol, value, name  );
      else if ( name == "urlcopy" )
	setStr( &urlCopyScript, value, name  );
      else if ( name == "protocols" )
	setStr( &srmProtocols, value, name  );

      else if ( name == "x509_user_proxy" )
	setStr( &x509_userProxy, value, name  );
      else if ( name == "x509_user_cert" )
	setStr( &x509_userCert, value, name  );
      else if ( name == "x509_user_key" )
	setStr( &x509_userKey, value, name  );
      else if ( name == "x509_user_trusted_certificates" )
	setStr( &x509_userTrustedCertificates, value, name  );

      else if ( name == "gsiftpclient" )
	setStr( &gsiFtpClient, value, name  );


      XMLString::release(&strNameC);
      XMLString::release(&strValueC);

    }

    XMLString::release(&strName);
    XMLString::release(&strValue);
  }

  std::cout<< '\n';

  delete parser; 
  delete errHandler; 

  //--------------------
  // Print updated configuration:

  srm::SrmPrintOptions();

  return 0; 
}
