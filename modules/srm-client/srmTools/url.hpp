//
//
// $Id: url.hpp,v 1.1 2003-09-24 19:58:25 cvs Exp $
//

#ifndef _url_hpp_defined_
#define _url_hpp_defined_

#include <string>

//
// Header file for url.cpp
//
// Parses URLs of the form
//  <scheme>://[<user>[:<password>]@]<host>[:<port>]/<url-path>
//
//   * does not check validity of the symbols (alphanumeric, etc.) 
//   * uppper/lower case of the symbols
//

namespace srm {

class url {
public:
  url( const string& URL );

  string getURL()   { return _url; };  

  string scheme()   { return _scheme; };
  string user()     { return _user; };
  string pass()     { return _pass; };
  string host()     { return _host; };
  string port()     { return _port; };
  string urlPath()  { return _urlPath; };
  
private:
  string _scheme;  // a.k.a. 'protocol'
  string _user;
  string _pass;
  string _host;
  string _port;
  string _urlPath;

  string _url;
  string _userpass;
  string _hostport;
};

}
#endif // _url_hpp_defined_
