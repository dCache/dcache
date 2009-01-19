//
//
// $Id: url.cpp,v 1.1 2003-09-24 19:58:25 cvs Exp $
//

#include <string>

#include "url.hpp"

// A la globus_url.c, but 
//   * does not check validity of the symbols (alphanumeric, etc.) 
//   * uppper/lower case of the symbols
//
// Parses URLs of the form
//  <scheme>://[<user>[:<password>]@]<host>[:<port>]/<url-path>
//
// 1) find "://", get scheme
// 2) in the rest, find "/", get hostport and urlPath
// 3) parse hostport:
//      find "@", it is "userPass@hostport" or "hostport"
// 3a) if userPass present, find ":" to get "user" and "pass"
// 3b) find ":" to get "user" and "port"
//
namespace srm {

url::url( const string& URL )
  //  : _url( URL )
{
  string s;
  int pos;

  _url =  URL;
  // Get schema/protocol part
  pos = _url.find( "://" );

  if ( pos != string::npos ) {
    _scheme = _url.substr( 0, pos );
    s       = _url.substr( pos+3, _url.size() );

    // Get user path part
    pos = s.find( "/" );
    if ( pos != string::npos ) {
      _urlPath = s.substr( pos+1, s.size() );
      s = s.substr( 0, pos );
      _hostport = s;

      // Try extra: 
      pos = s.find( "@" );
      if ( pos == string::npos ) {
	_hostport = s;
      }else{
	_userpass = s.substr( 0, pos );
	_hostport = s.substr( pos+1, s.size() );
	
	// Find ":" in _userpass
	pos = _userpass.find( ":" );
	if ( pos == string::npos ) {
	  _user   = _userpass;
	}else{
	  _user   = _userpass.substr( 0, pos );
	  _pass   = _userpass.substr( pos+1, s.size() );
	}	
      }

      // Find ":" in hostport
      pos = _hostport.find( ":" );
      if ( pos == string::npos ) {
	_host     = _hostport;
      }else{
	_host     = _hostport.substr( 0, pos );
	_port     = _hostport.substr( pos+1, s.size() );
      }
    }
  }
}

} // namespace srm
