//
//
// $Id: url-t.cpp,v 1.1 2003-09-24 19:58:25 cvs Exp $
//


#include <stdio.h>
#include <iostream.h>
#include <string>

#include "url.hpp"

using namespace srm;

main( int argc, char *argv[] )
{
  cout << "Argc = " << argc << endl;

  if ( argc < 2 ) {
    cout << "Args ??" << endl;
    exit(1);
  }
  
  string sURL = argv[1];

  cout << "URL = " << sURL << endl;

  class url u( sURL );

  cout << endl;
  cout << "Got URL  = " << u.getURL()   << endl;
  cout << "scheme   = " << u.scheme()   << endl;
  cout << "user     = " << u.user()     << endl;
  cout << "pass     = " << u.pass()     << endl;
  cout << "host     = " << u.host()     << endl;
  cout << "port     = " << u.port()     << endl;
  cout << "urlPath  = " << u.urlPath() << endl;
}
