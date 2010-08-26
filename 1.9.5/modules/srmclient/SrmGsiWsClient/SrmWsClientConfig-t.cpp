#include <stdio.h>

#include "SrmWsClientConfig.hpp"

  // Keep track of the file CVS version in the binary file
  static char cvsid[] = "$Id: SrmWsClientConfig-t.cpp,v 1.2 2003-09-26 16:25:44 cvs Exp $";

//---------------------------------------------------------------------------------

using namespace srm;

int main (int argc, char* argv[]) 
{ 
  int   ind;
  class SrmWsClientConfig *cfg = new SrmWsClientConfig();

  cout << "Command line args #" << argc << endl;
  for (int j=0; j<argc; j++ )
    cout << "arg[" << j << "]: " << argv[j] << endl; 

  cout << "After Initialization:" << endl;
  cfg->printConfig();  // Print initial configuration:

  //--------------------
  cout << "Parse Command Line Options - prescan " << endl;
  ind = cfg->parseCLOptions0( argc, argv );
  cfg->printConfig();  // Print updated configuration:

  //--------------------
  cout << "Read Configuration file ..." << endl;
  cfg->readConfig();
  cfg->printConfig();  // Print updated configuration:
  //--------------------

  cout << "Parse Command Line Options ... " << endl;

  cout << "Command line args #" << argc << endl;
  for (int j=0; j<argc; j++ )
    cout << "arg[" << j << "]: " << argv[j] << endl; 
  cout.flush();

  ind = cfg->parseCLOptions( argc, argv );

  if ( ind < argc ){
    cout << endl;
    cout << "More Arguments:" << endl;
    while( ind < argc )
      cout << "\t" << argv[ind++] << endl;
    cout << endl;
  }

  //--------------------
  cout << "Finally: " << endl;
  cfg->printConfig();  // Print updated configuration:

  delete cfg;
}
