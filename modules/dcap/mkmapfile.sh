#!/bin/sh
#
# $Id: mkmapfile.sh,v 1.2 2003-07-24 10:02:22 tigran Exp $
#

awk '
BEGIN{
	printf("DCAP {\n");
	printf("\tglobal:\n");
	inComment = 0;
}



$1 !~ /^#/ {

	

	for( i = 3; i <= NF; i++) {
	
		
		_token = $i;
		
		if( index(_token, "(" ) != 0 ) {
			_token = substr( _token, 1, index( _token, "(" ) - 1 );
		}
		
		if(  index(_token, "*" ) != 0) {
			_token = substr( _token, index(_token, "*" ) +1 );
		}
		
		if( _token == "extern" )
			continue;
		if( _token == "void" )
			continue;
		if( _token == "int" )
			continue;
		if( _token == "char" )
			continue;
		if( _token == "off_t" )
			continue;
		if( _token == "ssize_t" )
			continue;
		if( _token == "*" )
			continue;
		if( _token == "const" )
			continue;
		if( _token == "time_t" )
			continue;
		if( _token == "unsigned" )
			continue;
		if( _token == "FILE" )
			continue;
		if( _token == "struct" )
			continue;
		if( _token == "stat" )
			continue;
		if( _token == "stat64" )
			continue;			
		
		if (_token ~ /([\)\;\,])/ )
			continue;
			
		if ( _token !~ /([a-zA-Z]+)/ )
			continue;
			
			printf("\t\t%s;\n",_token);
	}
	
}

END{
	printf("\tlocal:\n");
	printf("\t\t*;\n");
	printf("};\n");

}

'
