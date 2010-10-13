/*
 *   DCAP - dCache Access Protocol client interface
 *
 *   Copyright (C) 2000,2004 DESY Hamburg DMG-Division.
 *
 *   AUTHOR: Tigran Mkrtchayn (tigran.mkrtchyan@desy.de)
 *
 *   This program can be distributed under the terms of the GNU LGPL.
 *   See the file COPYING.LIB
 *
 */


/*
 * $Id: dcap_debug.c,v 1.33 2006-09-22 13:38:07 tigran Exp $
 */

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdarg.h>

#include "system_io.h"
#include "dcap.h"
#include "debug_level.h"
#include "dcap_debug.h"

/* FIXME: declaration of debugMap belongs in debug_map.h */

typedef struct {
	int debugLevel;
	const char *str;
} debugMap;

#include "debug_map.h"

static unsigned int debugLevel = DC_ERROR; /* default debug level - ERROR only */
static int isChangable = 1;
static int debug_stream = -1;

/* Local function prototypes */
static void init_dc_debug();
static int string2debugLevel( const char *str);
static void emit_debug(const char *format, va_list ap);
static void conditional_initialize();

void dc_setDebugLevel(unsigned int newLevel)
{
	if(isChangable) {
		if(newLevel == DC_NO_OUTPUT ) {
			debugLevel = DC_NO_OUTPUT;
		}else{
			debugLevel |= newLevel;
		}
	}
}

/* this is a quick hack to get a more information when someting goes wrong */
void dc_setRecoveryDebugLevel()
{
		void dc_debug( unsigned int, const char *, ...);

		debugLevel |= DC_ERROR|DC_INFO; /* at least have this two ... */
		dc_debug( DC_INFO , "\n!!! LibDCAP Debug Level modified by error recovery procedure !!!");
}

int string2debugLevel( const char *str)
{

	int i;

	/* try to look at as number */

	i = atoi(str);

	if( i != 0 ) {
		return i;
	}

	/* if zero - also fine */
	if( ( i == 0 ) && ( strcmp(str, "0" ) == 0 ) ) {
		return 0;
	}

	/* if not - what it is ? */

	for( i = 0; debugMapArray[i].debugLevel != -1 ; i++ ) {
#ifdef WIN32
		if( strcmp(str, debugMapArray[i].str) == 0 ) {
#else
		if( strcasecmp(str, debugMapArray[i].str) == 0 ) {
#endif
			return debugMapArray[i].debugLevel;
		}
	}

	/* nothing ! */

	return -1;

}

void dc_setStrDebugLevel(const char *str)
{
	int rc;

	rc = string2debugLevel(str);
	if(rc != -1 ) {
		dc_setDebugLevel( rc );
	}

}

void dc_debug(unsigned int level, const char *format, ...)
{
	va_list args;

	if(dc_is_debug_level_enabled(level)) {
		va_start(args, format);
		emit_debug(format, args);
		va_end(args);
	}
}

void dc_vdebug(unsigned int level, const char *format, va_list ap)
{
	if(dc_is_debug_level_enabled(level)) {
		emit_debug(format, ap);
	}
}

int dc_is_debug_level_enabled( unsigned int level)
{
	conditional_initialize();

	return level & debugLevel;
}

void conditional_initialize()
{
	if(debug_stream == -1) {
		init_dc_debug();
	}
}


void emit_debug(const char *format, va_list ap)
{
	char msg[MAX_MESSAGE_LEN];
	int len;

#ifndef WIN32
	len = vsnprintf(msg, MAX_MESSAGE_LEN, format, ap);
#else
	len = vsprintf(msg, format, ap);
#endif
	system_write(debug_stream, msg, len);
	system_write(debug_stream, "\n", 1);
}


/* to set up debug level at application startup time */
void init_dc_debug()
{
	char *env;
	char *out_file;
	int efd;

	/* let do in only once */
	if(!isChangable) return;

	env = (char *)getenv("DCACHE_DEBUG");
	if(env != NULL) {
		dc_setDebugLevel(atoi(env));
		isChangable = 0;
	}

	out_file = (char *)getenv("DCACHE_DEBUG_FILE");
	if(out_file != NULL) {
		efd = system_open(out_file, O_WRONLY | O_APPEND | O_CREAT, 0644 );
		if(efd > 0 ) {
			debug_stream = efd;
		}else{
			debug_stream = 2; /* stderr */
		}
	}else{
		debug_stream = 2; /* stderr */
	}

	dc_debug(DC_INFO, "Dcap Version %d.%d.%d-%s",
		dc_getProtocol(),
		dc_getMajor(),
		dc_getMinor(),
		dc_getPatch());
}
