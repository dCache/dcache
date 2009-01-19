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
 * $Id: gettrace.c,v 1.3 2004-11-01 19:33:29 tigran Exp $
 */



#include <stdio.h>
#include <stdlib.h>
#include <bfd.h>

#include <signal.h>
#include <string.h>
#include <unistd.h>
#include <sys/param.h>


#include "dcap_debug.h"

static asymbol **read_bfd_stuff(bfd *);
extern void getStackTrace(unsigned long *, int *);

static asymbol **sym;
static bfd *abfd;
static asection *core_text_sect;
static int init;


static
void mcheck_init_bfd(char *name)
{

    bfd_init();
    abfd = bfd_openr(name, 0);
    if (!abfd) {
		dc_debug(DC_CALLS, "Unable to open it '%s'", name);
	return;
    }
    if (!bfd_check_format(abfd, bfd_object)) {
		dc_debug(DC_CALLS, "Bad format!");
		return;
    }
/* get core's text section: */
    core_text_sect = bfd_get_section_by_name(abfd, ".text");
    if (!core_text_sect) {
	core_text_sect = bfd_get_section_by_name(abfd, "$CODE$");
	if (!core_text_sect) {
	    dc_debug(DC_CALLS, "Can't find .text section");
	    return;
	}
	sym = (asymbol **) read_bfd_stuff(abfd);
    }
}

static
asymbol **read_bfd_stuff(bfd * abfd)
{
    long storage_needed;
    asymbol **symbol_table;
    long number_of_symbols;

    storage_needed = bfd_get_symtab_upper_bound(abfd);
    if (storage_needed < 0) {
		return 0;
	}
	
	if (storage_needed == 0) {
		return 0;
	}
    symbol_table = (asymbol **) malloc(storage_needed);

    number_of_symbols = bfd_canonicalize_symtab(abfd, symbol_table);
	if (number_of_symbols < 0) {
		return 0;
	}
    return symbol_table;
}


void mcheck_lookup_method_info(const char **file,
			       const char **func,
			       unsigned int *line, const char *address)
{
    bfd_vma vma;
    vma = (bfd_vma) (address - core_text_sect->vma);
    bfd_find_nearest_line(abfd, core_text_sect, sym, vma, file, func,
			  line);
}

static int printStackInfo(const char *address, int i)
{
    const char *file = NULL;
    const char *func = NULL, *s;
    unsigned int line;
    int ret = 1;

    mcheck_lookup_method_info(&file, &func, &line, address);


    if (!file) {
	file = "<unknown-file>";
    } else {

	s = strrchr(file, '/');
	if (s != NULL)
	    file = s + 1;
    }

    if (!func) {
		func = "<unknown-function>";
    } else {  
		if ( (strstr(func, "_start") )|| (strstr(func, "main") ) ) {
			ret = 0;
		}
	}

    dc_debug(DC_CALLS, "#%d 0x%x in %s () at %s:%d", i, (unsigned int)address, func, file, line);

    return ret;
}


static void init_traceBack()
{
    char exename[MAXPATHLEN];
	int res;

	if( !init) {


    	/*
    	 * get the name of the current executable
    	 */
    	if ((res = readlink("/proc/self/exe", exename, MAXPATHLEN - 1)) == -1)
		exename[0] = '\0';
    	else
		exename[res] = '\0';

	    mcheck_init_bfd(exename);
		++init;
		
	}
}

void showTraceBack()
{

    unsigned long returnStack[16];
    int i;
    int max = 0;
    int more = 1;

	init_traceBack();


    dc_debug(DC_CALLS, "Stack backtrace:");

    getStackTrace(returnStack, &max);

    for (i = 1; i < max && more; i++) {
		more = printStackInfo((const char *) (returnStack[i]), i - 1);
    }

    return;
}
