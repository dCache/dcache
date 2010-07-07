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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "dcap_shared.h"
#include "dcap_types.h"
#include "dcap_errno.h"
#include "dcap_error.h"
#include "dcap_url.h"


/*
 *  Simple program to test the dcap parser.
 */
int main(int argc, char *argv[])
{
        dcap_url *url;
        char str[128];
        int i;

        for( i = 1; i < argc; i++) {
                printf("URL: %s\n", argv[i]);

                url = dc_getURL(argv[i]);
                if(url != NULL) {
                        printf("  host: %s\n", url->host);
                        printf("  file: %s\n", url->file);
                        printf("  type: %d\n", url->type);
                        printf("  prefix: %s\n", url->prefix ? url->prefix : "none");
                        printf("  config line: %s\n", url2config(str, sizeof(str), url) );
                        free(url->host);
                        free(url->file);
                        if( url->prefix != NULL) free(url->prefix);
                        free(url);
                } else {
                        printf("  dc_getURL() returned NULL\n");
                }
        }

        return 0;
}

