#ifndef _DCAP_LCB_H_
#define _DCAP_LCB_H_

#include <sys/types.h>
#include "dcap_types.h"

long dc_lcb_init( struct vsp_node *node );
void dc_lcb_clean(struct vsp_node *node );
ssize_t dc_lcb_read( struct vsp_node * node, char *buf, ssize_t len );
#endif
