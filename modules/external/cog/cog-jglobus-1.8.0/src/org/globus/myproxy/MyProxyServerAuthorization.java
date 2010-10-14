/*
 * Copyright 1999-2006 University of Chicago
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.globus.myproxy;

import org.ietf.jgss.GSSContext;

import org.globus.gsi.gssapi.auth.Authorization;
import org.globus.gsi.gssapi.auth.HostAuthorization;
import org.globus.gsi.gssapi.auth.AuthorizationException;

/**
 * Implements the MyProxy server authorization mechanism.
 */
public class MyProxyServerAuthorization
    extends Authorization {
    
    private HostAuthorization authzHostService, authzMyProxyService;

    public MyProxyServerAuthorization() {
        this.authzMyProxyService = new HostAuthorization("myproxy");
        this.authzHostService = HostAuthorization.getInstance();
    }

    /**
     * Performs MyProxy server authorization checks. The hostname of
     * the server is compared with the hostname specified in the
     * server's (topmost) certificate in the certificate chain. The
     * hostnames must match exactly (in case-insensitive way). The
     * service in the certificate may be "host" or "myproxy".
     * <code>AuthorizationException</code> if the authorization fails.
     * Otherwise, the function completes normally.
     *
     * @param context the security context.
     * @param host host address of the peer.
     * @exception AuthorizationException if the peer is
     *            not authorized to access/use the resource.
     */
    public void authorize(GSSContext context, String host) 
        throws AuthorizationException {
        try {
            this.authzMyProxyService.authorize(context, host);
        } catch (AuthorizationException e) {
            this.authzHostService.authorize(context, host);
        }
    }
}
