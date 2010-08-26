/*
 * Portions of this file Copyright 1999-2005 University of Chicago
 * Portions of this file Copyright 1999-2005 The University of Southern California.
 *
 * This file or a portion of this file is licensed under the
 * terms of the Globus Toolkit Public License, found at
 * http://www.globus.org/toolkit/download/license.html.
 * If you redistribute this file, with or without
 * modifications, you must include this notice in the file.
 */
package org.dcache.srm.security;
import org.globus.tomcat.catalina.net.HTTPSSocket;
import org.globus.common.ChainedIOException;
import org.globus.gsi.gssapi.auth.AuthorizationException;
import org.globus.security.gridmap.GridMap;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;

import java.io.IOException;
import java.net.Socket;

public class TomcatGSISocket extends HTTPSSocket {


    protected GSSContext context;

    private boolean init = false;

    public TomcatGSISocket(Socket socket, GSSContext context) {
        super(socket, context);
    }

    

    public GSSContext getGSSContext() {
        return context;
    }



    public synchronized void startHandshake()
            throws IOException {
        super.startHandshake();

        if (init) {
            return;
        }

        context = getContext();


        init = true;
    }

}
