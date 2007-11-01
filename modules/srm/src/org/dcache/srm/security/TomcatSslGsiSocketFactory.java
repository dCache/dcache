/*
 * This class is the modification of org.globus.tomcat.catalina.net.GSIServerSocketFactory
 * from jglobus-fx product
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

//import org.globus.tomcat.catalina.net.*;
import org.globus.tomcat.catalina.net.HTTPSServerSocketFactory;
import org.apache.log4j.Logger;
import org.globus.gsi.GSIConstants;
import org.globus.gsi.gssapi.GSSConstants;
import org.globus.security.gridmap.GridMap;
import org.gridforum.jgss.ExtendedGSSContext;
import org.ietf.jgss.GSSException;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

public class TomcatSslGsiSocketFactory
        extends HTTPSServerSocketFactory {

    private boolean sslMode;

    public TomcatSslGsiSocketFactory() {
        sslMode = false;
        super.setCert("/etc/grid-security/hostcert.pem");
        super.setKey("/etc/grid-security/hostkey.pem");
        super.setCacertdir("/etc/grid-security/certificates");
    }


    public void setSslMode(boolean sslMode) {
        this.sslMode = sslMode;
    }

    public boolean getSslMode() {
        return sslMode;
    }

    // ------------------------------------------

    /**
     * Creates a secure server socket on a specified port with default
     * user credentials. A port of <code>0</code> creates a socket on
     * any free port or if the tcp.port.range system property is set
     * it creates a socket within the specified port range.
     * <p/>
     * The maximum queue length for incoming connection indications (a
     * request to connect) is set to the <code>backlog</code> parameter. If
     * a connection indication arrives when the queue is full, the
     * connection is refused.
     *
     * @param port     the port number, or <code>0</code> to use any
     *                 free port or if the tcp.port.range property set
     *                 to use any available port within the specified port
     *                 range.
     * @param backlog  the maximum length of the queue.
     * @param bindAddr the local InetAddress the server will bind to.
     * @throws IOException if an I/O error occurs when opening the socket.
     */
    public ServerSocket createSocket(int port,
                                     int backlog,
                                     InetAddress bindAddr)
            throws IOException {


        GSIServerSocket serverSocket = (GSIServerSocket)
                super.createSocket(port, backlog, bindAddr);


        return serverSocket;
    }

    protected HTTPSServerSocket createServerSocket(
            int port, int backlog, InetAddress bindAddr)
            throws IOException {
        return new GSIServerSocket(port, backlog, bindAddr);
    }

    class GSIServerSocket extends HTTPSServerSocket {

        private GridMap _gridMap;

        public GSIServerSocket(int port, int backlog,
                               InetAddress bindAddr)
                throws IOException {
            super(port, backlog, bindAddr);
        }

        public void setGridMap(GridMap gridMap) {
            this._gridMap = gridMap;
        }

        public Socket accept()
                throws IOException {

            TomcatGSISocket s = (TomcatGSISocket) super.accept();

            return s;
        }

        protected Socket createSocket(Socket s, ExtendedGSSContext context) {
            return new TomcatGSISocket(s, context);
        }

        protected void setContextOptions(ExtendedGSSContext context) throws GSSException {
            if (getSslMode()) {
                context.setOption(GSSConstants.GSS_MODE,
                        GSIConstants.MODE_SSL);
            }

            if (this._trustedCerts != null) {
                context.setOption(GSSConstants.TRUSTED_CERTIFICATES,
                        this._trustedCerts);
            }
        }

    }

}
