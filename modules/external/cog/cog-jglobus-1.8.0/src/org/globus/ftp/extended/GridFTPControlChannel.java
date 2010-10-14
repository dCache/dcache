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
package org.globus.ftp.extended;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.globus.ftp.GridFTPSession;
import org.globus.ftp.exception.ServerException;
import org.globus.ftp.exception.UnexpectedReplyCodeException;
import org.globus.ftp.exception.FTPReplyParseException;
import org.globus.ftp.vanilla.Reply;
import org.globus.ftp.vanilla.FTPControlChannel;
import org.globus.ftp.vanilla.Command;
import org.globus.common.ChainedIOException;
import org.globus.gsi.gssapi.auth.Authorization;
import org.globus.gsi.gssapi.auth.GSSAuthorization;
import org.globus.gsi.gssapi.auth.HostAuthorization;
import org.globus.gsi.gssapi.auth.AuthorizationException;
import org.globus.gsi.gssapi.GSSConstants;

import org.gridforum.jgss.ExtendedGSSManager;

import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSName;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/** GridFTP control channel, unlike the vanilla control channel,
    uses GSI autentication.
 **/
public class GridFTPControlChannel extends FTPControlChannel {

    private static Log logger = 
        LogFactory.getLog(GridFTPControlChannel.class.getName());

    protected static final int TIMEOUT = 120000;

    //maybe this is useless
    protected GSSCredential credentials = null;

    protected Authorization authorization = 
        HostAuthorization.getInstance();

    protected int protection = GridFTPSession.PROTECTION_PRIVATE;
    
    public GridFTPControlChannel(String host, int port) {
        super(host,port);
    }

    public GridFTPControlChannel(InputStream in, OutputStream out) {
        super(in, out);
    }

    /**
     * Sets data channel protection level.
     *
     * @param protection should be 
     *             {@link GridFTPSession#PROTECTION_CLEAR CLEAR},
     *             {@link GridFTPSession#PROTECTION_SAFE SAFE}, or
     *             {@link GridFTPSession#PROTECTION_PRIVATE PRIVATE}, or
     *             {@link GridFTPSession#PROTECTION_CONFIDENTIAL CONFIDENTIAL}.
     **/
    public void setProtection(int protection) {

        switch(protection) {
        case GridFTPSession.PROTECTION_CLEAR:
            throw new IllegalArgumentException("Unsupported protection: " +
                                               protection);
        case GridFTPSession.PROTECTION_SAFE:
        case GridFTPSession.PROTECTION_CONFIDENTIAL:
        case GridFTPSession.PROTECTION_PRIVATE:
            break;
        default: 
            throw new IllegalArgumentException("Bad protection: " +
                                               protection);
        }
        
        this.protection = protection;
    }
    
    /**
     * Returns control channel protection level.
     * 
     * @return control channel protection level: 
     *             {@link GridFTPSession#PROTECTION_CLEAR CLEAR},
     *             {@link GridFTPSession#PROTECTION_SAFE SAFE}, or
     *             {@link GridFTPSession#PROTECTION_PRIVATE PRIVATE}, or
     *             {@link GridFTPSession#PROTECTION_CONFIDENTIAL CONFIDENTIAL}.
     **/
    public int getProtection() {
        return this.protection;
    }

    /**
     * Sets authorization method for the control channel.
     *
     * @param authorization authorization method.
     */
    public void setAuthorization(Authorization authorization) {
        this.authorization = authorization;
    }

    /**
     * Returns authorization method for the control channel.
     * 
     * @return authorization method performed on the control channel.
     */
    public Authorization getAuthorization() {
        return this.authorization;
    }

    /**
     * Performs authentication with specified user credentials.
     *
     * @param credential user credentials to use.
     * @throws IOException on i/o error
     * @throws ServerException on server refusal or faulty server behavior
     */
    public void authenticate(GSSCredential credential)
        throws IOException, ServerException {
        authenticate(credential, null);
    }

    /**
     * Performs authentication with specified user credentials and
     * a specific username (assuming the user dn maps to the passed username).
     *
     * @param credential user credentials to use.
     * @param username specific username to authenticate as.
     * @throws IOException on i/o error
     * @throws ServerException on server refusal or faulty server behavior
     */
    public void authenticate(GSSCredential credential,
                             String username)
        throws IOException, ServerException {
        
        setCredentials( credential );
         
        write(new Command("AUTH", "GSSAPI"));

        Reply reply0 = null;
        try {
            reply0 = read();
        } catch (FTPReplyParseException rpe) {
            throw ServerException.embedFTPReplyParseException(
                                      rpe,
                                      "Received faulty reply to AUTH GSSAPI");
        }

        if (! Reply.isPositiveIntermediate(reply0)) {
           close();
           throw ServerException.embedUnexpectedReplyCodeException(
                                  new UnexpectedReplyCodeException(reply0),   
                                  "Server refused GSSAPI authentication.");
        }

        GSSManager manager = ExtendedGSSManager.getInstance();

        GSSContext context = null;
        GridFTPOutputStream gssout = null;
        GridFTPInputStream gssin = null;

        try {
            String host = this.socket.getInetAddress().getHostAddress();

            GSSName expectedName = null;


            if (this.authorization instanceof GSSAuthorization) {
                GSSAuthorization auth = (GSSAuthorization)this.authorization;
                expectedName = auth.getExpectedName(credential, host);
            }

            context = manager.createContext(expectedName,
                                            GSSConstants.MECH_OID,
                                            credential,
                                            GSSContext.DEFAULT_LIFETIME);
            context.requestCredDeleg(true);
            context.requestConf(this.protection == 
                                GridFTPSession.PROTECTION_PRIVATE);
            
            gssout = new GridFTPOutputStream(ftpOut, context);
            gssin = new GridFTPInputStream(rawFtpIn, context);

            byte [] inToken = new byte[0];
            byte [] outToken = null;
            
            while( !context.isEstablished() ) {
                
                outToken = context.initSecContext(inToken, 0, inToken.length);
                
                if (outToken != null) {
                    gssout.writeHandshakeToken(outToken);
                }

                if (!context.isEstablished()) {
                    inToken = gssin.readHandshakeToken();
                }
            }

        } catch (GSSException e) {
            throw new ChainedIOException("Authentication failed", e);
        }

        if (this.authorization != null) {
            try {
                this.authorization.authorize(context, host);
            } catch (AuthorizationException e) {

                throw new ChainedIOException("Authorization failed", e);
            }
        }

        // this should be authentication success msg (plain)
        // 234 (ok, no further data required)
        Reply reply1 = null;
        try {
            reply1 = read();
        } catch (FTPReplyParseException rpe) {
            throw ServerException.embedFTPReplyParseException(
                                      rpe,
                                      "Received faulty reply to authentication");

        }
        
        if ( ! Reply.isPositiveCompletion(reply1)) {
            close();
            throw ServerException.embedUnexpectedReplyCodeException(
                                    new UnexpectedReplyCodeException(reply1),
                                    "GSSAPI authentication failed.");
        }
        
        // enter secure mode - send MIC commands
        setInputStream(gssin);
        setOutputStream(gssout);
        //from now on, the commands and replies
        //are protected and pass through gsi wrapped socket

        write(new Command("USER", 
                          (username == null) ? ":globus-mapping:" : username));

        Reply reply2 = null;
        try {
            reply2 = read();
        } catch (FTPReplyParseException rpe) {
            throw ServerException.embedFTPReplyParseException(
                                      rpe,
                                      "Received faulty reply to USER command");
        }

        if (Reply.isPositiveCompletion(reply2) ||
            Reply.isPositiveIntermediate(reply2)) {
           // wu-gsiftp sends intermediate code while
           // gssftp send completion reply code
        } else {
           close();
           throw ServerException.embedUnexpectedReplyCodeException(
                                        new UnexpectedReplyCodeException(reply2),
                                        "User authorization failed.");
        }

        write(new Command("PASS", "dummy"));

        Reply reply3 = null;
        try {
            reply3=read();
        } catch (FTPReplyParseException rpe) {
            throw ServerException.embedFTPReplyParseException(
                                     rpe,
                                     "Received faulty reply to PASS command");
        }

        if (!Reply.isPositiveCompletion(reply3)) {
            close();
            throw ServerException.embedUnexpectedReplyCodeException(
                                    new UnexpectedReplyCodeException(reply3),
                                    "Bad password.");
        }
    }

    protected void setCredentials( GSSCredential credentials ) {
        this.credentials = credentials;
    }

    protected GSSCredential getCredentials() {
        return credentials;
    }

}








