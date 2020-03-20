/*
COPYRIGHT STATUS:
Dec 1st 2001, Fermi National Accelerator Laboratory (FNAL) documents and
software are sponsored by the U.S. Department of Energy under Contract No.
DE-AC02-76CH03000. Therefore, the U.S. Government retains a  world-wide
non-exclusive, royalty-free license to publish or reproduce these documents
and software for U.S. Government purposes.  All documents and software
available from this server are protected under the U.S. and Foreign
Copyright Laws, and FNAL reserves all rights.

Distribution of the software available from this server is free of
charge subject to the user following the terms of the Fermitools
Software Legal Information.

Redistribution and/or modification of the software shall be accompanied
by the Fermitools Software Legal Information  (including the copyright
notice).

The user is asked to feed back problems, benefits, and/or suggestions
about the software to the Fermilab Software Providers.

Neither the name of Fermilab, the  URA, nor the names of the contributors
may be used to endorse or promote products derived from this software
without specific prior written permission.

DISCLAIMER OF LIABILITY (BSD):

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED  WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED  WARRANTIES OF MERCHANTABILITY AND FITNESS
FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL FERMILAB,
OR THE URA, OR THE U.S. DEPARTMENT of ENERGY, OR CONTRIBUTORS BE LIABLE
FOR  ANY  DIRECT, INDIRECT,  INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
OF SUBSTITUTE  GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY  OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT  OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE  POSSIBILITY OF SUCH DAMAGE.

Liabilities of the Government:

This software is provided by URA, independent from its Prime Contract
with the U.S. Department of Energy. URA is acting independently from
the Government and in its own private capacity and is not acting on
behalf of the U.S. Government, nor as its contractor nor its agent.
Correspondingly, it is understood and agreed that the U.S. Government
has no connection to this software and in no manner whatsoever shall
be liable for nor assume any responsibility or obligation for any claim,
cost, or damages arising out of or resulting from the use of the software
available from this server.

Export Control:

All documents and software available from this server are subject to U.S.
export control laws.  Anyone downloading information from this server is
obligated to secure any necessary Government licenses before exporting
documents or software obtained from this server.
 */
package org.dcache.xrootd.plugins.authz;

import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;

import java.net.InetSocketAddress;
import java.util.Map;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PermissionDeniedCacheException;

import org.dcache.auth.BearerTokenCredential;
import org.dcache.auth.LoginReply;
import org.dcache.auth.LoginStrategy;
import org.dcache.auth.attributes.Activity;
import org.dcache.xrootd.core.XrootdException;
import org.dcache.xrootd.door.LoginEvent;
import org.dcache.xrootd.plugins.AuthorizationHandler;
import org.dcache.xrootd.protocol.XrootdProtocol.FilePerm;

import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_InvalidRequest;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_NotAuthorized;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_ServerError;

public class XrootdSciTokenAuthzHandler implements AuthorizationHandler
{
    private static final Logger LOGGER
                    = LoggerFactory.getLogger(XrootdSciTokenAuthzHandler.class);

    /**
     * The path query name to which the SciToken value is assigned.
     */
    private static final String SCITOKEN = "authz";

    /**
     * The open call from the originating client to the destination server in
     * third-party-copy.
     */
    private static final String TPC_STAGE = "tpc.stage";

    /**
     * The initial phase of the TPC_STAGE open call.  The client does
     * not pass the path query tokens to the server, so it is
     * necessary to skip this phase with respect to token authorization.
     */
    private static final String TPC_PLACEMENT = "placement";

    private static Activity toActivity(FilePerm perm)
    {
        switch(perm)
        {
            case WRITE:
            case WRITE_ONCE:
                return Activity.UPLOAD;
            case DELETE:
                return Activity.DELETE;
            case READ:
            default:
                return Activity.DOWNLOAD;
        }
    }

    /*
     * Caching should be enabled on the xrootd door by default.
     */
    private final LoginStrategy loginStrategy;

    /*
     * The xrootd protocol states that the server can specify supporting
     * different authentication protocols via a list which the client
     * should try in order.  The xrootd4j library allows for the chaining
     * of multiple such handlers on the Netty pipeline (though currently
     * dCache only supports one protocol, either GSI or none, at a time).
     *
     * Authorization, on the other hand, takes place after the authentication
     * phase; the xrootd4j authorization handler assumes that the module it
     * loads is the only authorization procedure allowed, and there is no
     * provision for passing a failed authorization on to a
     * successive handler on the pipeline.
     *
     * We thus make provision here for failing over to "standard" behavior
     * via this property.   If it is true, then we require the presence
     * of the token.  If false, and the token is missing, we return the
     * path and allow whatever restrictions that are already in force from
     * a prior login to apply.
     */
    private final boolean strict;

    private final ChannelHandlerContext ctx;

    public XrootdSciTokenAuthzHandler(LoginStrategy loginStrategy,
                                      boolean strict,
                                      ChannelHandlerContext ctx)
    {
        this.loginStrategy = loginStrategy;
        this.strict = strict;
        this.ctx = ctx;
    }

    @Override
    public String authorize(Subject subject,
                            InetSocketAddress localAddress,
                            InetSocketAddress remoteAddress,
                            String path,
                            Map<String, String> opaque,
                            int request,
                            FilePerm mode)
                    throws XrootdException, SecurityException
    {
        LOGGER.trace("authorize: {}, {}, {}, {}, {}, {}, {}.",
                    subject, localAddress, remoteAddress,
                    path, opaque, request, mode);

        String tpcStage = opaque.get(TPC_STAGE);
        if (TPC_PLACEMENT.equals(tpcStage)) {
            return path;
        }

        String authz = opaque.get(SCITOKEN);

        if (authz == null) {
            LOGGER.debug("no token for {}; strict? {}.", path, strict);

            if (!strict) {
                return path;
            }

            throw new XrootdException(kXR_InvalidRequest,
                                      "user provided no bearer token.");
        }

        Subject tokenSubject = new Subject();
        tokenSubject.getPrivateCredentials().add(new BearerTokenCredential(authz));

        LoginReply loginReply;

        try {
            LOGGER.debug("getting login reply with: {}.",
                        tokenSubject.getPrivateCredentials());
            loginReply = loginStrategy.login(tokenSubject);
        } catch (PermissionDeniedCacheException e) {
            throw new XrootdException(kXR_NotAuthorized, e.toString());
        } catch (CacheException e) {
            throw new XrootdException(kXR_ServerError, e.toString());
        }

        /**
         *  It is possible the the user is already logged in via a standard
         *  authentication protocol.  In that case, the XrootdRedirectHandler
         *  in the door already has stored a Restriction object and user
         *  metadata.  This needs to be overwritten with the current values.
         */
        LOGGER.debug("notifying door of new login reply: {}.", loginReply);
        ctx.fireUserEventTriggered(new LoginEvent(loginReply));

        return path;
    }
}
