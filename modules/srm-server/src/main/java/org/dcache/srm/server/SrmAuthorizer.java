/**
 * SrmAuthorizer.java
 *
 * Authors:  LH - Leo Heska
 *
 * History:
 *    2005/07/22 LH Extracted from SrmSoapBindingImpl.java
 */

/*
COPYRIGHT STATUS:
Dec 1st 2001, Fermi National Accelerator Laboratory (FNAL) documents and
software are sponsored by the U.S. Department of Energy under Contract
No. DE-AC02-76CH03000. Therefore, the U.S. Government retains a
world-wide non-exclusive, royalty-free license to publish or reproduce
these documents and software for U.S. Government purposes.  All
documents and software available from this server are protected under
the U.S. and Foreign Copyright Laws, and FNAL reserves all rights.


 Distribution of the software available from this server is free of
 charge subject to the user following the terms of the Fermitools
 Software Legal Information.

 Redistribution and/or modification of the software shall be accompanied
 by the Fermitools Software Legal Information  (including the copyright
 notice).

 The user is asked to feed back problems, benefits, and/or suggestions
 about the software to the Fermilab Software Providers.


Neither the name of Fermilab, the  URA, nor the names of the
contributors may be used to endorse or promote products derived from
this software without specific prior written permission.

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
cost, or damages arising out of or resulting from the use of the
software available from this server.


Export Control:

All documents and software available from this server are subject to
U.S. export control laws.  Anyone downloading information from this
server is obligated to secure any necessary Government licenses before
exporting documents or software obtained from this server.
 */

package org.dcache.srm.server;

import org.apache.axis.MessageContext;
import org.glite.voms.PKIVerifier;
import org.globus.gsi.gssapi.auth.AuthorizationException;
import org.gridforum.jgss.ExtendedGSSContext;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;

import org.dcache.auth.util.GSSUtils;
import org.dcache.srm.SRMAuthorization;
import org.dcache.srm.SRMAuthorizationException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.request.RequestCredentialStorage;

import static org.apache.axis.transport.http.HTTPConstants.MC_HTTP_SERVLETREQUEST;
import static org.globus.axis.gsi.GSIConstants.GSI_CONTEXT;

/**
 * The SrmAUthorizer provides helper methods that mediates access to
 * RequestCredentialStorage.  It uses information (attributes) taken from the
 * Axis request context and updates the Axis MessageContext (properties),
 * specifically:
 *
 *   Property "org.globus.gsi.context" from Attribute "org.globus.gsi.context"
 *   Property "REMOTE_ADDR" from HttpServletRequest's remote address.
 */
public class SrmAuthorizer
{
    private static final Logger log = LoggerFactory.getLogger(SrmAuthorizer.class);
    private static final String REMOTE_ADDR = "REMOTE_ADDR";

    private final RequestCredentialStorage storage;
    private final SRMAuthorization authorization;
    private final boolean isClientDNSLookup;

    public SrmAuthorizer(SRMAuthorization authorization,
            RequestCredentialStorage storage, boolean isClientDNSLookup)
    {
        this.isClientDNSLookup = isClientDNSLookup;
        this.authorization = authorization;
        this.storage = storage;
        log.debug("Successfully initialized");
    }


    /**
     * Derive the UserCredential from the current Axis context.  The method
     * also updates various Axis MessageContext properties as a side-effect.
     */
    public UserCredential getUserCredentials() throws SRMAuthorizationException
    {
        try {
            MessageContext mctx = MessageContext.getCurrentContext();
            setUpEnv(mctx);

            GSSContext gsscontext = (GSSContext)mctx.getProperty(GSI_CONTEXT);

            if(gsscontext == null) {
                throw new SRMAuthorizationException("cant extract gsscontext " +
                        "from MessageContext, gsscontext is null");
            }

            String secureId = gsscontext.getSrcName().toString();
            log.debug("User ID (secureId) is: " + secureId);
            GSSCredential delegcred = gsscontext.getDelegCred();
            if(delegcred != null) {
                try {
                    log.debug("User credential (delegcred) is: " +
                    delegcred.getName());
                } catch (Exception e) {
                    log.debug("Caught occasional (usually harmless) exception" +
                        " when calling " + "delegcred.getName()): ", e);
                }
            }

            UserCredential userCredential = new UserCredential();
            userCredential.secureId = secureId;
            userCredential.context = gsscontext;
            userCredential.credential = delegcred;
            String remote_addr = (String) mctx.getProperty(REMOTE_ADDR);

            if(isClientDNSLookup) {
                userCredential.clientHost = InetAddress.getByName(remote_addr).
                        getCanonicalHostName();
            } else {
                userCredential.clientHost = remote_addr;
            }

            return userCredential;
        } catch (GSSException | UnknownHostException e) {
            log.error("getUserCredentials failed with exception", e);
            throw new SRMAuthorizationException(e.toString(), e);
        }
   }


    /**
     * Obtain the SRMUser object if the user is authorized to use the
     * back-end system.  Throws SRMAuthorizationException if the user is
     * not authorized.
     */
    public SRMUser getRequestUser(RequestCredential requestCredential,
            String role, GSSContext context) throws SRMAuthorizationException
    {
        MessageContext mctx = MessageContext.getCurrentContext();
        String remoteIP = (String) mctx.getProperty(REMOTE_ADDR);

        return authorization.authorize(requestCredential.getId(),
                requestCredential.getCredentialName(), role, context, remoteIP);
    }


    /**
     * Obtain a RequestCredential containing the delegated credential for the
     * current user with the specified role (primary FQAN).  If an existing
     * delegated credential already exists then this method will use the "best"
     * available credential, where best is the credential that will remain valid
     * for the longest.  The method ensures the best credential is saved in
     * the storage.
     */
    public RequestCredential getRequestCredential(UserCredential credential,
            String role)
    {
        try {
            String id = credential.secureId;
            GSSCredential gssCredential = credential.credential;
            RequestCredential requestCredential = RequestCredential.newRequestCredential(id, role, storage);
            requestCredential.keepBestDelegatedCredential(gssCredential);
            requestCredential.saveCredential();
            return requestCredential;
        } catch(GSSException e) {
            throw new RuntimeException("Problem getting request credential", e);
        }
   }

    private void setUpEnv(MessageContext msgContext)
    {
        Object tmp = msgContext.getProperty(MC_HTTP_SERVLETREQUEST);

        if(tmp == null || !(tmp instanceof HttpServletRequest)) {
            return;
        }

        HttpServletRequest req = (HttpServletRequest) tmp;

        tmp = req.getAttribute(GSI_CONTEXT);
        if (tmp != null) {
            msgContext.setProperty(GSI_CONTEXT, tmp);
        }

        tmp = req.getRemoteAddr();
        if (tmp != null) {
            msgContext.setProperty(REMOTE_ADDR, tmp);
        }
    }

    static Iterable<String> getFQANsFromContext(ExtendedGSSContext gssContext)
            throws SRMAuthorizationException
    {
        try {
            return GSSUtils.getFQANsFromGSSContext(gssContext);
        } catch (AuthorizationException ae) {
            log.error("Could not extract FQANs from context",ae);
            throw new SRMAuthorizationException("Could not extract FQANs from context " + ae.getMessage());
        }
    }
}
