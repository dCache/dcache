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

import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.net.InetAddresses;
import org.apache.axis.MessageContext;
import org.globus.gsi.bc.BouncyCastleUtil;
import org.globus.gsi.gssapi.auth.AuthorizationException;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;

import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import org.dcache.auth.util.GSSUtils;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.srm.SRMAuthenticationException;
import org.dcache.srm.SRMAuthorization;
import org.dcache.srm.SRMAuthorizationException;
import org.dcache.srm.SRMInternalErrorException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.request.RequestCredentialStorage;

import static org.apache.axis.transport.http.HTTPConstants.MC_HTTP_SERVLETREQUEST;
import static org.globus.axis.gsi.GSIConstants.GSI_CREDENTIALS;

/**
 * The SrmAUthorizer provides helper methods that mediates access to
 * RequestCredentialStorage.
 */
public class SrmAuthorizer
{
    private static final Logger log = LoggerFactory.getLogger(SrmAuthorizer.class);

    private final RequestCredentialStorage storage;
    private final String vomsdir;
    private final String capath;
    private final SRMAuthorization authorization;
    private final boolean isClientDNSLookup;

    public SrmAuthorizer(SRMAuthorization authorization,
                         RequestCredentialStorage storage, boolean isClientDNSLookup,
                         String vomsdir, String capath)
    {
        this.isClientDNSLookup = isClientDNSLookup;
        this.authorization = authorization;
        this.storage = storage;
        this.vomsdir = vomsdir;
        this.capath = capath;
    }

    /**
     * Derive the UserCredential from the current Axis context.  The method
     * also updates various Axis MessageContext properties as a side-effect.
     */
    public UserCredential getUserCredentials() throws SRMInternalErrorException, SRMAuthenticationException
    {
        try {
            MessageContext mctx = MessageContext.getCurrentContext();

            Object tmp = mctx.getProperty(MC_HTTP_SERVLETREQUEST);
            if (!(tmp instanceof HttpServletRequest)) {
                throw new SRMInternalErrorException("HttpServletRequest is missing from Axis message context.");
            }
            HttpServletRequest req = (HttpServletRequest) tmp;

            GSSCredential delegcred = (GSSCredential) req.getAttribute(GSI_CREDENTIALS);
            if (delegcred != null) {
                try {
                    log.debug("User credential (delegcred) is: {}", delegcred.getName());
                } catch (GSSException e) {
                    Throwables.propagate(e);
                }
            }
            X509Certificate[] chain = (X509Certificate[]) req.getAttribute("javax.servlet.request.X509Certificate");
            if (chain == null) {
                throw new SRMAuthenticationException("Client's certificate chain is missing from request.");
            }

            String dn = BouncyCastleUtil.getIdentity(BouncyCastleUtil.getIdentityCertificate(chain));

            log.debug("User ID is: {}", dn);

            UserCredential userCredential = new UserCredential();
            userCredential.secureId = dn;
            userCredential.credential = delegcred;
            userCredential.chain = chain;
            if (isClientDNSLookup) {
                userCredential.clientHost = InetAddresses.forString(req.getRemoteAddr()).getCanonicalHostName();
            } else {
                userCredential.clientHost = req.getRemoteAddr();
            }

            return userCredential;
        } catch (CertificateException e) {
            throw new SRMAuthenticationException(e.toString(), e);
        }
    }


    /**
     * Obtain the SRMUser object if the user is authorized to use the
     * back-end system.  Throws SRMAuthorizationException if the user is
     * not authorized.
     */
    public SRMUser getRequestUser(RequestCredential requestCredential, X509Certificate[] chain)
            throws SRMInternalErrorException, SRMAuthorizationException, SRMAuthenticationException
    {
        MessageContext mctx = MessageContext.getCurrentContext();
        HttpServletRequest req = (HttpServletRequest) mctx.getProperty(MC_HTTP_SERVLETREQUEST);
        return authorization.authorize(requestCredential.getId(),
                requestCredential.getCredentialName(), chain, req.getRemoteAddr());
    }


    /**
     * Obtain a RequestCredential containing the delegated credential for the
     * current user with the specified role (primary FQAN).  If an existing
     * delegated credential already exists then this method will use the "best"
     * available credential, where best is the credential that will remain valid
     * for the longest.  The method ensures the best credential is saved in
     * the storage.
     */
    public RequestCredential getRequestCredential(UserCredential credential) throws SRMAuthenticationException
    {
        try {
            Iterable<String> roles = GSSUtils.extractFQANs(vomsdir, capath, credential.chain);
            String role = Iterables.getFirst(roles, null);

            String id = credential.secureId;
            GSSCredential gssCredential = credential.credential;
            RequestCredential requestCredential = RequestCredential.newRequestCredential(id, role, storage);
            requestCredential.keepBestDelegatedCredential(gssCredential);
            requestCredential.saveCredential();
            return requestCredential;
        } catch (GSSException | AuthorizationException e) {
            throw new SRMAuthenticationException("Problem getting request credential: " + e.getMessage(), e);
        }
    }
}
