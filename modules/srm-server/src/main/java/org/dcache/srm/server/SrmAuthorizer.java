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

import com.google.common.collect.Iterables;
import com.google.common.net.InetAddresses;
import org.globus.gsi.gssapi.auth.AuthorizationException;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.cert.X509Certificate;

import org.dcache.auth.util.GSSUtils;
import org.dcache.srm.SRMAuthenticationException;
import org.dcache.srm.SRMAuthorization;
import org.dcache.srm.SRMAuthorizationException;
import org.dcache.srm.SRMInternalErrorException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.request.RequestCredentialStorage;
import org.dcache.srm.util.Axis;


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
     * Obtain the SRMUser object if the user is authorized to use the
     * back-end system.  Throws SRMAuthorizationException if the user is
     * not authorized.
     */
    public SRMUser getRequestUser()
            throws SRMInternalErrorException, SRMAuthorizationException, SRMAuthenticationException
    {
        String dn = Axis.getDN().orElseThrow(() ->
                new SRMAuthenticationException("Failed to resolve DN"));

        X509Certificate[] certificates = Axis.getCertificateChain().orElseThrow(() ->
                new SRMAuthenticationException("Client's certificate chain is missing from request"));

        return authorization.authorize(dn, certificates, Axis.getRemoteAddress());
    }

    /**
     * Check whether the current user is authorized to use the SRM without
     * mapping that user.
     * @return true if the user can use the SRM.
     */
    public boolean isUserAuthorized() throws SRMInternalErrorException,
            SRMAuthenticationException
    {
        String dn = Axis.getDN().orElseThrow(() ->
                new SRMAuthenticationException("Failed to resolve DN"));

        X509Certificate[] certificates = Axis.getCertificateChain().orElseThrow(() ->
                new SRMAuthenticationException("Client's certificate chain is missing from request"));

        return authorization.isAuthorized(dn, certificates, Axis.getRemoteAddress());
    }


    /**
     * Obtain a RequestCredential containing the delegated credential for the
     * current user with the specified role (primary FQAN).  If an existing
     * delegated credential already exists then this method will use the "best"
     * available credential, where best is the credential that will remain valid
     * for the longest.  The method ensures the best credential is saved in
     * the storage.
     */
    public RequestCredential getRequestCredential() throws SRMAuthenticationException
    {
        X509Certificate[] certificates = Axis.getCertificateChain().orElseThrow(() ->
                new SRMAuthenticationException("Client's certificate chain is missing from request"));

        String dn = Axis.getDN().orElseThrow(() ->
                new SRMAuthenticationException("Failed to resolve DN"));

        GSSCredential credential = Axis.getDelegatedCredential().orElse(null);

        try {
            Iterable<String> roles = GSSUtils.extractFQANs(vomsdir, capath, certificates);
            String role = Iterables.getFirst(roles, null);

            RequestCredential requestCredential = RequestCredential.newRequestCredential(dn, role, storage);
            requestCredential.keepBestDelegatedCredential(credential);
            requestCredential.saveCredential();
            return requestCredential;
        } catch (GSSException e) {
            throw new SRMAuthenticationException("Problem getting request credential: " + e.getMessage(), e);
        }
    }
}
