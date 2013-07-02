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


/*
 * SRMAuthorization.java
 *
 * Created on October 4, 2002, 12:11 PM
 */

package diskCacheV111.srm.dcache;

import org.globus.gsi.gssapi.GSSConstants;
import org.globus.gsi.jaas.GlobusPrincipal;
import org.gridforum.jgss.ExtendedGSSContext;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.cert.X509Certificate;

import diskCacheV111.util.CacheException;

import org.dcache.auth.LoginNamePrincipal;
import org.dcache.auth.LoginStrategy;
import org.dcache.auth.Origin;
import org.dcache.auth.Origin.AuthType;
import org.dcache.srm.SRMAuthorization;
import org.dcache.srm.SRMAuthorizationException;
import org.dcache.srm.SRMUser;

/**
 *
 * @author  timur
 */
public final class DCacheAuthorization implements SRMAuthorization
{
    private static final Logger LOGGER = LoggerFactory.getLogger(DCacheAuthorization.class);
    private final DcacheUserPersistenceManager persistenceManager;
    private final LoginStrategy loginStrategy;

    public DCacheAuthorization(LoginStrategy loginStrategy,
            DcacheUserPersistenceManager persistenceManager)
    {
       this.loginStrategy = loginStrategy;
       this.persistenceManager = persistenceManager;
    }

    /** Performs authorization checks. Throws
     * <code>SRMAuthorizationException</code> if the authorization fails.
     * Otherwise, the function completes normally.
     *
     * @param requestCredentialId
     * @param secureId
     * @param name
     * @param gsscontext
     * @exception <code>SRMAuthorizationException</code> if the peer is
     *            not authorized to access/use the resource.
     */
    @Override
    public SRMUser authorize(Long requestCredentialId,
                             String secureId,String name, GSSContext gsscontext, String remoteIP)
            throws SRMAuthorizationException
    {
        LOGGER.trace("authorize {}:{}:{}", requestCredentialId, secureId, name);
        try {
            if (!(gsscontext instanceof ExtendedGSSContext)) {
                throw new RuntimeException("GSSContext not instance of ExtendedGSSContext");
            }

            ExtendedGSSContext extendedcontext =
                (ExtendedGSSContext) gsscontext;
            X509Certificate[] chain =
                (X509Certificate[]) extendedcontext.inquireByOid(GSSConstants.X509_CERT_CHAIN);

            Subject subject = new Subject();
            if (name != null && !name.isEmpty()) {
                subject.getPrincipals().add(new LoginNamePrincipal(name));
            }
            if (secureId != null && !secureId.isEmpty()) {
                /* Technically, the secureId could be a
                 * KerberosPrincipal too. At this point we cannot tell
                 * the difference anymore.
                 */
                subject.getPrincipals().add(new GlobusPrincipal(secureId));
            }
            subject.getPublicCredentials().add(chain);

            try {
                InetAddress remoteOrigin = InetAddress.getByName(remoteIP);
                subject.getPrincipals().add(new Origin(AuthType.ORIGIN_AUTHTYPE_STRONG,
                                                      remoteOrigin));
                LOGGER.debug("User connected from the following IP, setting as origin: {}.",
                        remoteIP);
            } catch (UnknownHostException uhex) {
                LOGGER.info("Could not add the remote-IP {} as an origin principal.",
                        remoteIP);
            }

            return persistenceManager.persist(loginStrategy.login(subject));
        } catch (GSSException | CacheException e) {
            throw new SRMAuthorizationException(e.getMessage(), e);
        }
    }
}
