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

package org.dcache.srm.request;

import com.google.common.collect.MapMaker;
import eu.emi.security.authn.x509.X509Credential;
import org.springframework.dao.DataAccessException;

import java.security.cert.X509Certificate;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;

import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.scheduler.JobIdGeneratorFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class RequestCredential
{
    private static final ConcurrentMap<Long, RequestCredential> weakRequestCredentialStorage =
            new MapMaker().weakValues().makeMap();
    private static final List<RequestCredentialStorage> requestCredentailStorages = new CopyOnWriteArrayList<>();

    private final long id;
    private final long creationtime;
    private final String credentialName;
    private final String role;
    private final RequestCredentialStorage storage;

    private boolean saved; //false by default
    private X509Credential delegatedCredential;
    private long delegatedCredentialExpiration;

    public static void registerRequestCredentialStorage(RequestCredentialStorage requestCredentialStorage)
    {
        requestCredentailStorages.add(checkNotNull(requestCredentialStorage));
    }

    public static RequestCredential getRequestCredential(Long requestCredentialId) throws DataAccessException
    {
        if (requestCredentialId == null) {
            return null;
        }

        synchronized (weakRequestCredentialStorage) {
            RequestCredential credential = weakRequestCredentialStorage.get(requestCredentialId);
            if (credential != null) {
                return credential;
            }

            for (RequestCredentialStorage storage : requestCredentailStorages) {
                credential = storage.getRequestCredential(requestCredentialId);
                if (credential != null) {
                    weakRequestCredentialStorage.put(credential.id, credential);
                    return credential;
                }
            }
        }
        return null;
    }

    public static RequestCredential newRequestCredential(String credentialName, String role, RequestCredentialStorage storage)
    {
        synchronized (weakRequestCredentialStorage) {
            for (RequestCredential credential : weakRequestCredentialStorage.values()) {
                String credName = credential.getCredentialName();
                String credRole = credential.getRole();
                if (credName.equals(credentialName) && Objects.equals(role, credRole)) {
                    return credential;
                }
            }

            RequestCredential credential = storage.getRequestCredential(credentialName, role);
            if (credential != null) {
                weakRequestCredentialStorage.put(credential.id, credential);
                return credential;
            }

            credential = new RequestCredential(credentialName, role, storage);
            weakRequestCredentialStorage.put(credential.id, credential);
            return credential;
        }
    }

    /** Creates a new instance of requestCredential */
    public RequestCredential(String credentialName,
                             String role,
                             RequestCredentialStorage storage)
    {
        this.id = JobIdGeneratorFactory.getJobIdGeneratorFactory().getJobIdGenerator().getNextId();
        this.creationtime = System.currentTimeMillis();
        this.credentialName = credentialName;
        this.role = role;
        this.storage = storage;
    }

    /** restores a previously stored instance of the requestcredential */
    public RequestCredential(Long id,
                             long creationtime,
                             String credentialName,
                             String role,
                             X509Credential delegatedCredential,
                             long delegatedCredentialExpiration,
                             RequestCredentialStorage storage)
    {
        this.id = id;
        this.creationtime = creationtime;
        this.credentialName = credentialName;
        this.role = role;
        this.delegatedCredential = delegatedCredential;
        this.delegatedCredentialExpiration = delegatedCredentialExpiration;
        this.storage = storage;
        this.saved = true;
    }

    /**
     * Create a new RequestCredential to wrap an existing GSSCredential.
     */
    public RequestCredential(String credentialName,
                             String role,
                             X509Credential delegatedCredential,
                             RequestCredentialStorage storage)
    {
        this.id = JobIdGeneratorFactory.getJobIdGeneratorFactory().getJobIdGenerator().getNextId();
        this.creationtime = System.currentTimeMillis();
        this.credentialName = credentialName;
        this.role = role;
        this.delegatedCredential = delegatedCredential;
        this.delegatedCredentialExpiration = expiryDateFor(delegatedCredential);
        this.storage = storage;
    }

    public synchronized X509Credential getDelegatedCredential()
    {
        return delegatedCredential;
    }

    public synchronized void keepBestDelegatedCredential(X509Credential credential)
    {
        if (credential != null && (this.delegatedCredential == null ||
                expiryDateFor(credential) > this.delegatedCredentialExpiration)) {
            updateCredential(credential);
        }
    }

    private static long expiryDateFor(X509Credential credential)
    {
        return Stream.of(credential.getCertificateChain())
                        .map(X509Certificate::getNotAfter)
                        .min(Date::compareTo)
                        .map(Date::getTime)
                        .orElse(0L);
    }

    private void updateCredential(X509Credential credential)
    {
        this.delegatedCredential = credential;
        this.delegatedCredentialExpiration = expiryDateFor(credential);
        this.saved = false;
    }

    /**
     * Getter for property requestCredentialId.
     *
     * @return Value of property requestCredentialId.
     */
    public long getId()
    {
        return id;
    }

    /**
     * Getter for property credentialName.
     *
     * @return Value of property credentialName.
     */
    public String getCredentialName()
    {
        return credentialName;
    }

    @Override
    public synchronized String toString()
    {
        return "RequestCredential[" + credentialName + "," +
                ((delegatedCredential == null) ? "nondelegated" : "delegated, remaining lifetime : " + getDelegatedCredentialRemainingLifetime() + " millis") +
                "  ]";
    }

    public synchronized  void saveCredential()
    {
        if (!saved) {
            storage.saveRequestCredential(this);
            saved = true;
        }
    }

    /**
     * Getter for property role.
     *
     * @return Value of property role.
     */
    public String getRole()
    {
        return role;
    }

    /**
     * Getter for property delegatedCredentialExpiration.
     *
     * @return Value of property delegatedCredentialExpiration.
     */
    public synchronized long getDelegatedCredentialExpiration()
    {
        return delegatedCredentialExpiration;
    }

    /**
     * Getter for property creationtime.
     *
     * @return Value of property creationtime.
     */
    public long getCreationtime()
    {
        return creationtime;
    }

    /** Returns the remaining lifetime in milliseconds for a credential. */

    public synchronized long getDelegatedCredentialRemainingLifetime()
    {
        long lifetime = delegatedCredentialExpiration - System.currentTimeMillis();
        return Math.max(0, lifetime);
    }

    /**
     * Allow a user to specify an alternative credential to use.
     */
    public synchronized void acceptAlternative(String description) throws SRMInvalidRequestException
    {
        if (description == null) {
            return;
        }

        if (!description.startsWith("gridsite:")) {
            throw new SRMInvalidRequestException("Unknown credential type: " +
                    description);
        }

        String idLabel = description.substring(9);

        try {
            long id = Long.parseLong(idLabel);
            RequestCredential credential = storage.getRequestCredential(id);

            if (credential == null || !credential.credentialName.equals(this.credentialName)) {
                // when the credential belongs to someone else, throw the same
                // error as if no credential was found.  This prevents
                // discovering which other credentials exist.
                throw new IllegalArgumentException("Unknown credential: " +
                        credentialName);
            }

            if (credential.getDelegatedCredentialRemainingLifetime() == 0) {
                throw new IllegalArgumentException("Credential " + description +
                        " has expired");
            }

            updateCredential(credential.delegatedCredential);
        } catch (NumberFormatException e) {
            throw new SRMInvalidRequestException("Badly formatted credential id: " + idLabel);
        }
    }
}
