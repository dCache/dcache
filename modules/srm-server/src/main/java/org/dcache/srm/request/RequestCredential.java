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
 * requestCredential.java
 *
 * Created on May 13, 2004, 11:04 AM
 */

package org.dcache.srm.request;

import com.google.common.collect.MapMaker;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.dcache.srm.scheduler.JobIdGeneratorFactory;

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
    private GSSCredential delegatedCredential;
    private long delegatedCredentialExpiration;

    public static void registerRequestCredentialStorage(RequestCredentialStorage requestCredentialStorage)
    {
        requestCredentailStorages.add(requestCredentialStorage);
    }

    public static RequestCredential getRequestCredential(Long requestCredentialId) throws DataAccessException
    {
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
            throws GSSException
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
            throws GSSException
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
                             GSSCredential delegatedCredential,
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
                             GSSCredential delegatedCredential,
                             RequestCredentialStorage storage) throws GSSException
    {
        this.id = JobIdGeneratorFactory.getJobIdGeneratorFactory().getJobIdGenerator().getNextId();
        this.creationtime = System.currentTimeMillis();
        this.credentialName = credentialName;
        this.role = role;
        this.delegatedCredential = delegatedCredential;
        this.delegatedCredentialExpiration = System.currentTimeMillis() +
                    delegatedCredential.getRemainingLifetime() * 1000L;
        this.storage = storage;
    }

    public synchronized GSSCredential getDelegatedCredential()
    {
        return delegatedCredential;
    }

    public synchronized void keepBestDelegatedCredential(GSSCredential delegatedCredential)
            throws GSSException
    {
        if (delegatedCredential != null) {
            long newCredentialExpiration = System.currentTimeMillis() +
                    delegatedCredential.getRemainingLifetime() * 1000L;
            if (this.delegatedCredential == null ||
                    newCredentialExpiration > this.delegatedCredentialExpiration) {
                this.delegatedCredential = delegatedCredential;
                this.delegatedCredentialExpiration = newCredentialExpiration;
                this.saved = false;
            }
        }
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
}
