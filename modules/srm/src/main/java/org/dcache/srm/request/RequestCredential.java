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
import org.ietf.jgss.GSSCredential;
import java.util.Set;
import java.util.HashSet;
import java.util.Iterator;
import java.util.WeakHashMap;
import java.util.Collections;
import java.util.Map;
import java.lang.ref.WeakReference;
import org.dcache.srm.scheduler.JobIdGeneratorFactory;
import java.sql.SQLException;
/**
 *
 * @author  timur
 */
public class RequestCredential {
    private static final Map weakRequestCredentialStorage =
    Collections.synchronizedMap(new WeakHashMap());
    static
    {
        //System.out.println("RequestCredential static constructor");
    }
    private Long id;
    private long creationtime;
    private String credentialName;
    private String role;
    private boolean saved; //false by default
    private GSSCredential delegatedCredential;
    private long delegatedCredentialExpiration;
    private RequestCredentialStorage storage;

    //if this number goes to 0, the request credential delegated credential part is set to null
    private int credential_users;
    private static final Set requestCredentailStorages = new HashSet();

    public static final void registerRequestCredentialStorage(RequestCredentialStorage requestCredentialStorage) {
        synchronized(requestCredentailStorages) {
            requestCredentailStorages.add(requestCredentialStorage);
        }

    }

/*    public void start() {
        new Thread(this).start();
    }
    public void run() {
        while(true) {
            //System.out.println("delegatedCredential = "+delegatedCredential+" id="+id);

            try {
                Thread.sleep(100);
            }
            catch(Exception e) {
                return;
            }
        }
    }
  */
    public static RequestCredential getRequestCredential(Long requestCredentialId) {
      synchronized(weakRequestCredentialStorage) {
          Object o = weakRequestCredentialStorage.get(requestCredentialId);
            //System.out.println("RequestCredential.getRequestCredential: weakRequestCredentialStorage.get("+requestCredentialId+") = "+o);
            if(o!= null) {
                WeakReference ref = (WeakReference) o;
                Object o1 = ref.get();
              //System.out.println("RequestCredential.getRequestCredential: weakRequestCredentialStorage.get("+requestCredentialId+").get() = "+o1);
                if(o1 != null) {
                    return (RequestCredential) o1;
                }
            }
      }
       //System.out.println("RequestCredential.getRequestCredential: weakRequestCredentialStorage does not have it");
/*      synchronized(weakRequestCredentialStorage) {
       for(Iterator i =weakRequestCredentialStorage.keySet().iterator();i.hasNext();) {
            Long l = (Long) i.next();
            weakRequestCredentialStorage.get(l);
            Object o = weakRequestCredentialStorage.get(requestCredentialId);
            //System.out.println("----RequestCredential.getRequestCredential: weakRequestCredentialStorage.get("+l+") = "+o);
            if(o!= null) {
                WeakReference ref = (WeakReference) o;
                Object o1 = ref.get();
              //System.out.println("=====RequestCredential.getRequestCredential: weakRequestCredentialStorage.get("+requestCredentialId+").get() = "+o1);
            }
        }
      }
*/
        RequestCredentialStorage requestCreatorStoragesArray[];
        synchronized(requestCredentailStorages) {
            requestCreatorStoragesArray =
            (RequestCredentialStorage[]) requestCredentailStorages
                    .toArray(new RequestCredentialStorage[requestCredentailStorages
                            .size()]);
        }

        for (RequestCredentialStorage aRequestCreatorStoragesArray : requestCreatorStoragesArray) {
            RequestCredential requestCredential = aRequestCreatorStoragesArray
                    .getRequestCredential(requestCredentialId);
            if (requestCredential != null) {
                synchronized (weakRequestCredentialStorage) {
                    //System.out.println("RequestCredential.getRequestCredential weakRequestCredentialStorage.put("+requestCredential.id+
                    //","+requestCredential+")");
                    weakRequestCredentialStorage
                            .put(requestCredential.id, new WeakReference(requestCredential));
                }
                return requestCredential;
            }
        }
        return null;
    }

    public static RequestCredential getRequestCredential(String credentialName,String role)
    {
       //System.out.println("RequestCredential.getRequestCredential("+credentialName+","+role+")");
        synchronized(weakRequestCredentialStorage) {
            for (Object o : weakRequestCredentialStorage.values()) {
                WeakReference ref = (WeakReference) o;
                Object o1 = ref.get();
                //System.out.println("RequestCredential.getRequestCredential: weakRequestCredentialStorage.next = "+o1);
                if (o1 != null) {
                    RequestCredential cred = (RequestCredential) o1;
                    String credName = cred.getCredentialName();
                    String credRole = cred.getRole();
                    if (credName.equals(credentialName)) {
                        //System.out.println("RequestCredential.getRequestCredential: weakRequestCredentialStorage.next has same credential name ");
                        if ((role == null && credRole == null) || role != null && role
                                .equals(credRole)) {
                            return cred;
                        }
                        //System.out.println("RequestCredential.getRequestCredential: weakRequestCredentialStorage.next but not the same ");
                    }
                }
            }
        }

        RequestCredentialStorage requestCreatorStoragesArray[];
        synchronized(requestCredentailStorages) {
            requestCreatorStoragesArray =
            (RequestCredentialStorage[]) requestCredentailStorages
                    .toArray(new RequestCredentialStorage[requestCredentailStorages
                            .size()]);
        }

        for (RequestCredentialStorage requestCredentialStorage : requestCreatorStoragesArray) {
            RequestCredential requestCredential =
                    requestCredentialStorage
                            .getRequestCredential(credentialName, role);
            if (requestCredential != null) {
                synchronized (weakRequestCredentialStorage) {
                    //System.out.println("RequestCredential.getRequestCredential weakRequestCredentialStorage.put("+requestCredential.id+
                    //","+requestCredential+")");
                    weakRequestCredentialStorage
                            .put(requestCredential.id, new WeakReference(requestCredential));
                }
                return requestCredential;
            }
        }
        return null;
    }

    /** Creates a new instance of requestCredential */
    public RequestCredential(String credentialName,
                            String role,
                            GSSCredential delegatedCredential,
                            RequestCredentialStorage storage)
                            throws SQLException,org.ietf.jgss.GSSException {
        //System.out.println("RequestCredential  constructor");
        //start();

        this.id =
            JobIdGeneratorFactory.getJobIdGeneratorFactory().getJobIdGenerator().getNextId();
        this.creationtime = System.currentTimeMillis();
        this.credentialName = credentialName;
        this.role = role;
        if(delegatedCredential != null) {
            //System.out.println("RequestCredential.delegatedCredential 1 assigned"+delegatedCredential);
            this.delegatedCredential = delegatedCredential;

        ////System.out.println("delegatedCredential.getRemainingLifetime()="+delegatedCredential.getRemainingLifetime()+" sec");
            this.delegatedCredentialExpiration = creationtime + delegatedCredential.getRemainingLifetime()*1000L;
        }
        this.storage = storage;
        synchronized(weakRequestCredentialStorage) {
           //System.out.println("RequestCredential constructor weakRequestCredentialStorage.put("+id+
           //        ","+this+")");

            weakRequestCredentialStorage.put(this.id, new WeakReference(this));
        }
    }

    /** restores a previously stored instance of the requestcredential*/
    public RequestCredential(Long id,
                             long creationtime,
                            String credentialName,
                            String role,
                            GSSCredential delegatedCredential,
                            long delegatedCredentialExpiration,
                            RequestCredentialStorage storage)
                            throws SQLException{
        //System.out.println("RequestCredential restore constructor");
        //new Throwable().printStackTrace();
        //start();
        this.id = id;
        this.creationtime = creationtime;
        this.credentialName = credentialName;
        this.role = role;
        if(delegatedCredential != null) {
            //System.out.println("RequestCredential.delegatedCredential 2 assigned"+delegatedCredential);
            this.delegatedCredential = delegatedCredential;
            this.delegatedCredentialExpiration = delegatedCredentialExpiration;
        }
        this.storage = storage;
        synchronized(weakRequestCredentialStorage) {
           //System.out.println("RequestCredential restore weakRequestCredentialStorage.put("+id+
           //         ","+this+")");
            weakRequestCredentialStorage.put(this.id, new WeakReference(this));
        }
    }
    //public static

    /** Getter for property delegatedCredential.
     * @return Value of property delegatedCredential.
     *
     */
    public org.ietf.jgss.GSSCredential getDelegatedCredential() {
        return delegatedCredential;
    }

    public void keepBestDelegatedCredential(GSSCredential delegatedCredential)
    throws org.ietf.jgss.GSSException
    {
       if(delegatedCredential == null)
       {
        //System.out.println("keepBestDelegatedCredential(delegatedCredential is null)");
           return;
       }
    //System.out.println("keepBestDelegatedCredential(delegatedCredential is non null)");

       long newCredentialExpiration = System.currentTimeMillis() +
                delegatedCredential.getRemainingLifetime()*1000L;
       if(this.delegatedCredential == null ||
        newCredentialExpiration > this.delegatedCredentialExpiration)
       {
            //System.out.println("RequestCredential.delegatedCredential 3 assigned"+delegatedCredential);
            this.delegatedCredential = delegatedCredential;
            this.delegatedCredentialExpiration = newCredentialExpiration;
            saved = false;
            return;
       }

    }

    /** Getter for property requestCredentialId.
     * @return Value of property requestCredentialId.
     *
     */
    public Long getId() {
        return id;
    }

    /** Getter for property credentialName.
     * @return Value of property credentialName.
     *
     */
    public java.lang.String getCredentialName() {
        return credentialName;
    }

    @Override
    public String toString() {
        return "RequestCredential["+credentialName+","+
        ((delegatedCredential==null)?"nondelegated":"delegated, remaining lifetime : "+getDelegatedCredentialRemainingLifetime()+" millis")+
        "  ]";
    }

    public void decreaseCredential_users() {
        //System.out.println("RequestCredentials.decreaseCredential_users");
        credential_users--;
        if(credential_users == 0) {
            //System.out.println("RequestCredential.delegatedCredential 4 assigned null");
            delegatedCredential = null;
        }
        storage.saveRequestCredential(this);
    }

    /** Getter for property credential_users.
     * @return Value of property credential_users.
     *
     */
    public int getCredential_users() {
        return credential_users;
    }

    /** Setter for property credential_users.
     * @param credential_users New value of property credential_users.
     *
     */
    public void setCredential_users(int credential_users) {
        this.credential_users = credential_users;
    }

    public void saveCredential() {
        if(saved) {
            return;
        }
    //System.out.println("RequestCredential.saveCredential(), id="+this.id+" storage="+storage);
        storage.saveRequestCredential(this);
        saved = true;
    }

    /** Getter for property role.
     * @return Value of property role.
     *
     */
    public java.lang.String getRole() {
        return role;
    }

    /**
     * Getter for property delegatedCredentialExpiration.
     * @return Value of property delegatedCredentialExpiration.
     */
    public long getDelegatedCredentialExpiration() {
        return delegatedCredentialExpiration;
    }

    /**
     * Getter for property creationtime.
     * @return Value of property creationtime.
     */
    public long getCreationtime() {
        return creationtime;
    }

   /**
    * Returns the remaining lifetime in milliseconds for a credential.
    */

    public long getDelegatedCredentialRemainingLifetime()
    {
        long lifetime =  delegatedCredentialExpiration - System.currentTimeMillis();
        return lifetime <0 ? 0 : lifetime;
    }

    public boolean isSaved() {
        return saved;
    }

    public void setSaved(boolean saved) {
        this.saved = saved;
    }


}
