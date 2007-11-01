// $Id: JobCreator.java,v 1.2 2007-08-03 20:20:40 timur Exp $
// $Log: not supported by cvs2svn $
// Revision 1.1  2005/01/14 23:07:15  timur
// moving general srm code in a separate repository
//
// Revision 1.5  2004/11/09 08:04:48  tigran
// added SerialVersion ID
//
// Revision 1.4  2004/08/06 19:35:25  timur
// merging branch srm-branch-12_May_2004 into the trunk
//
// Revision 1.3.2.4  2004/06/18 22:20:52  timur
// adding sql database storage for requests
//
// Revision 1.3.2.3  2004/06/16 22:14:32  timur
// copy works for mulfile request
//
// Revision 1.3.2.2  2004/06/16 19:44:33  timur
// added cvs logging tags and fermi copyright headers at the top, removed Copier.java and CopyJob.java
//

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
 * JobCreator.java
 *
 * Created on March 24, 2004, 3:25 PM
 */

package org.dcache.srm.scheduler;

import java.util.HashSet;
import java.util.Set;
import java.util.WeakHashMap;
import java.lang.ref.WeakReference;
import java.util.Map;
import java.util.Iterator;
import java.util.Collections;

/**
 *
 * @author  timur
 */
public  class JobCreator  {
    private static final JobCreatorStorage defaultstorage = new HashtableJobCreatorStorage();
    
    
    private static final Set jobCreatorStorages = new HashSet();
    
    private static final Map weakJobCreatorStorage =
    Collections.synchronizedMap(new WeakHashMap());
    
    
    public static final JobCreator getJobCreator(String creatorId) {
        
        Object o = weakJobCreatorStorage.get(creatorId);
        if(o!= null) {
            WeakReference ref = (WeakReference) o;
            Object o1 = ref.get();
            if(o1 != null) {
                return (JobCreator) o1;
            }
        }
        
        JobCreatorStorage jobCreatorStoragesArray[];
        synchronized(jobCreatorStorages) {
            jobCreatorStoragesArray =
            (JobCreatorStorage[])jobCreatorStorages.toArray(new JobCreatorStorage[0]);
        }
        
        for(int i = 0; i<jobCreatorStoragesArray.length; ++i) {
            JobCreator jobCreator = (JobCreator) jobCreatorStoragesArray[i].getJobCreator(creatorId);
            if(jobCreator != null) {
                weakJobCreatorStorage.put(jobCreator.id,new WeakReference(jobCreator));
                return jobCreator;
            }
        }
        
        return null;
    }
    
    
    private String id;
    
    private int priority;
    
    public JobCreatorStorage storage;
    
    private static final long serialVersionUID = 1315039071295594246L;
    
    /** Creates a new instance of JobCreator */
    protected JobCreator(String id) {
        this(id, 0, defaultstorage);
    }
    protected JobCreator(String id,int priority) {
        this(id, priority, defaultstorage);
    }
    protected JobCreator(String id,JobCreatorStorage storage) {
        this(id,0,storage);
    }
    
    protected JobCreator(String id,int priority,JobCreatorStorage storage) {
        
        if(priority < 0) {
            throw new IllegalArgumentException(
            "priority should be greater than or equal to zero");
        }
        
        this.id = new String(id);
        this.priority = priority;
        this.storage = storage;
        weakJobCreatorStorage.put(new String(id), new WeakReference(this));
    }
    
    
    public static final void registerJobCreatorStorage(JobCreatorStorage jobCreatorStorage) {
        synchronized(jobCreatorStorages) {
            jobCreatorStorages.add(jobCreatorStorage);
        }
    }
    
    static{
        registerJobCreatorStorage(defaultstorage);
    }
    
    /** Getter for property name.
     * @return Value of property name.
     *
     */
    public java.lang.String getId() {
        return id;
    }
    
    /** Setter for property name.
     * @param name New value of property name.
     *
     */
    public void setId(java.lang.String id) {
        this.id = id;
    }
    
    /** Getter for property priority.
     * @return Value of property priority.
     *
     */
    public int getPriority() {
        return priority;
    }
    
    /** Setter for property priority.
     * @param priority New value of property priority.
     *
     */
    public void setPriority(int priority) {
        this.priority = priority;
    }
    
    public String toString() {
        return "JobCreator name="+id+" priority="+priority;
    }
    
    public boolean equals(Object o) {
        if(o == null || ! (o instanceof JobCreator)) {
            return false;
        }
        JobCreator creator = (JobCreator) o;
        return id.equals(creator.id) && priority == creator.priority;
    }
    
    public int hashCode() {
        return id.hashCode() + priority;
    }
    
    public void saveCreator()
    {
        this.storage.saveJobCreator(this);
    }
    
}
