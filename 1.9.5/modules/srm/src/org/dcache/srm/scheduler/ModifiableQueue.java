// $Id$
// $Log: not supported by cvs2svn $
// Revision 1.4  2006/04/21 22:58:27  timur
// we do not need a thread running when we start a remote transfer, but we want to control the number of the transfers, I hope the code accomplishes this now, though an addition of the new job state
//
// Revision 1.3  2006/04/12 23:16:24  timur
// storing state transition time in database, storing transferId for copy requests in database, renaming tables if schema changes without asking
//
// Revision 1.2  2005/03/01 23:10:39  timur
// Modified the database scema to increase database operations performance and to account for reserved space"and to account for reserved space
//
// Revision 1.1  2005/01/14 23:07:15  timur
// moving general srm code in a separate repository
//
// Revision 1.6  2004/10/28 02:41:31  timur
// changed the database scema a little bit, fixed various synchronization bugs in the scheduler, added interactive shell to the File System srm
//
// Revision 1.5  2004/08/31 18:12:29  timur
// use linked list to store the queue
//
// Revision 1.4  2004/08/10 17:03:47  timur
// more monitoring, change file request state, when request is complete
//
// Revision 1.3  2004/08/06 19:35:25  timur
// merging branch srm-branch-12_May_2004 into the trunk
//
// Revision 1.2.2.5  2004/07/12 21:52:07  timur
// remote srm error handling is improved, minor issues fixed
//
// Revision 1.2.2.4  2004/07/09 22:14:54  timur
// more synchronization problems resloved
//
// Revision 1.2.2.3  2004/07/09 01:58:40  timur
// fixed a syncronization problem, added auto dirs creation for copy function
//
// Revision 1.2.2.2  2004/07/02 20:10:25  timur
// fixed the leak of sql connections, added propogation of srm errors
//
// Revision 1.2.2.1  2004/06/16 19:44:34  timur
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
 * ModifiableQueue.java
 *
 * Created on March 23, 2004, 9:47 AM
 *  Ideas based on EDU.oswego.cs.dl.util.concurrent.BoundedLinkedQueue,
 *  an example for the Doug Lee's "Concurrent Programming in Java"
 *
 */

package org.dcache.srm.scheduler;

import java.util.List;
import java.util.LinkedList;
import java.util.Iterator;
import org.dcache.srm.SRMInvalidRequestException;
/**
 *
 * @author  timur
 */
public class ModifiableQueue  {
    String name;
    String scheduler_name;
    private int capacity=1024;
    private List queue = new LinkedList();
    
    /** Creates a new instance of ModifiableQueue */
    public ModifiableQueue(String name, String scheduler_name, int capacity) {
        this.name = name;
        this.scheduler_name = scheduler_name;
        this.capacity=capacity;
    }
    
    public ModifiableQueue(String name, String scheduler_name) {
        this(name,scheduler_name,1024);
    }
    
    
    public int size() {
        synchronized(queue){
            return queue.size();
        }
    }
    
    
    public  Job peek()  throws java.sql.SQLException,SRMInvalidRequestException {
        
        Long headId;
        synchronized(queue){
                if(queue.isEmpty()) {
                    
                    ////System.out.println(" queue is empty, returning null ");
                    return null;
                }
                headId = (Long) queue.get(0);
                //System.out.println("headId is "+headId+" returning job ");
                return Job.getJob(headId);
            }
    }
    
    
    public Job take()
            throws InterruptedException,
            java.sql.SQLException,
            SRMInvalidRequestException {
        for(;;) {
            Long id = null;
            synchronized(queue) {
                if(!queue.isEmpty()){
                     id =(Long) queue.remove(0);
                     queue.notifyAll();
                }
                if(id != null) 
                {
                    return Job.getJob(id);
                }
                try {
                    queue.wait();
                }
                catch(InterruptedException ie) {
                    queue.notify();
                    throw ie;
                }
            }
        }
    }
    
    public Job poll(long msecs)
            throws InterruptedException,
            java.sql.SQLException,
            SRMInvalidRequestException {
        
        long waitTime = msecs;
        long start = (msecs <= 0)? 0: System.currentTimeMillis();
        for(;;) {
            Long id = null;
            synchronized(queue) {
                if(!queue.isEmpty()){
                     id =(Long) queue.remove(0);
                     queue.notifyAll();
                }
                if(id != null) 
                {
                    return Job.getJob(id);
                }
                if ( waitTime <= 0) {
                    return null;
                }

                try {
                    queue.wait(waitTime);
                }
                catch(InterruptedException ie) {
                    queue.notify();
                    throw ie;
                }
            }
            waitTime = msecs - (System.currentTimeMillis() - start);
        }
    }
    
    
    public void put(Job job) throws InterruptedException {
        //System.out.println("QUEUE.put("+job.getId()+")");
        if(job == null) {
            throw new IllegalArgumentException("job is null");
        }
        Long id = job.getId();
        job = null;    
        for(;;){
            synchronized(queue) {
                if(queue.size() < capacity) {
                    queue.add(id);
                    queue.notifyAll();
                    return;
                }
                try {
                    queue.wait();
                }
                catch(InterruptedException ie) {
                    queue.notify();
                    throw ie;
                }
            }
        }
    }
    
    
    public boolean offer(Job job, long msecs) throws InterruptedException {
        //System.out.println("QUEUE.offer("+job.getId()+","+msecs+")");
        if(job == null) {
            throw new IllegalArgumentException("job is null");
        }
        long waitTime = msecs;
        long start = (msecs <= 0)? 0: System.currentTimeMillis();
        Long id = job.getId();
        job = null;    
        for(;;){
            synchronized(queue) {
                if(queue.size() < capacity) {
                    queue.add(id);
                    queue.notifyAll();
                    //System.out.println("QUEUE.offer() returns true");
                    return true;
                }
                if(waitTime <= 0) {
                    return false;
                }
                try {
                    queue.wait(waitTime);
                }
                catch(InterruptedException ie) {
                    queue.notify();
                    throw ie;
                }
                waitTime = msecs - (System.currentTimeMillis() - start);
                
            }
        }
    }
    
    public boolean isEmpty() {
        synchronized(queue){
            return queue.isEmpty();
        }
    }
    
    public Job remove(Job job)  {
        //System.out.println("QUEUE.remove(" +job.getId()+")");
        if(job == null ) {
            return null;
        }
        Long id = job.getId();
        synchronized(queue) {
            boolean found = queue.contains(id);
            while(queue.contains(id)){
                queue.remove(id);
            }
            if(found) {
                queue.notifyAll();
                 return job;
            }
            return null;
        }
    }
    
    public void setCapacity(int newCapacity) {
        synchronized(queue) {
            capacity = newCapacity;
            queue.notifyAll();
        }
    }
    
    public interface ValueCalculator {
        public int calculateValue(int queueLength, int queuePosition, Job job);
    }
    
    public synchronized Job getGreatestValueObject(ValueCalculator calc)  
            throws java.sql.SQLException, SRMInvalidRequestException{
        Job greatestValueJob;
        int greatestValue;
       //System.out.println("QUEUE.getGreatestValueObject()");
        synchronized(queue) {
            
            if(queue.isEmpty()) {
               //System.out.println("QUEUE.getGreatestValueObject() returns NULL, queue is empty");
                return null;
            }
            int size = queue.size();
            greatestValueJob = Job.getJob((Long)queue.get(0));
            greatestValue = calc.calculateValue(size,0,greatestValueJob);
            Job currentJob = greatestValueJob;
            Long currentJobId = currentJob.getId();
            //int i=0;
            //while(currentJob.getNextJobId() != null) {
            // i++;
            int index =0;
            for (Iterator i = queue.iterator(); i.hasNext();){
                currentJobId = (Long)i.next();
                currentJob = Job.getJob(currentJobId);
                int currentValue = calc.calculateValue(size,index,currentJob);
                if(currentValue > greatestValue) {
                    greatestValueJob = currentJob;
                    greatestValue = currentValue;
                }
                index++;
            }
            //System.out.println("QUEUE.getGreatestValueObject() returns" +greatestValueJob.getId());
            return greatestValueJob;
        }
    }
    
    public String printQueue()  throws java.sql.SQLException{
        
        StringBuffer sb = new StringBuffer();
        printQueue(sb);
        return sb.toString();
    }
    public void printQueue(StringBuffer sb)  throws java.sql.SQLException{
        synchronized(queue)
        {
            if(queue.isEmpty()) {
                    sb.append("Queue is empty\n");
                    return;
            }
            int index =0;
            for (Iterator i = queue.iterator(); i.hasNext();){
                sb.append("queue element # "+index+" "+i.next()).append('\n');
                index++;  
            }
        }
        
    }
    
}
