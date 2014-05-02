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

package org.dcache.srm.scheduler;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.request.Job;

public class ModifiableQueue  {
    private final Class<? extends Job> type;
    private final List<Long> queue = new LinkedList<>();

    public ModifiableQueue(Class<? extends Job> type) {
        this.type = type;
    }

    public int size() {
        synchronized(queue){
            return queue.size();
        }
    }


    public  Job peek()  throws SRMInvalidRequestException {

        long headId;
        synchronized(queue){
                if(queue.isEmpty()) {

                    ////System.out.println(" queue is empty, returning null ");
                    return null;
                }
                headId =  queue.get(0);
                //System.out.println("headId is "+headId+" returning job ");
        }
        return Job.getJob(headId, type);
    }


    public Job take()
            throws InterruptedException,
            SRMInvalidRequestException {
        while (true) {
            Long id = null;
            synchronized (queue) {
                if (!queue.isEmpty()) {
                    id = queue.remove(0);
                    queue.notifyAll();
                }
                if (id != null) {
                    return Job.getJob(id, type);
                }
                try {
                    queue.wait();
                } catch (InterruptedException ie) {
                    queue.notify();
                    throw ie;
                }
            }
        }
    }

    public void put(Job job) {
        long id = job.getId();
        synchronized (queue) {
            queue.add(id);
            queue.notifyAll();
        }
    }

    public boolean isEmpty() {
        synchronized(queue){
            return queue.isEmpty();
        }
    }

    public Job remove(Job job)  {
        if(job == null ) {
            return null;
        }
        long id = job.getId();
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

    public interface ValueCalculator {
        public int calculateValue(int queueLength, int queuePosition, Job job);
    }

    public Job getGreatestValueObject(ValueCalculator calc)
            throws SRMInvalidRequestException{
        Job greatestValueJob;
        int greatestValue;
       //System.out.println("QUEUE.getGreatestValueObject()");
        List<Long> queueCopy;

        synchronized(queue) {

            if(queue.isEmpty()) {
               //System.out.println("QUEUE.getGreatestValueObject() returns NULL, queue is empty");
                return null;
            }
            queueCopy = new ArrayList<>(queue);
        }

        greatestValueJob =null;
        greatestValue = Integer.MIN_VALUE;
        //int i=0;
        //while(currentJob.getNextJobId() != null) {
        // i++;
        int index =0;
        int size = queueCopy.size();
        for (long currentJobId:queueCopy) {
            Job currentJob = Job.getJob(currentJobId, type);
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

    public void printQueue(StringBuilder sb) {
        synchronized(queue)
        {
            if(queue.isEmpty()) {
                    sb.append("Queue is empty\n");
                    return;
            }
            int index =0;
            for (long nextId:queue){
                sb.append("queue element # ").append(index).append(" : ")
                        .append(nextId).append('\n');
                index++;
            }
        }

    }

    public Class<? extends Job> getType()
    {
        return type;
    }
}
