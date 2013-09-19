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
 * State.java
 *
 * Created on March 19, 2004, 2:51 PM
 */

package org.dcache.srm.scheduler;

/**
 *
 * @author  timur
 */
public enum State {
    PENDING        ("Pending",        0),
    PRIORITYTQUEUED("PriorityTQueued",1),
    TQUEUED        ("TQueued"        ,2),
    RUNNING        ("Running"        ,3),
    RETRYWAIT      ("RetryWait"      ,4),
    ASYNCWAIT      ("AsyncWait"      ,5),
    RQUEUED        ("RQueued"        ,6),
    READY          ("Ready"          ,7),
    TRANSFERRING   ("Transferring"   ,8),
    DONE           ("Done"           ,9, true),
    CANCELED       ("Canceled"       ,10, true),
    FAILED         ("Failed"         ,11, true),
    RESTORED       ("Restored"       ,12),
    RUNNINGWITHOUTTHREAD("RunningWithoutThread"       ,13);
    
    private final String name;
    private final int stateId;
    private final boolean isFinal;
    
    
    
    private static final long serialVersionUID = 4561665427863772427L;
    
    /** Creates a new instance of non final State  */
    private State(String name,int stateId) {
        this(name, stateId, false);
    }

    /** Creates a new instance of State */
    private State(String name,int stateId, boolean isFinal) {
        this.name = name;
        this.stateId = stateId;
        this.isFinal = isFinal;
    }
    
    public String toString() {
        return name;
    }
    
    public int getStateId() {
        return stateId;
    }
    /** this package visible method is used to restore the State from
     * the database
     */
    public static State getState(String state) throws IllegalArgumentException {
        if(state == null || state.equalsIgnoreCase("null")) {
            throw new NullPointerException(" null state ");
        }
        for(State aState: values()) {
            if(aState.name.equalsIgnoreCase(state)) {
                return aState;
            }
        }
        try{
            int stateId = Integer.parseInt(state);
            return getState(stateId);
        } catch (NumberFormatException nfe) {
            throw new IllegalArgumentException("Unknown State:"+state);
        }
        }
    
    public static State getState(int stateId) throws IllegalArgumentException {
        
      for(State aState: values()) {
            if(aState.stateId == stateId) {
                return aState;
    }
        }
        throw new IllegalArgumentException("Unknown State Id:"+stateId);
    }
    
   /**
     * a utility method which tells if the given state is a final state
     * @param state
     * @return true if the state is final
     */
    public static boolean isFinalState(State state) {
        return state.isFinal;
    }
    
    /**
     * a utility method which tells if this state is a final state
     * @return true if the state is final
     */
    public boolean isFinalState() {
        return isFinal;
}
    
}
