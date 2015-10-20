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

import java.util.Arrays;

public enum State {
    /** Initial state: no processing has happened for job. */
    UNSCHEDULED    ("Unscheduled",    0),

    /** Job is waiting in scheduler for initial activity. */
    QUEUED         ("Queued"         ,2),

    /** Job is being processed. */
    INPROGRESS     ("InProgress"     ,3),

    /** Job is successful and waits client requesting its status when the Ready queue isn't full. */
    RQUEUED        ("RQueued"        ,6),

    /** Job is successful, this is known to client and further client interaction is expected. */
    READY          ("Ready"          ,7),

    /** Client has called SRMv1 setFileStatus("Running"). */
    TRANSFERRING   ("Transferring"   ,8),

    /** Job is successful and no further client interaction is possible. */
    DONE           ("Done"           ,9, true),

    /** Client interaction prevented job from completing successfully. */
    CANCELED       ("Canceled"       ,10, true),

    /** A resource limitation or some failure prevented job from completing successfully. */
    FAILED         ("Failed"         ,11, true),
    RESTORED       ("Restored"       ,12);

    private final String name;
    private final int stateId;
    private final boolean isFinal;

    private static final long serialVersionUID = 4561665427863772427L;

    State(String name, int stateId) {
        this(name, stateId, false);
    }

    State(String name, int stateId, boolean isFinal) {
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

    public boolean hasName(String name) {
        return this.name.equalsIgnoreCase(name);
    }

    public boolean hasId(int id) {
        return this.stateId == id;
    }

    public boolean isFinal() {
        return isFinal;
    }

    /** this package visible method is used to restore the State from
     * the database
     */
    public static State getState(String state) throws IllegalArgumentException {
        if (state == null || state.equalsIgnoreCase("null")) {
            throw new NullPointerException(" null state ");
        }
        try {
            return Arrays.stream(values())
                    .filter(s -> s.hasName(state))
                    .findFirst()
                    .orElseGet(() -> getState(Integer.parseInt(state)));
        } catch (NumberFormatException nfe) {
            throw new IllegalArgumentException("Unknown State:"+state);
        }
    }

    public static State getState(int stateId) throws IllegalArgumentException {
        return Arrays.stream(values())
                .filter(s -> s.hasId(stateId))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown State Id:"+stateId));
    }
}
