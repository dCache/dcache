// $Id: FileState.java,v 1.4 2007-08-03 15:46:03 timur Exp $
// $Log: not supported by cvs2svn $

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
 * FileState.java
 *
 * Created on March 19, 2004, 2:51 PM
 */

package diskCacheV111.services.space;

/**
 *
 * @author  timur
 */
public final class FileState implements java.io.Serializable {

    private final String name;
    private final int stateId;

    public static final FileState RESERVED       = new FileState("Reserved",    0);
    public static final FileState TRANSFERRING    = new FileState("Transferring", 1);
    public static final FileState STORED         = new FileState("Stored",      2);
    public static final FileState FLUSHED        = new FileState("Flushed",     3);

    /**
     * Creates a new instance of FileState
     */
    private FileState(String name,int stateId) {
        this.name = name;
        this.stateId = stateId;
    }

    public static FileState[] getAllStates() {
        return new FileState[] {
         RESERVED,
         TRANSFERRING,
         STORED,
         FLUSHED
        };
    }
    public String toString() {
        return name;
    }

    public int getStateId() {
        return stateId;
    }
    /**
     * this package visible method is used to restore the FileState from
     * the database
     */
    public static FileState getState(String state) throws IllegalArgumentException {
        if(state == null || state.equalsIgnoreCase("null")) {
            throw new NullPointerException(" null state ");
        }

        if(RESERVED.name.equals(state)) {
            return RESERVED;
        }

        if(TRANSFERRING.name.equals(state)) {
            return TRANSFERRING;
        }

        if(STORED.name.equals(state)) {
            return STORED;
        }

        if(FLUSHED.name.equals(state)) {
            return FLUSHED;
        }
        try{
            int stateId = Integer.parseInt(state);
            return getState(stateId);
        }
        catch(Exception e) {
            throw new IllegalArgumentException("Unknown State");
        }
    }

    public static FileState getState(int stateId) throws IllegalArgumentException {

        if(RESERVED.stateId == stateId) {
            return RESERVED;
        }

        if(TRANSFERRING.stateId == stateId) {
            return TRANSFERRING;
        }

        if(STORED.stateId == stateId) {
            return STORED;
        }

        if(FLUSHED.stateId == stateId) {
            return FLUSHED;
        }

        throw new IllegalArgumentException("Unknown State Id");
    }

    public static boolean isFinalState(FileState state) {
        return state == FLUSHED;
    }

    // this is what we need to correctly implement
    // serialization of the singleton
    public Object readResolve()
    		throws java.io.ObjectStreamException {
        return getState(stateId);
    }

    public int hashCode() {
        return name.hashCode();
    }
}
