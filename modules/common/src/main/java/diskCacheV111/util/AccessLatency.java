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
package diskCacheV111.util;

import java.io.ObjectStreamException;
import java.io.Serializable;

/*
 * FIXME: this class have to be converted into java enum, but we have to keep
 * compatibility with existing pools.
 */

/**
 * @author timur
 */
public final class AccessLatency implements Serializable {


    private static final long serialVersionUID = -6473179157424112725L;
    private final String _name;
    private final int _id;

    /**
     * An {@code AccessLatency} with lowest possible latency for given system.
     */
    public static final AccessLatency ONLINE = new AccessLatency("ONLINE", 1);

    /**
     * An {@code AccessLatency} which indicates that file's access latency can be improved by
     * changing to {@link ONLINE} state.
     */
    public static final AccessLatency NEARLINE = new AccessLatency("NEARLINE", 0);

    private final static AccessLatency[] ALL_LATENCIES = {
          ONLINE,
          NEARLINE
    };

    /**
     * Creates a new instance of FileState
     */
    private AccessLatency(String name, int id) {
        _name = name;
        _id = id;
    }

    public static AccessLatency[] getAllLatencies() {
        return ALL_LATENCIES;
    }

    @Override
    public String toString() {
        return _name;
    }

    public int getId() {
        return _id;
    }

    /**
     * this package visible method is used to restore the FileState from the database
     */
    public static AccessLatency getAccessLatency(String latency) throws IllegalArgumentException {
        if (latency == null || latency.equalsIgnoreCase("null")) {
            throw new NullPointerException(" null state ");
        }

        for (AccessLatency al : getAllLatencies()) {
            if (al._name.equalsIgnoreCase(latency)) {
                return al;
            }
        }

        try {
            int id = Integer.parseInt(latency);
            return getAccessLatency(id);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Unknown AccessLatency");
        }
    }

    public static AccessLatency getAccessLatency(int id) throws IllegalArgumentException {

        for (AccessLatency al : getAllLatencies()) {
            if (al._id == id) {
                return al;
            }
        }

        throw new IllegalArgumentException("Unknown AccessLatency Id");
    }

    @Override
    public boolean equals(Object obj) {
        return (obj instanceof AccessLatency) && (((AccessLatency) obj).getId() == this.getId());
    }

    @Override
    public int hashCode() {
        return _name.hashCode();
    }

    public static AccessLatency valueOf(String value) {
        return getAccessLatency(value);
    }

    public Object readResolve() throws ObjectStreamException {
        return AccessLatency.getAccessLatency(getId());
    }
}
