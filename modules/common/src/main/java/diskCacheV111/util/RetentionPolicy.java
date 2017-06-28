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
 *
 * @author  timur
 */
public final class RetentionPolicy implements Serializable {
    private static final long serialVersionUID = -2206085171393244383L;
    private final String _name;
    private final int _id;

    /**
     * A {@code RetentionPolicy} with highest probability of loss, but is appropriate
     * for data that can be replaced because other copies can be accessed in a
     * timely fashion.
     */
    public static final RetentionPolicy REPLICA    = new RetentionPolicy("REPLICA", 2);

    /**
     * A {@code RetentionPolicy} for an intermediate level and refers to the data which can
     * be replaced by lengthy or effort-full processes.
     */
    public static final RetentionPolicy OUTPUT     = new RetentionPolicy("OUTPUT", 1);

    /**
     * A {@code RetentionPolicy} with  low probability of loss.
     */
    public static final RetentionPolicy CUSTODIAL  = new RetentionPolicy("CUSTODIAL", 0);

    private final static RetentionPolicy[] ALL_POLICIES = {
        REPLICA,
        OUTPUT,
        CUSTODIAL
    };

    /**
     * Creates a new instance of FileState
     */
    private RetentionPolicy(String name,int id) {
        _name = name;
        _id = id;
    }

    public static RetentionPolicy[] getAllPolicies() {
            return ALL_POLICIES;
    }

    @Override
    public String toString() {
        return _name;
    }

    public int getId() {
        return _id;
    }
    /**
     * this package visible method is used to restore the FileState from
     * the database
     */
    public static RetentionPolicy getRetentionPolicy(String policy) throws IllegalArgumentException {
        if(policy == null || policy.equalsIgnoreCase("null")) {
            throw new NullPointerException(" null state ");
        }

        for(RetentionPolicy rp: getAllPolicies()) {
            if (rp._name.equalsIgnoreCase(policy)) {
                return rp;
            }
        }

        try {
            int id = Integer.parseInt(policy);
            return getRetentionPolicy(id);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Unknown Policy");
        }
    }

    public static RetentionPolicy getRetentionPolicy(int id) throws IllegalArgumentException {

        for(RetentionPolicy rp: getAllPolicies()) {
            if (rp._id == id) {
                return rp;
            }
        }

        throw new IllegalArgumentException("Unknown policy Id");
    }

    public static RetentionPolicy valueOf(String value) {
        return getRetentionPolicy(value);
    }

    @Override
    public boolean equals(Object obj) {
        return ( obj instanceof RetentionPolicy) && ( ((RetentionPolicy)obj).getId() == this.getId() );
    }

    @Override
    public int hashCode() {
        return _name.hashCode();
    }

    public Object readResolve() throws ObjectStreamException {
       return RetentionPolicy.getRetentionPolicy(getId());
    }
}
