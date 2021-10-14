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
package org.dcache.qos.data;

/**
 * Indicates what kind of adjustment needs to be done with reference to a file's QoS status and
 * requirements.
 * <p/>
 * </table>
 * <tr><td>COPY_REPLICA<td></td>the file needs another replica</td></tr>
 * <tr><td>CACHE_REPLICA<td></td>the file has an excess copy</td></tr>
 * <tr><td>PERSIST_REPLICA<td></td>the file needs a replica, but has a cached one it can
 * convert</td></tr>
 * <tr><td>UNSET_PRECIOUS_REPLICA<td></td>the file has a precious replica which is no longer needs
 * to be precious</td></tr>
 * <tr><td>WAIT_FOR_STAGE<td></td>the file has no accessible replicas, but can be staged</td></tr>
 * <tr><td>FLUSH<td></td>the file has disk replicas but needs to go to tape</td></tr>
 * <tr><td>NOTIFY_MISSING<td></td>the file has no known locations</td></tr>
 * <tr><td>NOTIFY_INACCESSIBLE<td></td>the file is currently unreadable everywhere</td></tr>
 * <tr><td>NOTIFY_OUT_OF_SYNC<td></td>the locations in namespace and pools do not
 * correspond</td></tr>
 * <tr><td>MISCONFIGURED_POOL_GROUP<td></td>there are not enough available pools in the required
 * pool group</td></tr>
 * <tr><td>VOID<td></td>nothing needs to be done</td></tr>
 * </table>
 */
public enum QoSAction {
    COPY_REPLICA,               // the file needs another replica
    CACHE_REPLICA,              // the file has an excess copy
    PERSIST_REPLICA,            // the file needs a replica, but has a cached one it can convert
    UNSET_PRECIOUS_REPLICA,     // the file has a precious replica which is no longer needs to be precious
    WAIT_FOR_STAGE,             // the file has no accessible replicas, but can be staged
    FLUSH,                      // the file has disk replicas but needs to go to tape
    NOTIFY_MISSING,             // the file has no known locations
    NOTIFY_INACCESSIBLE,        // the file is currently unreadable everywhere
    NOTIFY_OUT_OF_SYNC,         // the locations in namespace and pools do not correspond
    MISCONFIGURED_POOL_GROUP,   // there are not enough available pools in the required pool group
    VOID                        // nothing needs to be done
}
