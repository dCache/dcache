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
 *  Used to identify messages dealt with by the qos system.  The first five are received from
 *  external sources, the next three are internal notifications based on pool monitor updates;
 *  the last one is used by the qos system to indicate it is rerequesting validation of
 *  the file's requirements.
 *  <p/>
 *  <table>
 *     <tr><td>CORRUPT_FILE</td><td>REPORT BROKEN FILE DISCOVERED IN REPOSITORY</td></tr>
 *     <tr><td>CLEAR_CACHE_LOCATION</td><td>ENTRY REMOVED FROM REPOSITORY AND LOCATION CLEARED IN NAMESPACE</td></tr>
 *     <tr><td>ADD_CACHE_LOCATION</td><td>ENTRY ADDED TO REPOSITORY AND LOCATION ADDED TO NAMESPACE</td></tr>
 *     <tr><td>QOS_MODIFIED</td><td>REQUEST TO CHANGE FILE QOS</td></tr>
 *     <tr><td>QOS_MODIFIED_CANCELED</td><td>REQUEST TO CANCEL FILE QOS CHANGE (IF POSSIBLE)</td></tr>
 *     <tr><td>POOL_STATUS_DOWN</td><td>POOL DISABLED OR OFFLINE</td></tr>
 *     <tr><td>POOL_STATUS_UP</td><td>POOL ENABLED OR BACK ONLINE; ALSO USED FOR PERIODIC OR FORCED SCAN</td></tr>
 *     <tr><td>SYSTEM_SCAN</td><td>CHECK NAMESPACE INODE-BY-INODE (PERIODIC)</td></tr>
 *     <tr><td>VALIDATE_ONLY</td><td>REQUEST TO FIND REQUIREMENTS AND LOCATIONS OF A FILE</td></tr>
 *  </table>
 */
public enum QoSMessageType {
    CORRUPT_FILE,           // REPORT BROKEN FILE DISCOVERED IN REPOSITORY
    CLEAR_CACHE_LOCATION,   // ENTRY REMOVED FROM REPOSITORY AND LOCATION CLEARED IN NAMESPACE
    ADD_CACHE_LOCATION,     // ENTRY ADDED TO REPOSITORY AND LOCATION ADDED TO NAMESPACE
    QOS_MODIFIED,           // REQUEST TO CHANGE FILE QOS
    QOS_MODIFIED_CANCELED,  // REQUEST TO CANCEL FILE QOS CHANGE (IF POSSIBLE)
    POOL_STATUS_DOWN,       // POOL DISABLED OR OFFLINE
    POOL_STATUS_UP,         // POOL ENABLED OR BACK ONLINE; ALSO USED FOR PERIODIC OR FORCED SCAN
    SYSTEM_SCAN,            // CHECK NAMESPACE INODE-BY-INODE (PERIODIC)
    VALIDATE_ONLY           // REQUEST TO FIND REQUIREMENTS AND LOCATIONS OF A FILE
}
