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
package org.dcache.qos.listeners;

import diskCacheV111.util.PnfsId;
import org.dcache.qos.QoSException;
import org.dcache.qos.vehicles.QoSAdjustmentResponse;
import org.dcache.qos.vehicles.QoSScannerVerificationRequest;
import org.dcache.qos.vehicles.QoSVerificationRequest;

public interface QoSVerificationListener {
  /**
   *  Request to check the file's status on disk and tape against its requirements.
   *
   *  @param verificationRequest composing file update and requirements data.
   */
  void fileQoSVerificationRequested(QoSVerificationRequest verificationRequest) throws QoSException;

  /**
   *  Request to check a collection of files residing at a given (pool) location.
   *
   *  @param verificationRequest includes the location name, originating message type, and list of
   *                             pnfsIds.
   */
  void fileQoSVerificationRequested(QoSScannerVerificationRequest verificationRequest) throws QoSException;

  /**
   *  A single adjustment request has completed.
   *
   *  @param adjustmentResponse encapsulates the file id, action, and status of the adjustment.
   */
  void fileQoSAdjustmentCompleted(QoSAdjustmentResponse adjustmentResponse) throws QoSException;

  /**
   *  This is a notification to cancel and remove the verification operation for a file.
   *
   *  @param pnfsId the file for which to cancel verification operation.
   */
  void fileQoSVerificationCancelled(PnfsId pnfsId) throws QoSException;

  /**
   *  Scanning activity has been cancelled; this is a notification to cancel all outstanding
   *  verification operations having this pool as 'parent'.
   *
   *  @param pool the parent pool to search for in order to cancel verification operations.
   */
  void fileQoSBatchedVerificationCancelled(String pool) throws QoSException;

  /**
   *  The verifier will usually need to know if a location has been manually excluded.
   *  This information is conveyed by the scanner.
   *
   *  @param location whose exclusion status is being reported.
   */
  void notifyLocationExclusion(String location) throws QoSException;

  /**
   *  The verifier will usually need to know if a location has been manually (re)included.
   *  This information is conveyed by the scanner.
   *
   *  @param location whose exclusion status is being reported.
   */
  void notifyLocationInclusion(String location) throws QoSException;
}
