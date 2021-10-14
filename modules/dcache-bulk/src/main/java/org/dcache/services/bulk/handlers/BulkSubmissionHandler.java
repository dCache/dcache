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
package org.dcache.services.bulk.handlers;

import javax.security.auth.Subject;
import org.dcache.services.bulk.BulkRequest;
import org.dcache.services.bulk.BulkServiceException;
import org.dcache.services.bulk.job.BulkJobKey;
import org.dcache.services.bulk.job.MultipleTargetJob;
import org.dcache.vehicles.FileAttributes;

/**
 * Defines the basic submission methods for interacting with the queue.
 */
public interface BulkSubmissionHandler {

    /**
     * Unrecoverable internal failure.  This usually means that the target has not yet been
     * processed into a job.
     *
     * @param requestId unique identifier
     * @param target    specific target
     * @param exception error
     */
    void abortRequestTarget(String requestId, String target,
          Throwable exception)
          throws BulkServiceException;

    /**
     * Services request (from user) to (cancel) the request.
     *
     * @param subject   of the user.
     * @param requestId of the request to cancel.
     * @throws BulkServiceException
     */
    void cancelRequest(Subject subject, String requestId)
          throws BulkServiceException;

    /**
     * Services request (from user) to free all data associated with the request.
     *
     * @param subject   of the user.
     * @param requestId of the request to clear.
     * @throws BulkServiceException
     */
    void clearRequest(Subject subject, String requestId)
          throws BulkServiceException;

    /**
     * Should configure and submit the top-level job responsible for processing all targets defined
     * by the bulk request.
     *
     * @param request to be processed.
     * @throws BulkServiceException
     */
    void submitRequest(BulkRequest request) throws BulkServiceException;

    /**
     * Submits a job which takes action on a specific target.  The job is created by the factory on
     * the basis of the request's activity definition (this is passed through the parent).
     *
     * @param target     file or directory.
     * @param parentKey  key of the generating job (i.e., with a directory target).
     * @param attributes received during directory listing.
     * @param parent     the generating job.
     * @throws BulkServiceException
     */
    void submitSingleTargetJob(String target,
          BulkJobKey parentKey,
          FileAttributes attributes,
          MultipleTargetJob parent)
          throws BulkServiceException;

    /**
     * Submits a job which manages multiple targets resulting from a target (i.e., directory)
     * expansion.
     *
     * @param target     directory
     * @param attributes of the directory
     * @param parent     the generating job.
     * @throws BulkServiceException
     */
    void submitTargetExpansionJob(String target,
          FileAttributes attributes,
          MultipleTargetJob parent)
          throws BulkServiceException;
}
