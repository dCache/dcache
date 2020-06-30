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
package org.dcache.services.bulk.job;

import com.google.common.collect.Range;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FsPath;

import org.dcache.services.bulk.BulkJobExecutionException;
import org.dcache.services.bulk.BulkRequest;
import org.dcache.services.bulk.BulkServiceException;
import org.dcache.util.list.DirectoryEntry;
import org.dcache.util.list.DirectoryStream;
import org.dcache.util.list.ListDirectoryHandler;
import org.dcache.vehicles.FileAttributes;

/**
 *  For expanding the contents of directories.
 *
 *  Provides both breadth-first and depth-first algorithms.
 *
 *  These, along with what kinds of targets the expansion should submit as
 *  single target jobs, are determined by the specific activity.
 */
public final class TargetExpansionJob extends MultipleTargetJob
{
    public enum ExpansionType
    {
        BREADTH_FIRST, DEPTH_FIRST
    }

    /**
     *  Set on the basis of the specific request.
     */
    private final ExpansionType   expansionType;

    /**
     *  Streaming pnfs list handler for scalability.
     */
    private ListDirectoryHandler listHandler;

    public TargetExpansionJob(BulkJobKey key,
                              BulkJobKey parentKey,
                              BulkRequest request,
                              TargetType targetType,
                              ExpansionType expansionType)
    {
        super(key, parentKey, request, targetType);
        this.expansionType = expansionType;
    }

    @Override
    public boolean cancel()
    {
        if (super.cancel()) {
            completionHandler.clear();
            return true;
        }

        return false;
    }

    public void setListHandler(ListDirectoryHandler listHandler)
    {
        this.listHandler = listHandler;
    }

    /**
     *  Entry point for the job.
     *
     *  In breadth-first expansion, each directory encountered "forks"
     *  a new job.
     *
     *  In depth-first expansion, the "root" expansion job is responsible
     *  for all expansion of subdirectories (via recursion).  This
     *  avoids a memory-unfriendly chain of job dependencies (breadth-first
     *  expansions have no such chained dependencies).
     */
    @Override
    protected void doRun()
    {
        LOGGER.trace("{}, doRun() called ...", loggingPrefix());

        switch (expansionType)
        {
            case BREADTH_FIRST:
            case DEPTH_FIRST:
                break;
            default:
                String error = String.format("Expansion of %s failed; unknown "
                                                             + "expansion "
                                                             + "algorithm: %s.",
                                             key, expansionType.name());
                errorObject = new BulkJobExecutionException(error);
                completionHandler.jobFailed(this);
                return;
        }

        try {
            expand(target, key, parentKey, attributes);
            setState(State.COMPLETED);
        } catch (CacheException | BulkServiceException e) {
            errorObject = e;
            completionHandler.jobFailed(this);
        }

        LOGGER.trace("{}, doRun(), key {}, target {} exiting ...",
                     loggingPrefix(), key, target);
    }

    protected void postCompletion()
    {
        completionHandler.jobCompleted(this);
    }

    private void expand(String target,
                        BulkJobKey key,
                        BulkJobKey parentKey,
                        FileAttributes attributes)
                    throws CacheException, BulkServiceException
    {
        /*
         *  Fail-fast in case the job has been cancelled.
         */
        if (isTerminated()) {
            LOGGER.debug("{}, expansion job for {} {}; returning ...",
                         loggingPrefix(), target, state.name());
            return;
        }

        LOGGER.debug("{}, expand() called for {}: key {}, parent {}.",
                     loggingPrefix(),
                     target,
                     key.getJobId(),
                     parentKey.getJobId());

        if (expansionType == ExpansionType.BREADTH_FIRST) {
            /*
             *  In breadth-first it should not matter that directories
             *  are processed as targets before their children.
             */
            checkForDirectoryTarget(target, parentKey, attributes);
        }

        try {
            LOGGER.debug("{}, listing target {}", key.getJobId(), target);
            DirectoryStream stream = getDirectoryListing(target);
            LOGGER.debug("{}, handling children", key.getJobId());
            for (DirectoryEntry entry : stream) {
                handleChildTarget(target, key, entry);
                if (isTerminated()) {
                    LOGGER.debug("{}, expansion job for {} {}; returning ...",
                                 loggingPrefix(), target, state.name());
                    return;
                }
            }
            LOGGER.debug("{}, finished handling children", key.getJobId());

            if (expansionType == ExpansionType.DEPTH_FIRST) {
                /*
                 *  Choosing depth-first implies the need to preserve
                 *  that order in the visiting of the nodes.  Since
                 *  submission and execution are asynchronous, it is thus
                 *  necessary to wait after a directory expansion until
                 *  all its children have completed. This guarantees these
                 *  directories will always be processed only after all
                 *  their descendants have been (necessary for, say,
                 *  deletion).
                 */
                LOGGER.debug("{}, {}, waiting for children of "
                                             + "{} to terminate.",
                             loggingPrefix(),
                             expansionType.name(),
                             key.getJobId());

                completionHandler.waitForChildren(key.getJobId());

                LOGGER.debug("{}, {}, children of " + "{} have terminated.",
                            loggingPrefix(),
                            expansionType.name(),
                            key.getJobId());

                /*
                 *  In depth-first it may indeed be necessary to process
                 *  directories as targets only after all their children
                 *  have terminated, so we do this here.
                 */
                checkForDirectoryTarget(target, parentKey, attributes);
            }
        } catch (InterruptedException e) {
            State state = getState();
            if (state == State.CANCELLED) {
                /*
                 *  The call to cancel will have
                 *  already notified the listener.
                 */
            } else {
                setState(State.CANCELLED);
                completionHandler.jobInterrupted(this);
            }
        }

        LOGGER.trace("{}, expand() {}, exiting ...", loggingPrefix(), target);
    }

    /*
     *  Creates single target jobs for files and for directories if
     *  they are not to be expanded but are targets.
     *
     *  For breadth-first expansion of directories, a new job is submitted.
     *  Each job carries this job's key as parent key.
     *
     *  Depth-first does not submit a new job, but calls expand recursively.
     */
    private void handleChildTarget(String target,
                                   BulkJobKey key,
                                   DirectoryEntry entry)
                    throws CacheException, BulkServiceException
    {
        /*
         *  Fail-fast in case the job has been cancelled.
         */
        if (isTerminated()) {
            LOGGER.debug("{}, expansion job for {} {}; returning ...",
                         loggingPrefix(), target, state.name());
            return;
        }

        LOGGER.trace("{}, handleChildTarget() called for {}, entry {}, parent {}.",
                     loggingPrefix(), target, entry.getName(), key.getKey());

        String childTarget = target + "/" + entry.getName();
        FileAttributes attributes = entry.getFileAttributes();

        switch (attributes.getFileType())
        {
            case DIR:
                switch (request.getExpandDirectories()) {
                    case ALL:
                        LOGGER.debug("{}, {}, found directory {}, expand ALL.",
                                     loggingPrefix(),
                                     expansionType.name(),
                                     childTarget);

                        switch (expansionType)
                        {
                            case BREADTH_FIRST:
                                submitTargetExpansionJob(childTarget, attributes);
                                break;
                            case DEPTH_FIRST:
                                /*
                                 *  The new key is merely a numerical marker
                                 *  and does not actually represent a new job
                                 *  (as in the case of breadth-first).
                                 *
                                 *  The expansion method gives all child tasks
                                 *  this key as parent key, so that waitForChildren
                                 *  can identify which jobs belong to the barrier.
                                 *
                                 *  This expansion only returns when all such
                                 *  children complete.
                                 */
                                expand(childTarget,
                                       BulkJobKey.newKey(request.getId()),
                                       key,
                                       attributes);
                                break;
                        }
                        break;
                    case TARGETS:
                        /*
                         *  We only need to check this if the directory
                         *  is not being expanded.  An expanded directory
                         *  is checked for target status in the main
                         *  expand method.
                         */
                        checkForDirectoryTarget(childTarget,
                                                key,
                                                attributes);
                        break;
                        /*
                         *  If expandDirectories == NONE, we wouldn't
                         *  be here.
                         */

                    default:
                }
                break;
            case LINK:
            case REGULAR:
                checkForFileTarget(childTarget, key, attributes);
                break;
            case SPECIAL:
            default:
                LOGGER.trace("{}, handleChildTarget(), "
                                             + "cannot handle special file {}.",
                             loggingPrefix(), childTarget);
                break;
        }
    }

    private DirectoryStream getDirectoryListing(String target)
                    throws CacheException, InterruptedException
    {
        LOGGER.trace("{}, getDirectoryListing() called ...", loggingPrefix());

        FsPath path = computeFsPath(request.getTargetPrefix(), target);

        LOGGER.trace("{}, getDirectoryListing(), path {}, calling list ...",
                     loggingPrefix(), path);
        return listHandler.list(subject,
                                restriction,
                                path,
                                null,
                                Range.closedOpen(0, Integer.MAX_VALUE),
                                REQUIRED_ATTRIBUTES);
    }

    private void checkForDirectoryTarget(String target,
                                         BulkJobKey parentKey,
                                         FileAttributes attributes)
                    throws BulkServiceException
    {
        /*
         *  Fail-fast in case the job has been cancelled.
         */
        if (isTerminated()) {
            LOGGER.debug("{}, expansion job for {} {}; returning ...",
                         loggingPrefix(), target, state.name());
            return;
        }

        switch (targetType)
        {
            case BOTH:
            case DIR:
                LOGGER.debug("{} {}, directory {} included as target.",
                             loggingPrefix(), expansionType.name(), target);
                submitSingleTargetJob(target, parentKey, attributes);
                break;
            default:
                LOGGER.debug("{} {}, directory {} not included as target, "
                                             + "skipping.",
                             loggingPrefix(), expansionType.name(), target);
        }
    }

    private void checkForFileTarget(String target,
                                    BulkJobKey parentKey,
                                    FileAttributes attributes)
                    throws BulkServiceException
    {
        /*
         *  Fail-fast in case the job has been cancelled.
         */
        if (isTerminated()) {
            LOGGER.debug("{}, expansion job for {} {}; returning ...",
                         loggingPrefix(), target, state.name());
            return;
        }

        switch (targetType)
        {
            case BOTH:
            case FILE:
                LOGGER.debug("{} {}, file {} included as target.",
                             loggingPrefix(), expansionType.name(), target);
                submitSingleTargetJob(target, parentKey, attributes);
                break;
            default:
                LOGGER.debug("{} {}, file {} not included as target, "
                                             + "skipping.",
                             loggingPrefix(), expansionType.name(), target);
        }
    }

    private String loggingPrefix()
    {
        return this.target + " TargetExpansionJob";
    }
}
