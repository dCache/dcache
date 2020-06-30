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

import com.google.gson.GsonBuilder;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PnfsHandler;

import org.dcache.namespace.FileAttribute;
import org.dcache.namespace.FileType;
import org.dcache.services.bulk.BulkRequest;
import org.dcache.services.bulk.BulkRequest.Depth;
import org.dcache.services.bulk.BulkServiceException;
import org.dcache.services.bulk.NamespaceHandlerAware;
import org.dcache.vehicles.FileAttributes;

/**
 *  Acts as a shallow container for a list of targets which
 *  may not be associated with each other via a common parent.
 *  Immediate wrapper for a BulkRequest.
 */
public class BulkRequestJob extends MultipleTargetJob
                implements NamespaceHandlerAware
{
    protected static final Set<FileAttribute> REQUIRED_ATTRIBUTES
                    = Collections.unmodifiableSet(EnumSet.of(FileAttribute.PNFSID,
                                                             FileAttribute.TYPE));

    private PnfsHandler pnfsHandler;

    public BulkRequestJob(BulkJobKey key,
                          BulkRequest request,
                          TargetType targetType)
    {
        /*
         *  Top-level job has no parent.
         */
        super(key, null, request, targetType);
        this.target = request.getTarget();
    }

    @Override
    protected void doOnCancellation()
    {
       /*
        *  We do not need to call the completion handler
        *  because this job is never added to the parent-child map,
        *  and that method relies on the parent key, which for
        *  this kind of job is null.
        */
    }

    @Override
    public void setNamespaceHandler(PnfsHandler pnfsHandler)
    {
        this.pnfsHandler = pnfsHandler;
    }

    @Override
    protected void doRun()
    {
        LOGGER.trace("RequestJob, doRun() called ...");

        /*
         *  Requests support a compound target, which takes the form
         *  of a json array.
         */
        String[] targets;
        if (target.contains("[")) {
            GsonBuilder builder = new GsonBuilder();
            targets = builder.create().fromJson(target, String[].class);
        } else {
            targets = new String[] { target };
        }

        String prefix = request.getTargetPrefix();

        completionHandler.requestProcessingStarted(key.getJobId());

        for (String t : targets) {
            FsPath path = computeFsPath(prefix, t);
            LOGGER.debug("RequestJob, path {}.", path);
            try {
                FileAttributes attributes
                                = pnfsHandler.getFileAttributes(path,
                                                                REQUIRED_ATTRIBUTES);
                if (attributes.getFileType() == FileType.DIR) {
                    handleDirectory(t, attributes);
                } else if (attributes.getFileType() != FileType.SPECIAL) {
                    handleFile(t, attributes);
                }
            } catch (CacheException | BulkServiceException e) {
                LOGGER.error("RequestJob, path {}, error {}.",
                             path, e.toString());
                try {
                    submissionHandler.abortRequestTarget(request.getId(), t, e);
                } catch (BulkServiceException e1) {
                    LOGGER.error("RequestJob, could not abort {}: {}.",
                                 t, e1.toString());
                }
            }

            /*
             *  Fail-fast in case the job has been cancelled.
             */
            if (isTerminated()) {
                break;
            }
        }

        setState(State.COMPLETED);

        LOGGER.trace("{}, RequestJob, doRun() exiting ...", target);
    }

    protected void postCompletion()
    {
        completionHandler.requestProcessingFinished(key.getJobId());
    }

    private void handleDirectory(String target, FileAttributes attributes)
                    throws BulkServiceException
    {
        LOGGER.trace("RequestJob, handleDirectory() called for {}.",
                     target);

        Depth expand = request.getExpandDirectories();
        switch (expand) {
            case ALL:
            case TARGETS:
                LOGGER.debug("RequestJob, expand {},"
                                             + " submitting directory {}"
                                             + " for expansion.",
                            expand, target);
                submitTargetExpansionJob(target,
                                         attributes);
                break;
            case NONE:
            default:
                if (targetType != TargetType.FILE) {
                    LOGGER.debug("RequestJob, expand NONE, directory {} "
                                                 + "included as target.",
                                 target);
                    submitSingleTargetJob(target, key, attributes);
                }
                LOGGER.debug("RequestJob, expand NONE, directory {} "
                                             + "not included as target, skipping.",
                             target);
        }

        LOGGER.trace("RequestJob, handleDirectory() for {} exiting ...",
                     target);
    }

    private void handleFile(String target,
                            FileAttributes attributes)
                    throws BulkServiceException
    {
        LOGGER.trace("RequestJob handleFile() called for {}.",
                     target);

        switch(targetType)
        {
            case BOTH:
            case FILE:
                LOGGER.debug("RequestJob, file {} included"
                                             + " as target.",
                             target);
                submitSingleTargetJob(target, key, attributes);
                break;
            default:
                LOGGER.debug("RequestJob, file {} "
                                             + "not included"
                                             + " as target, skipping.",
                             target);
        }

        LOGGER.trace("RequestJob, handleFile() for {} exiting ...",
                     target);
    }
}
