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

import org.junit.Before;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.dcache.services.bulk.BulkServiceException;
import org.dcache.services.bulk.job.BulkJob;
import org.dcache.services.bulk.job.BulkJobKey;
import org.dcache.services.bulk.queue.SignalAware;

/**
 *  Tests the logic of the completion listener for correctness.
 */
public class BulkJobCompletionHandlerTest
{
    class SimpleTestJob extends BulkJob
    {
        SimpleTestJob(BulkJobKey parentKey) throws BulkServiceException
        {
            super(BulkJobKey.newKey(requestId), parentKey, "test");
        }

        @Override
        protected void doRun()
        {
            // NOP
        }

        @Override
        protected void postCompletion()
        {
            // NOP
        }
    }

    class JobSet
    {
        static final int ROOT = 0;
        static final int FILE_1 = 1;
        static final int DIR_1 = 2;
        static final int FILE_2 = 3;
        static final int DIR_2 = 4;
        static final int FILE_3 = 5;
        static final int DIR_3 = 6;

        BulkJob[] jobs;

        JobSet(String requestId) throws Exception
        {
            jobs = new BulkJob[7];
            jobs[ROOT] = addJobWithParent(BulkJobKey.newKey(requestId));
            jobs[FILE_1] = addJobWithParent(jobs[ROOT].getKey());
            jobs[DIR_1] = addJobWithParent(jobs[ROOT].getKey());
            jobs[FILE_2] = addJobWithParent(jobs[DIR_1].getKey());
            jobs[DIR_2] = addJobWithParent(jobs[DIR_1].getKey());
            jobs[FILE_3] = addJobWithParent(jobs[DIR_1].getKey());
            jobs[DIR_3] = addJobWithParent(jobs[DIR_2].getKey());
        }

        Long idOf(int job)
        {
            return jobs[job].getKey().getJobId();
        }

        void whenAllJobsTerminate()
        {
            for (BulkJob job : jobs) {
                listener.jobCompleted(job);
            }
        }

        void whenJobTerminates(int job)
        {
            listener.jobCompleted(jobs[job]);
        }

        private BulkJob addJobWithParent(BulkJobKey parentId)
                        throws Exception
        {
            SimpleTestJob job = new SimpleTestJob(parentId);
            listener.addChild(job);
            return job;
        }
    }

    class SignalAwareQueue implements SignalAware
    {
        private AtomicInteger signalled = new AtomicInteger(0);

        @Override
        public void signal()
        {
            signalled.incrementAndGet();
        }

        @Override
        public int countSignals()
        {
            return signalled.get();
        }
    }

    SignalAware              queue;
    BulkJobCompletionHandler listener;
    String                   requestId;
    Long                     requestJobId;
    JobSet                   jobSet;

    @Before
    public void setup() throws Exception
    {
        queue = new SignalAwareQueue();
        listener = new BulkJobCompletionHandler(queue);
        requestId = UUID.randomUUID().toString();
        requestJobId = BulkJobKey.newKey(requestId).getJobId();
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenListenerIsShared() throws Exception
    {
        givenJobWithParentFromDifferentRequest();
    }

    @Test
    public void shouldConsiderChildrenTerminatedForEmptyDir() throws Exception
    {
        givenNewJobSet();
        assertChildrenHaveAllTerminated(jobSet.idOf(JobSet.DIR_3));
    }

    @Test
    public void shouldConsiderChildrenTerminatedForDirWithOnlyChildTerminated() throws Exception
    {
        givenNewJobSet();
        jobSet.whenJobTerminates(JobSet.DIR_3);
        assertChildrenHaveAllTerminated(jobSet.idOf(JobSet.DIR_2));
    }

    @Test
    public void shouldNotConsiderChildrenTerminatedPrematurely() throws Exception
    {
        givenNewJobSet();
        jobSet.whenJobTerminates(JobSet.DIR_3);
        assertChildrenHaveNotAllTerminated(jobSet.idOf(JobSet.DIR_1));
        jobSet.whenJobTerminates(JobSet.DIR_2);
        assertChildrenHaveNotAllTerminated(jobSet.idOf(JobSet.DIR_1));
        jobSet.whenJobTerminates(JobSet.FILE_2);
        assertChildrenHaveNotAllTerminated(jobSet.idOf(JobSet.DIR_1));
        jobSet.whenJobTerminates(JobSet.FILE_3);
        assertChildrenHaveAllTerminated(jobSet.idOf(JobSet.DIR_1));
    }

    @Test
    public void shouldNotConsiderRequestCompletedWithNoJobsButMarkerPresent() throws Exception
    {
        givenRequestProcessingStarted();
        givenNewJobSet();
        jobSet.whenAllJobsTerminate();
        assertRequestNotTerminated();
    }

    @Test
    public void shouldCompleteRequestIfAllJobsTerminated() throws Exception
    {
        givenRequestProcessingStarted();
        givenNewJobSet();
        givenRequestProcessingFinished();
        jobSet.whenAllJobsTerminate();
        assertRequestTerminated();
    }

    @Test
    public void queueShouldReceiveSignalFromQueueOnJobTermination() throws Exception
    {
        givenNewJobSet();
        jobSet.whenAllJobsTerminate();
        assertAllJobsHaveSignalledQueue();
    }

    private void assertRequestTerminated()
    {
        assert(listener.isRequestCompleted());
    }

    private void assertRequestNotTerminated()
    {
        assert(!listener.isRequestCompleted());
    }

    private void assertChildrenHaveAllTerminated(Long parentId)
    {
        assert(listener.areChildrenAllTerminated(parentId));
    }

    private void assertChildrenHaveNotAllTerminated(Long parentId)
    {
        assert(!listener.areChildrenAllTerminated(parentId));
    }

    private void assertAllJobsHaveSignalledQueue()
    {
        assert(queue.countSignals() == jobSet.jobs.length);
    }

    private void givenNewJobSet() throws Exception
    {
        jobSet = new JobSet(requestId);
    }

    private BulkJob givenJobWithParentFromDifferentRequest()
                    throws Exception
    {
        SimpleTestJob job = new SimpleTestJob(BulkJobKey.newKey(UUID.randomUUID()
                                                                    .toString()));
        listener.addChild(job);
        return job;
    }

    private void givenRequestProcessingStarted()
    {
        listener.requestProcessingStarted(requestJobId);
    }

    private void givenRequestProcessingFinished()
    {
        listener.requestProcessingFinished(requestJobId);
    }
}
