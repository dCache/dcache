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
package org.dcache.services.bulk.queue;

import com.google.common.collect.Ordering;
import com.google.common.collect.TreeMultimap;
import com.google.common.util.concurrent.MoreExecutors;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import diskCacheV111.util.PnfsHandler;

import org.dcache.auth.Subjects;
import org.dcache.auth.attributes.Restrictions;
import org.dcache.services.bulk.BulkRequest;
import org.dcache.services.bulk.BulkRequest.Depth;
import org.dcache.services.bulk.BulkRequestStatus;
import org.dcache.services.bulk.BulkRequestStatus.Status;
import org.dcache.services.bulk.handlers.BulkJobCompletionHandler;
import org.dcache.services.bulk.handlers.BulkRequestCompletionHandler;
import org.dcache.services.bulk.handlers.BulkSubmissionHandler;
import org.dcache.services.bulk.job.BulkJob;
import org.dcache.services.bulk.job.BulkJob.State;
import org.dcache.services.bulk.job.BulkJobKey;
import org.dcache.services.bulk.job.BulkRequestJob;
import org.dcache.services.bulk.job.MultipleTargetJob.TargetType;
import org.dcache.services.bulk.job.SingleTargetJob;
import org.dcache.services.bulk.store.BulkRequestStore;
import org.dcache.services.bulk.store.memory.InMemoryBulkRequestStore;
import org.dcache.services.bulk.util.BulkServiceStatistics;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class BulkServiceQueueTest
{
    class PlaceholderJob extends SingleTargetJob
    {
        public PlaceholderJob() throws Exception
        {
            super(BulkJobKey.newKey(request.getId()),
                  BulkJobKey.newKey(request.getId()),
                  "test");
            this.completionHandler = jobCompletionHandler;
        }

        @Override
        protected void doRun()
        {
        }
    }

    BulkServiceQueue             queue;
    BulkRequestStore             store;
    BulkSubmissionHandler        submissionHandler;
    BulkRequestCompletionHandler completionHandler;
    BulkJobCompletionHandler     jobCompletionHandler;
    BulkRequest                  request;
    BulkRequestJob               requestJob;

    @Before
    public void setUp()
    {
        queue = new BulkServiceQueue()
        {
            public void initialize()
            {
                maxRequests=1;
                submitted = new LinkedHashMap<>();
                runningQueue = new ArrayDeque<>();
                waitingQueue = new HashSet<>();
                readyQueue = TreeMultimap.create(requestStore.getStatusComparator(),
                                                 Ordering.natural());
                jobProcessor = new NextJobProcessor();
                postProcessor = new JobPostProcessor();
                sweeper = new TerminalSweeper();
            }
        };
        store = new InMemoryBulkRequestStore();
        submissionHandler = mock(BulkSubmissionHandler.class);
        completionHandler = mock(BulkRequestCompletionHandler.class);
        jobCompletionHandler = mock(BulkJobCompletionHandler.class);
        queue.setRequestStore(store);
        queue.setCompletionHandler(completionHandler);
        queue.setSubmissionHandler(submissionHandler);
        queue.setBulkJobExecutorService(mock(ExecutorService.class));
        queue.setCleanupExecutorService(MoreExecutors.newDirectExecutorService());
        queue.setStatistics(mock(BulkServiceStatistics.class));
        queue.initialize();
    }

    @Test
    public void shouldCallSubmitRequestAndUpdateToStartedOnNewRequest()
                    throws Exception
    {
        givenMaxRunningJobs(1);
        givenReceptionOfNewRequest();
        afterQueueSweep();
        assertThatSubmitRequestHasBeenCalled();
        assertThatRunningRequestsEquals(1);
        assertThatRequestStatusIs(Status.STARTED);
    }

    @Test
    public void shouldNotCallSubmitRequestWithMaxQueueSizeOnNewRequest()
                    throws Exception
    {
        givenMaxRunningJobs(1);
        givenRunningRequest();
        givenReceptionOfNewRequest();
        givenRunningJobsEquals(1);
        givenQueuedJobsEquals(2);
        afterQueueSweep();
        assertThatSubmitRequestHasNotBeenCalled();
        assertThatRequestStatusIs(Status.QUEUED);
    }

    @Test
    public void shouldStartRequestJobWhenSpaceAvailable()
                    throws Exception
    {
        givenMaxRunningJobs(1);
        givenReceptionOfNewRequest();
        givenSubmissionOfNewRequestJob();
        afterQueueSweep();
        assertThatJobIsOnRunningQueue(requestJob);
        assertThatJobIsOnSubmittedQueue(request.getId());
        assertThatRunningJobsEquals(1);
        /*
         *  not STARTED because the executor is a NOP/mocked
         */
        assertThatJobStateIs(requestJob, State.INITIALIZED);
    }

    @Test
    public void shouldPutRequestJobOnReadyQueueWhenNoSpaceAvailable()
        throws Exception
    {
        givenMaxRunningJobs(1);
        givenReceptionOfNewRequest();
        givenSubmissionOfNewRequestJob();
        givenRunningJobsEquals(1);
        afterQueueSweep();
        assertThatJobIsOnReadyQueue(request.getId());
        assertThatJobIsOnSubmittedQueue(request.getId());
        assertThatJobStateIs(requestJob, State.CREATED);
    }

    @Test
    public void shouldPromoteRequestJobFromReadyQueueWhenAvailable()
                    throws Exception
    {
        givenMaxRunningJobs(1);
        givenReceptionOfNewRequest();
        givenSubmissionOfNewRequestJob();
        givenRunningJobsEquals(1);
        afterQueueSweep();
        whenRunningJobsHaveCompleted();
        afterQueueSweep();
        assertThatJobIsOnRunningQueue(requestJob);
        assertThatJobIsOnSubmittedQueue(request.getId());
        assertThatRunningJobsEquals(1);
        /*
         *  not STARTED because the executor is a NOP/mocked
         */
        assertThatJobStateIs(requestJob, State.INITIALIZED);
    }

    @Test
    public void shouldRemoveRequestJobWhenCompleted() throws Exception
    {
        givenReceptionOfNewRequest();
        givenSubmissionOfNewRequestJob();
        afterQueueSweep();
        whenRunningRequestJobCompletes();
        afterQueueSweep();
        assertThatRequestTargetCompletedHasNotBeenCalled();
        assertThatJobIsNotOnSubmittedQueue(request.getId());
        assertThatRunningRequestsEquals(1);
    }

    @Test
    public void shouldProcessRequestWhenCompleted() throws Exception
    {
        givenReceptionOfNewRequest();
        givenSubmissionOfNewRequestJob();
        afterQueueSweep();
        whenRunningRequestJobCompletes();
        whenRequestCompletes();
        afterQueueSweep();
        afterRequestCompletedHasBeenCalled();
        assertThatRunningRequestsEquals(0);
    }

    @Test
    public void shouldCallTargetCompletedWhenRunningJobsComplete() throws Exception

    {
        givenMaxRunningJobs(3);
        givenReceptionOfNewRequest();
        givenSubmissionOfJobs(3);
        afterQueueSweep();
        assertThatSubmittedQueueIsEmpty();
        assertThatRunningJobsEquals(3);
        whenRunningJobsHaveCompleted();
        afterQueueSweep();
        assertThatRequestTargetCompletedHasBeenCalled(3);
        assertThatRunningJobsEquals(0);
    }

    @Test
    public void shouldProcessReadyJobsAsRunningJobsComplete() throws Exception

    {
        givenMaxRunningJobs(3);
        givenReceptionOfNewRequest();
        givenSubmissionOfJobs(10);
        afterQueueSweep();
        assertThatSubmittedQueueIsEmpty();
        assertThatRunningJobsEquals(3);
        assertThatReadyQueueSizeIs(7);
        whenRunningJobsHaveCompleted();
        afterQueueSweep();
        assertThatRunningJobsEquals(3);
        assertThatReadyQueueSizeIs(4);
        whenRunningJobsHaveCompleted();
        afterQueueSweep();
        assertThatRunningJobsEquals(3);
        assertThatReadyQueueSizeIs(1);
        whenRunningJobsHaveCompleted();
        afterQueueSweep();
        assertThatRunningJobsEquals(1);
        assertThatReadyQueueSizeIs(0);
        whenRunningJobsHaveCompleted();
        afterQueueSweep();
        assertThatRunningJobsEquals(0);
        assertThatReadyQueueSizeIs(0);
    }

    private void afterQueueSweep() throws Exception
    {
        queue.jobProcessor.doRun();
        queue.postProcessor.doRun(true);
        queue.sweeper.doRun();
    }

    private void afterRequestCompletedHasBeenCalled()
                    throws Exception
    {
        /*
         *  Because the handler is a mock, we need to do its work
         *  here so that we can test the active running count.
         */
        store.update(request.getId(), Status.COMPLETED);
    }

    private void assertThatJobIsOnReadyQueue(String id)
    {
        assertNotNull(queue.readyQueue.get(id));
    }

    private void assertThatJobIsOnRunningQueue(BulkJob job)
    {
        assertTrue(queue.runningQueue.contains(job));
    }

    private void assertThatJobIsNotOnSubmittedQueue(String id)
    {
        assertNull(queue.submitted.get(id));
    }

    private void assertThatJobIsOnSubmittedQueue(String id)
    {
        assertNotNull(queue.submitted.get(id));
    }

    private void assertThatJobStateIs(BulkJob job, BulkJob.State state)
    {
        assertEquals(state, job.getState());
    }

    private void assertThatReadyQueueSizeIs(int size)
    {
        assertEquals(size, queue.readyQueue.size());
    }

    private void assertThatRequestStatusIs(BulkRequestStatus.Status status)
                    throws Exception
    {
        assertEquals(store.getStatus(request.getId()).get().getStatus(),
                     status);
    }

    private void assertThatRequestTargetCompletedHasBeenCalled(int times)
                    throws Exception
    {
        verify(completionHandler, times(times)).requestTargetCompleted(any(BulkJob.class));
    }

    private void assertThatRequestTargetCompletedHasNotBeenCalled()
                    throws Exception
    {
        verify(completionHandler, never()).requestTargetCompleted(any(BulkJob.class));
    }

    private void assertThatRunningJobsEquals(int running)
                    throws Exception
    {
        assertEquals(running, queue.runningQueue.size());
    }

    private void assertThatRunningRequestsEquals(int running)
                    throws Exception
    {
        assertEquals(running, queue.activeRequests());
    }

    private void assertThatSubmitRequestHasBeenCalled() throws Exception
    {
        verify(submissionHandler).submitRequest(any(BulkRequest.class));
    }

    private void assertThatSubmitRequestHasNotBeenCalled() throws Exception
    {
        verify(submissionHandler, never()).submitRequest(any(BulkRequest.class));
    }

    private void  assertThatSubmittedQueueIsEmpty() throws Exception
    {
        assertTrue(queue.submitted.isEmpty());
    }

    private void givenMaxRunningJobs(int maxjobs)
    {
        queue.setMaxRunningJobs(maxjobs);
    }

    private void givenReceptionOfNewRequest() throws Exception
    {
        request = new BulkRequest();
        request.setExpandDirectories(Depth.NONE);
        request.setActivity("test");
        request.setTarget("/");
        request.setId(UUID.randomUUID().toString());
        request.setClearOnSuccess(true);
        store.store(Subjects.ROOT, Restrictions.none(), request, null);
    }

    private void givenRunningJobsEquals(int running) throws Exception
    {
        for (int j = 0; j < running; ++j) {
            queue.runningQueue.add(new PlaceholderJob());
        }
    }

    private void givenQueuedJobsEquals(int running) throws Exception
    {
        for (int j = 0; j < running; ++j) {
            queue.readyQueue.put(request.getId(), new PlaceholderJob());
            queue.waitingQueue.add(new PlaceholderJob());
        }
    }

    private void givenRunningRequest() throws Exception
    {
        BulkRequest previous = new BulkRequest();
        previous.setExpandDirectories(Depth.NONE);
        previous.setActivity("test");
        previous.setTarget("/");
        previous.setId(UUID.randomUUID().toString());
        previous.setClearOnSuccess(true);
        BulkRequestStatus status = new BulkRequestStatus();
        status.setStatus(Status.QUEUED);
        store.store(Subjects.ROOT, Restrictions.none(), previous, status);
        store.update(previous.getId(), Status.STARTED);
    }

    private void givenSubmissionOfJobs(int number) throws Exception
    {
        for (int j = 0; j < number; ++j) {
            queue.submit(new PlaceholderJob());
        }
    }

    private void givenSubmissionOfNewRequestJob() throws Exception
    {
        requestJob = new BulkRequestJob(BulkJobKey.newKey(request.getId()),
                                        request,
                                        TargetType.FILE);
        requestJob.setCompletionHandler(jobCompletionHandler);
        requestJob.setNamespaceHandler(mock(PnfsHandler.class));
        queue.submit(requestJob);
    }

    private void whenRequestCompletes() throws Exception
    {
        when(jobCompletionHandler.isRequestCompleted()).thenReturn(true);
    }

    private void whenRunningJobsHaveCompleted()
    {
        queue.runningQueue.stream().forEach(j -> j.setState(State.COMPLETED));
    }

    private void whenRunningRequestJobCompletes()
    {
        requestJob.setState(State.COMPLETED);
    }
}
