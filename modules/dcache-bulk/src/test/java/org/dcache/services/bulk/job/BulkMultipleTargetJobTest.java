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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.PnfsMessage;

import org.dcache.auth.attributes.Restrictions;
import org.dcache.cells.CellStub;
import org.dcache.chimera.InodeId;
import org.dcache.namespace.FileAttribute;
import org.dcache.namespace.FileType;
import org.dcache.services.bulk.BulkRequest;
import org.dcache.services.bulk.BulkRequest.Depth;
import org.dcache.services.bulk.handlers.BulkJobCompletionHandler;
import org.dcache.services.bulk.handlers.BulkSubmissionHandler;
import org.dcache.services.bulk.job.MultipleTargetJob.TargetType;
import org.dcache.services.bulk.job.TargetExpansionJob.ExpansionType;
import org.dcache.services.bulk.queue.SignalAware;
import org.dcache.util.list.DirectoryEntry;
import org.dcache.util.list.ListDirectoryHandler;
import org.dcache.vehicles.FileAttributes;
import org.dcache.vehicles.PnfsListDirectoryMessage;

import static org.dcache.namespace.FileType.DIR;
import static org.dcache.namespace.FileType.REGULAR;
import static org.dcache.services.bulk.BulkRequest.Depth.ALL;
import static org.dcache.services.bulk.BulkRequest.Depth.TARGETS;
import static org.dcache.services.bulk.job.MultipleTargetJob.TargetType.BOTH;
import static org.dcache.services.bulk.job.MultipleTargetJob.TargetType.FILE;
import static org.dcache.services.bulk.job.TargetExpansionJob.ExpansionType.BREADTH_FIRST;
import static org.dcache.services.bulk.job.TargetExpansionJob.ExpansionType.DEPTH_FIRST;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class BulkMultipleTargetJobTest implements SignalAware
{
    private static PnfsId nextPnfsId()
    {
        return new PnfsId(InodeId.newID(0));
    }

    class TestPnfsHandler extends PnfsHandler
    {
        public TestPnfsHandler()
        {
            super((CellStub) null);
        }

        public long getPnfsTimeout()
        {
            return 0L;
        }

        public void send(PnfsMessage msg)
        {
            if (msg instanceof PnfsListDirectoryMessage) {
                PnfsListDirectoryMessage listMessage
                                = (PnfsListDirectoryMessage) msg;
                fileTree.get(listMessage.getFsPath().name())
                        .stream()
                        .forEach(de -> listMessage.addEntry(de.getName(),
                                                            de.getFileAttributes()));
                listMessage.setSucceeded(1);
                listMessage.setReply();
                listDirectoryHandler.messageArrived(listMessage);
            }
        }

        public FileAttributes getFileAttributes(String path, Set<FileAttribute> attr)
        {
            return attributesMap.get(new File(path).getName());
        }
    }

    Multimap<String, DirectoryEntry> fileTree;
    Map<String, FileAttributes>      attributesMap;
    TestPnfsHandler                  pnfsHandler;
    ListDirectoryHandler             listDirectoryHandler;
    BulkSubmissionHandler            submissionHandler;
    BulkJobCompletionHandler         completionHandler;

    BulkRequest        request;
    BulkJobKey         requestKey;
    TargetExpansionJob targetExpansionJob;
    BulkRequestJob     requestJob;

    @Test
    public void requestJobShouldSubmitOneExpansionJob()
                    throws Exception
    {
        givenRequestWithTargetAndDepth("pnfs", ALL);
        givenRequestJobOfType(FILE);
        whenRequestJobRuns();
        assertThatExpansionSubmitWasCalled(1);
    }

    @Test
    public void requestJobAllShouldSubmitTwoExpansionJobs()
                    throws Exception
    {
        givenRequestWithTargetAndDepth("['pnfs', 'pnfs/fs']", ALL);
        givenRequestJobOfType(FILE);
        whenRequestJobRuns();
        assertThatExpansionSubmitWasCalled(2);
    }

    @Test
    public void requestJobShouldSubmitSixSingleTargetJobs()
                    throws Exception
    {
        givenRequestWithTargetAndDepth("['pnfs/fs/scratch/scratch-file-1', "
                                                       + "'pnfs/fs/scratch/scratch-file-2',"
                                                       + "'pnfs/fs/scratch/scratch-file-3',"
                                                       + "'pnfs/fs/scratch/scratch-file-4',"
                                                       + "'pnfs/fs/scratch/scratch-file-5',"
                                                       + "'pnfs/fs/scratch/scratch-file-6']",
                                       Depth.NONE);
        givenRequestJobOfType(FILE);
        whenRequestJobRuns();
        assertThatSingleTargetSubmitWasCalled(6);
    }

    @Test
    public void requestJobTargetsShouldSubmitTwoExpansionJobs()
                    throws Exception
    {
        /*
         * The request job is like the expansion job in that it defers
         * treating directories as actual targets in the directory is also
         * to be expanded.
         */
        givenRequestWithTargetAndDepth("['pnfs', 'pnfs/fs']", Depth.TARGETS);
        givenRequestJobOfType(TargetType.DIR);
        whenRequestJobRuns();
        assertThatExpansionSubmitWasCalled(2);
        assertThatSingleTargetSubmitWasCalled(0);
    }

    @Test
    public void requestJobShouldSubmitTwoSingleTargetJobs()
                    throws Exception
    {
        givenRequestWithTargetAndDepth("['pnfs', 'pnfs/fs']", Depth.NONE);
        givenRequestJobOfType(TargetType.DIR);
        whenRequestJobRuns();
        assertThatSingleTargetSubmitWasCalled(2);
    }

    @Test
    public void breadthFirstExpansionShouldSubmitTargetExpansionJobOnce()
                    throws Exception
    {
        givenRequestWithTargetAndDepth("pnfs", ALL);
        givenExpansionWith("pnfs", BREADTH_FIRST, FILE);
        whenExpansionJobRuns();
        assertThatExpansionSubmitWasCalled(1);
    }

    @Test
    public void depthFirstExpansionShouldSubmitTargetExpansionJobOnce()
                    throws Exception
    {
        givenRequestWithTargetAndDepth("pnfs", ALL);
        givenExpansionWith("pnfs", DEPTH_FIRST, FILE);
        whenExpansionJobRuns();
        /*
         *  Depth-first does not submit new directory expansion tasks,
         *  but does them recursively.
         */
        assertThatExpansionSubmitWasCalled(0);
    }

    @Test
    public void breadthFirstExpansionShouldSubmitNoExpansionJob()
                    throws Exception
    {
        givenRequestWithTargetAndDepth("pnfs", TARGETS);
        givenExpansionWith("pnfs", BREADTH_FIRST, FILE);
        whenExpansionJobRuns();
        assertThatExpansionSubmitWasCalled( 0);
    }

    @Test
    public void depthFirstExpansionShouldSubmitNoExpansionJob()
                    throws Exception
    {
        givenRequestWithTargetAndDepth("pnfs", TARGETS);
        givenExpansionWith("pnfs", DEPTH_FIRST, FILE);
        whenExpansionJobRuns();
        assertThatExpansionSubmitWasCalled(0);
    }

    @Test
    public void breadthFirstExpansionShouldSubmitTargetExpansionJobTwice()
                    throws Exception
    {
        givenRequestWithTargetAndDepth("pnfs", ALL);
        givenExpansionWith("test", BREADTH_FIRST, FILE);
        whenExpansionJobRuns();
        assertThatExpansionSubmitWasCalled(2);
    }

    @Test
    public void breadthFirstExpansionShouldSubmitSingleTargetJobTwice()
                    throws Exception
    {
        givenRequestWithTargetAndDepth("pnfs", TARGETS);
        givenExpansionWith("test-child-2", BREADTH_FIRST, FILE);
        whenExpansionJobRuns();
        assertThatSingleTargetSubmitWasCalled(2);
    }

    @Test
    public void depthFirstExpansionShouldSubmitSingleTargetJobTwice()
                    throws Exception
    {
        givenRequestWithTargetAndDepth("pnfs", TARGETS);
        givenExpansionWith("test-child-2", DEPTH_FIRST, FILE);
        whenExpansionJobRuns();
        assertThatSingleTargetSubmitWasCalled(2);
    }

    @Test
    public void breadthFirstExpansionShouldSubmitSelfAndChildAsSingleTargetJob()
                    throws Exception
    {
        givenRequestWithTargetAndDepth("pnfs", TARGETS);
        givenExpansionWith("test-child-2", BREADTH_FIRST, TargetType.DIR);
        whenExpansionJobRuns();
        assertThatSingleTargetSubmitWasCalled(2);
    }

    @Test
    public void breadthFirstExpansionShouldSubmitOnlySelfAsSingleTargetJob()
                    throws Exception
    {
        givenRequestWithTargetAndDepth("pnfs", TARGETS);
        givenExpansionWith("scratch", BREADTH_FIRST, TargetType.DIR);
        whenExpansionJobRuns();
        assertThatSingleTargetSubmitWasCalled(1);
    }

    @Test
    public void depthFirstExpansionShouldSubmitSelfAndChildAsSingleTargetJob()
                    throws Exception
    {
        givenRequestWithTargetAndDepth("pnfs", TARGETS);
        givenExpansionWith("test-child-2", DEPTH_FIRST, TargetType.DIR);
        whenExpansionJobRuns();
        assertThatSingleTargetSubmitWasCalled(2);
    }

    @Test
    public void depthFirstExpansionShouldSubmitOnlySelfAsSingleTargetJob()
                    throws Exception
    {
        givenRequestWithTargetAndDepth("pnfs", TARGETS);
        givenExpansionWith("scratch", DEPTH_FIRST, TargetType.DIR);
        whenExpansionJobRuns();
        assertThatSingleTargetSubmitWasCalled(1);
    }

    @Test
    public void breadthFirstExpansionShouldSubmitFourSingleTargetJobs()
                    throws Exception
    {
        givenRequestWithTargetAndDepth("pnfs", TARGETS);
        givenExpansionWith("test-child-2", BREADTH_FIRST, BOTH);
        whenExpansionJobRuns();
        assertThatSingleTargetSubmitWasCalled(4);
    }

    @Test
    public void depthFirstExpansionShouldSubmitFourSingleTargetJobs()
                    throws Exception
    {
        givenRequestWithTargetAndDepth("pnfs", TARGETS);
        givenExpansionWith("test-child-2", DEPTH_FIRST, BOTH);
        whenExpansionJobRuns();
        assertThatSingleTargetSubmitWasCalled(4);
    }

    @Test
    public void depthFirstExpansionShouldSubmitThirteenSingleTargetJobs()
                    throws Exception
    {
        /*
         * Because the mocked submission handler does not add children
         * to the completion handler, depth-first should walk the entire
         * tree.
         */
        givenRequestWithTargetAndDepth("pnfs", ALL);
        givenExpansionWith("pnfs", DEPTH_FIRST, TargetType.FILE);
        whenExpansionJobRuns();
        assertThatSingleTargetSubmitWasCalled(13);
    }

    @Test
    public void depthFirstExpansionShouldSubmitSevenSingleTargetJobs()
                    throws Exception
    {
        /*
         * Because the mocked submission handler does not add children
         * to the completion handler, depth-first should walk the entire
         * tree.
         */
        givenRequestWithTargetAndDepth("pnfs", ALL);
        givenExpansionWith("pnfs", DEPTH_FIRST, TargetType.DIR);
        whenExpansionJobRuns();
        assertThatSingleTargetSubmitWasCalled(7);
    }

    @Test
    public void breadthFirstExpansionShouldSubmitSelfAndFileSingleTargetJobs()
                    throws Exception
    {
        /*
         *  Because a directory is treated "up front" as a target in
         *  breadth-first, and this is deferred (done by the new
         *  expansion job) when expansion is "on", we should see here
         *  only the directory itself plus its file children submitted
         *  as single target jobs.
         */
        givenRequestWithTargetAndDepth("pnfs", ALL);
        givenExpansionWith("test", BREADTH_FIRST, BOTH);
        whenExpansionJobRuns();
        assertThatSingleTargetSubmitWasCalled(3);
    }

    @Before
    public void setup() throws Exception
    {
        fileTree = HashMultimap.create();
        attributesMap = new HashMap<>();
        loadEntries();
        pnfsHandler = new TestPnfsHandler();
        listDirectoryHandler = new ListDirectoryHandler(pnfsHandler);
        submissionHandler = mock(BulkSubmissionHandler.class);
        completionHandler = new BulkJobCompletionHandler(this);
    }

    @Override
    public void signal()
    {
        // NOP
    }

    @Override
    public int countSignals()
    {
        return 0;
    }

    private void assertThatExpansionSubmitWasCalled(int times)
                    throws Exception
    {
        verify(submissionHandler, times(times))
                        .submitTargetExpansionJob(any(String.class),
                                                  any(FileAttributes.class),
                                                  any(MultipleTargetJob.class));
    }

    private void assertThatSingleTargetSubmitWasCalled(int times)
        throws Exception
    {
        verify(submissionHandler, times(times))
                        .submitSingleTargetJob(any(String.class),
                                               any(BulkJobKey.class),
                                               any(),
                                               any(MultipleTargetJob.class));
    }

    private void add(String dir, String name, FileType type)
    {
        FileAttributes attr = new FileAttributes();
        attr.setFileType(type);
        attr.setPnfsId(nextPnfsId());
        DirectoryEntry entry = new DirectoryEntry(name, attr);
        attributesMap.put(name, attr);
        fileTree.put(dir, entry);
    }

    private void givenExpansionWith(String target,
                                    ExpansionType expansionType,
                                    TargetType targetType)
                    throws Exception
    {
        targetExpansionJob = new TargetExpansionJob(
                        BulkJobKey.newKey(requestKey.getRequestId()),
                        requestKey,
                        request,
                        targetType,
                        expansionType);
        targetExpansionJob.setTarget(target);
        targetExpansionJob.setRestriction(Restrictions.none());
        targetExpansionJob.setSubmissionHandler(submissionHandler);
        targetExpansionJob.setListHandler(listDirectoryHandler);
        targetExpansionJob.setCompletionHandler(completionHandler);
    }

    private void givenRequestJobOfType(TargetType targetType) throws Exception
    {
        requestJob = new BulkRequestJob(requestKey, request, targetType);
        requestJob.setNamespaceHandler(pnfsHandler);
        requestJob.setCompletionHandler(completionHandler);
        requestJob.setSubmissionHandler(submissionHandler);
        requestJob.setRestriction(Restrictions.none());
    }

    private void givenRequestWithTargetAndDepth(String target, Depth depth)
                    throws Exception
    {
        request = new BulkRequest();
        request.setExpandDirectories(depth);
        request.setTarget(target);
        request.setActivity("TEST");
        request.setUrlPrefix(null);
        request.setId(UUID.randomUUID().toString());
        requestKey = BulkJobKey.newKey(request.getId());
    }

    private void loadEntries()
    {
        FileAttributes attr = new FileAttributes();
        attr.setFileType(FileType.DIR);
        attr.setPnfsId(nextPnfsId());
        attributesMap.put("pnfs", attr);
        add("pnfs", "fs", DIR);
        add("fs", "test", DIR);
        add("fs", "scratch", DIR);
        add("test", "test-file-1", REGULAR);
        add("test", "test-file-2", REGULAR);
        add("test", "test-child-1", DIR);
        add("test", "test-child-2", DIR);
        add("scratch", "scratch-file-1", REGULAR);
        add("scratch", "scratch-file-2", REGULAR);
        add("scratch", "scratch-file-3", REGULAR);
        add("scratch", "scratch-file-4", REGULAR);
        add("scratch", "scratch-file-5", REGULAR);
        add("scratch", "scratch-file-6", REGULAR);
        add("test-child-1", "test-child-1-file-1", REGULAR);
        add("test-child-1", "test-child-1-file-2", REGULAR);
        add("test-child-1", "test-child-1-file-3", REGULAR);
        add("test-child-2", "test-child-2-file-1", REGULAR);
        add("test-child-2", "test-child-2-file-2", REGULAR);
        add("test-child-2", "test-child-2-empty", DIR);
    }

    private void whenExpansionJobRuns()
    {
        targetExpansionJob.run();
    }

    private void whenRequestJobRuns()
    {
        requestJob.run();
    }
}
