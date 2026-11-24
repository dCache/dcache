/*
COPYRIGHT STATUS:
Dec 1st 2001, Fermi National Accelerator Laboratory (FNAL) documents and
software are sponsored by the U.S. Department of Energy under Contract No.
DE-AC02-76CH03000. Therefore, the U.S. Government retains a  world-wide
non-exclusive, royalty-free license to publish or reproduce these documents
and software for U.S. Government purposes.  All documents and software
available from this server are private under the U.S. and Foreign
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

import static org.dcache.services.bulk.util.BulkRequestTarget.PLACEHOLDER_PNFSID;
import static org.dcache.services.bulk.util.BulkRequestTarget.ROOT_REQUEST_PATH;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.dcache.namespace.FileType;
import org.dcache.services.bulk.BulkRequest;
import org.dcache.services.bulk.BulkRequest.Depth;
import org.dcache.services.bulk.activity.BulkActivity;
import org.dcache.services.bulk.store.BulkTargetStore;
import org.dcache.services.bulk.util.BulkRequestTarget;
import org.dcache.services.bulk.util.BulkRequestTarget.PID;
import org.dcache.services.bulk.util.BulkRequestTarget.State;
import org.dcache.services.bulk.util.BulkRequestTargetBuilder;
import org.dcache.services.bulk.util.BulkTaskThrottle;
import org.dcache.util.list.DirectoryEntry;
import org.dcache.util.list.DirectoryStream;
import org.dcache.util.list.ListDirectoryHandler;
import org.dcache.vehicles.FileAttributes;
import org.dcache.vehicles.PnfsGetFileAttributes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Recursive expansion of a directory tree wider than the number of directory listing permits
 * (GitHub issue #8062).
 */
public class BulkRequestContainerJobTest {

    private static final FsPath ROOT = FsPath.create("/test");
    private static final int FANOUT = 5;
    private static final int DEPTH = 4;
    private static final int LIST_PERMITS = 2;

    private final AtomicLong pnfsIds = new AtomicLong();
    private final Map<FsPath, Integer> performed = new ConcurrentHashMap<>();
    private final List<FsPath> performedDirs = Collections.synchronizedList(new ArrayList<>());

    private volatile long listDelayMs;

    private ExecutorService executor;
    private ExecutorService listExecutor;
    private ExecutorService callbackExecutor;
    private BulkTaskThrottle throttle;
    private BulkRequestTarget jobTarget;
    private BulkRequestContainerJob job;

    @Before
    public void setUp() throws Exception {
        executor = Executors.newCachedThreadPool();
        /*
         *  As in production, the list executor has as many threads as there are permits.
         */
        listExecutor = Executors.newFixedThreadPool(LIST_PERMITS);
        callbackExecutor = Executors.newCachedThreadPool();
        throttle = new BulkTaskThrottle(LIST_PERMITS);

        BulkRequest request = new BulkRequest();
        request.setId(1L);
        request.setUid("test-request");
        request.setActivity("TEST");
        request.setExpandDirectories(Depth.ALL);
        request.setTargetPrefix("/");

        BulkActivity<Object> activity = new BulkActivity<>("TEST",
              BulkActivity.TargetType.BOTH) {
            @Override
            public ListenableFuture<Object> perform(String rid, long tid, String prefix,
                  FsPath path, FileAttributes attributes) {
                performed.merge(path, 1, Integer::sum);
                if (attributes.getFileType() == FileType.DIR) {
                    performedDirs.add(path);
                }
                return Futures.immediateFuture(path);
            }

            @Override
            protected void configure(Map<String, String> arguments) {
            }

            @Override
            public void handleCompletion(BulkRequestTarget target, Future<Object> future) {
                target.setState(State.COMPLETED);
            }
        };

        FileAttributes placeholder = new FileAttributes();
        placeholder.setFileType(FileType.SPECIAL);
        placeholder.setPnfsId(PLACEHOLDER_PNFSID);
        jobTarget = BulkRequestTargetBuilder.builder(null).activity("TEST").rid(1L)
              .ruid("test-request").pid(PID.ROOT).attributes(placeholder)
              .path(ROOT_REQUEST_PATH).build();

        BulkRequestTarget initial = BulkRequestTargetBuilder.builder(null).activity("TEST")
              .id(2L).rid(1L).ruid("test-request").pid(PID.INITIAL).path(ROOT).build();

        BulkTargetStore targetStore = mock(BulkTargetStore.class);
        when(targetStore.getInitialTargets(1L, true)).thenReturn(List.of(initial));

        PnfsHandler pnfsHandler = mock(PnfsHandler.class);
        when(pnfsHandler.requestAsync(any())).thenAnswer(invocation -> {
            PnfsGetFileAttributes message = invocation.getArgument(0);
            message.setFileAttributes(attributes(FileType.DIR));
            return Futures.immediateFuture(message);
        });

        ListDirectoryHandler listHandler = mock(ListDirectoryHandler.class);
        when(listHandler.list(any(), any(), any(), any(), any(), any())).thenAnswer(
              invocation -> listing(invocation.getArgument(2)));

        job = new BulkRequestContainerJob(activity, jobTarget, request, null);
        job.setNamespaceHandler(pnfsHandler);
        job.setTargetStore(targetStore);
        job.setListHandler(listHandler);
        job.setDirListThrottle(throttle);
        job.setInFlightSemaphore(new Semaphore(100));
        job.setExecutor(executor);
        job.setListExecutor(listExecutor);
        job.setCallbackExecutor(callbackExecutor);
        job.initialize();
    }

    @After
    public void tearDown() {
        executor.shutdownNow();
        listExecutor.shutdownNow();
        callbackExecutor.shutdownNow();
    }

    @Test(timeout = 60_000)
    public void shouldCompleteRecursiveExpansionWiderThanListingPermits() throws Exception {
        executor.submit(job);

        while (jobTarget.getState() != State.COMPLETED) {
            assertTrue("job terminated in state " + jobTarget.getState(),
                  !jobTarget.isTerminated() || jobTarget.getState() == State.COMPLETED);
            TimeUnit.MILLISECONDS.sleep(20);
        }

        int dirs = 0;
        int files = 0;
        for (int level = 0, width = 1; level <= DEPTH; ++level, width *= FANOUT) {
            dirs += width;  /* directories at this level */
            files += width; /* each directory holds one file */
        }

        assertEquals(dirs + files, performed.size());
        assertTrue("some target performed more than once",
              performed.values().stream().allMatch(n -> n == 1));
        assertEquals(dirs, performedDirs.size());

        /*
         *  Directories must be processed deepest first (children before parents).
         */
        for (int i = 1; i < performedDirs.size(); ++i) {
            assertTrue(depth(performedDirs.get(i - 1)) >= depth(performedDirs.get(i)));
        }

        assertEquals(LIST_PERMITS, throttle.availablePermits());
        assertEquals(0, throttle.pendingCount());
    }

    @Test(timeout = 60_000)
    public void shouldReturnAllListingPermitsWhenCancelledDuringExpansion() throws Exception {
        listDelayMs = 5;
        executor.submit(job);

        while (performed.size() < 20) {
            TimeUnit.MILLISECONDS.sleep(5);
        }

        assertTrue("listings should be waiting for permits", throttle.pendingCount() > 0);

        job.cancel();

        assertEquals(State.CANCELLED, jobTarget.getState());
        while (throttle.availablePermits() != LIST_PERMITS || throttle.pendingCount() != 0) {
            TimeUnit.MILLISECONDS.sleep(10);
        }

        assertTrue("no directory should be processed after cancellation",
              performedDirs.isEmpty());
    }

    private FileAttributes attributes(FileType type) {
        FileAttributes attributes = new FileAttributes();
        attributes.setFileType(type);
        attributes.setPnfsId(new PnfsId(String.format("%036X", pnfsIds.incrementAndGet())));
        return attributes;
    }

    private static int depth(FsPath path) {
        return path.toString().split("/").length;
    }

    private DirectoryStream listing(FsPath path) throws InterruptedException {
        if (listDelayMs > 0) {
            TimeUnit.MILLISECONDS.sleep(listDelayMs);
        }

        List<DirectoryEntry> entries = new ArrayList<>();
        entries.add(new DirectoryEntry("file", attributes(FileType.REGULAR)));
        if (depth(path) - depth(ROOT) < DEPTH) {
            for (int i = 0; i < FANOUT; ++i) {
                entries.add(new DirectoryEntry("dir" + i, attributes(FileType.DIR)));
            }
        }

        return new DirectoryStream() {
            @Override
            public void close() {
            }

            @Override
            public Iterator<DirectoryEntry> iterator() {
                return entries.iterator();
            }
        };
    }
}
