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
package org.dcache.services.bulk.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class BulkTaskThrottleTest {

    private static final int PERMITS = 3;

    private BulkTaskThrottle throttle;
    private ExecutorService executor;
    private final AtomicInteger active = new AtomicInteger();
    private final AtomicInteger maxActive = new AtomicInteger();

    @Before
    public void setUp() {
        throttle = new BulkTaskThrottle(PERMITS);
        executor = Executors.newCachedThreadPool();
    }

    @After
    public void tearDown() {
        executor.shutdownNow();
    }

    @Test
    public void shouldRunLaunchersImmediatelyWhilePermitsAreAvailable() {
        AtomicInteger launched = new AtomicInteger();
        for (int i = 0; i < PERMITS; ++i) {
            throttle.submit(launched::incrementAndGet);
        }

        assertEquals(PERMITS, launched.get());
        assertEquals(0, throttle.availablePermits());
        assertEquals(0, throttle.pendingCount());
    }

    @Test
    public void shouldQueueWithoutBlockingWhenNoPermitIsAvailable() {
        AtomicInteger launched = new AtomicInteger();
        for (int i = 0; i < PERMITS + 2; ++i) {
            throttle.submit(launched::incrementAndGet);
        }

        assertEquals(PERMITS, launched.get());
        assertEquals(2, throttle.pendingCount());

        throttle.release();
        assertEquals(PERMITS + 1, launched.get());
        assertEquals(1, throttle.pendingCount());
    }

    @Test
    public void shouldReturnPermitIfLauncherFails() {
        throttle.submit(() -> {
            throw new IllegalStateException("test");
        });

        assertEquals(PERMITS, throttle.availablePermits());
    }

    @Test
    public void shouldNotRecurWhenLaunchersReleaseImmediately() {
        throttle = new BulkTaskThrottle(1);
        throttle.submit(() -> { /* hold the only permit */ });
        for (int i = 0; i < 100_000; ++i) {
            throttle.submit(throttle::release);
        }

        throttle.release();
        assertEquals(1, throttle.availablePermits());
        assertEquals(0, throttle.pendingCount());
    }

    /*
     *  Each task submits children while holding its permit, as directory listing does.
     *  A blocking acquisition would deadlock here.
     */
    @Test(timeout = 30_000)
    public void shouldCompleteTreeOfNestedSubmissions() throws Exception {
        int fanout = 4;
        int depth = 6;
        int total = 0;
        for (int level = 0, width = 1; level <= depth; ++level, width *= fanout) {
            total += width;
        }

        CountDownLatch done = new CountDownLatch(total);
        submitNode(0, depth, fanout, done);
        assertTrue(done.await(30, TimeUnit.SECONDS));

        while (throttle.availablePermits() != PERMITS) {
            TimeUnit.MILLISECONDS.sleep(1);
        }

        assertEquals(0, throttle.pendingCount());
        assertTrue("max concurrency " + maxActive.get(), maxActive.get() <= PERMITS);
    }

    private void submitNode(int level, int depth, int fanout, CountDownLatch done) {
        throttle.submit(() -> executor.execute(() -> {
            int now = active.incrementAndGet();
            maxActive.accumulateAndGet(now, Math::max);
            try {
                if (level < depth) {
                    for (int i = 0; i < fanout; ++i) {
                        submitNode(level + 1, depth, fanout, done);
                    }
                }
            } finally {
                active.decrementAndGet();
                done.countDown();
                throttle.release();
            }
        }));
    }
}
