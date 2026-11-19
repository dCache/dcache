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

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Non-blocking admission control for bulk tasks.
 * <p>
 * Tasks are queued and launched only when a permit is available.  Submission never blocks, so a
 * task holding a permit may safely submit further tasks (e.g., a directory listing discovering
 * subdirectories) without risk of all permits being held by tasks waiting for permits.
 * <p>
 * A launcher is invoked holding a permit, which it owns from that point on; it must eventually
 * call {@link #release()} exactly once.  If the launcher throws instead, the permit is returned
 * by the throttle and the launcher must not release it.  The throttle may be shared among
 * several requests.
 */
public final class BulkTaskThrottle {

    private static final Logger LOGGER = LoggerFactory.getLogger(BulkTaskThrottle.class);

    private final Semaphore permits;
    private final Queue<Runnable> pending = new ConcurrentLinkedQueue<>();

    /*
     *  Per-thread re-entrancy guard; it does NOT exclude other threads from draining
     *  concurrently (concurrent drains coordinate solely through the semaphore and the
     *  concurrent queue).
     *
     *  A launcher runs inside drain() and may call release() or submit() in the same
     *  thread (e.g., its request was cancelled while queued).  Rather than recurring once
     *  per pending task, the nested call returns, and the loop of the enclosing drain() in
     *  this same thread picks up the change on its next iteration.
     */
    private final ThreadLocal<Boolean> drainingInThisThread
          = ThreadLocal.withInitial(() -> false);

    public BulkTaskThrottle(int permits) {
        this.permits = new Semaphore(permits);
    }

    public int availablePermits() {
        return permits.availablePermits();
    }

    public int pendingCount() {
        return pending.size();
    }

    /**
     * Queue the launcher; it is run (possibly in the calling thread) as soon as a permit is
     * available.  Never blocks waiting for a permit.
     */
    public void submit(Runnable launcher) {
        pending.add(launcher);
        drain();
    }

    /**
     * Return a permit acquired on behalf of a launcher, and launch pending tasks if any.
     */
    public void release() {
        permits.release();
        drain();
    }

    /*
     *  Both submit and release publish their change (enqueue / release) before checking
     *  for the other condition, so a pending launcher cannot be stranded while a permit
     *  is available.
     */
    private void drain() {
        if (drainingInThisThread.get()) {
            return;
        }

        drainingInThisThread.set(true);
        try {
            while (!pending.isEmpty() && permits.tryAcquire()) {
                Runnable launcher = pending.poll();
                if (launcher == null) {
                    permits.release();
                    continue;
                }

                try {
                    launcher.run();
                } catch (RuntimeException e) {
                    LOGGER.error("Bulk task launcher failed; please report to support@dcache.org",
                          e);
                    permits.release();
                }
            }
        } finally {
            drainingInThisThread.set(false);
        }
    }
}
