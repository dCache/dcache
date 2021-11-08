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
package org.dcache.qos.services.scanner.data;

import dmg.cells.nucleus.CellInfoProvider;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.dcache.util.RunnableModule;

/**
 * Provides common run method for the maps and several other common methods and fields.
 */
abstract class ScanOperationMap extends RunnableModule implements CellInfoProvider {

    protected final Lock lock = new ReentrantLock();
    protected final Condition condition = lock.newCondition();

    protected volatile boolean resetInterrupt = false;
    protected volatile boolean runInterrupt = false;
    protected volatile int maxConcurrentRunning = 4;

    public void reset() {
        resetInterrupt = true;
        threadInterrupt();
    }

    @Override
    public void run() {
        while (!Thread.interrupted()) {
            long start = System.currentTimeMillis();
            lock.lock();
            try {
                condition.await(timeout, timeoutUnit);
            } catch (InterruptedException e) {
                if (resetInterrupt) {
                    LOGGER.trace("reset, returning to wait: timeout {} {}.", timeout, timeoutUnit);
                    resetInterrupt = false;
                    continue;
                }
                if (!runInterrupt) {
                    LOGGER.trace("wait was interrupted; exiting.");
                    break;
                }
                runInterrupt = false;
            } finally {
                lock.unlock();
            }

            if (Thread.interrupted()) {
                break;
            }

            LOGGER.trace("Pool watchdog initiating scan.");
            runScans();
            LOGGER.trace("Pool watchdog scan completed.");

            recordSweep(start, System.currentTimeMillis());
        }

        LOGGER.info("Exiting pool operation consumer.");
        clear();

        LOGGER.info("Pool operation queues cleared.");
    }

    public void runNow() {
        runInterrupt = true;
        threadInterrupt();
    }

    public int getMaxConcurrentRunning() {
        return maxConcurrentRunning;
    }

    public void setMaxConcurrentRunning(int maxConcurrentRunning) {
        this.maxConcurrentRunning = maxConcurrentRunning;
    }

    public abstract String configSettings();

    public abstract void runScans();

    protected abstract void clear();

    protected abstract void recordSweep(long start, long end);
}
