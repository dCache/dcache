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
package org.dcache.restful.services.admin;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellInfoProvider;
import dmg.cells.nucleus.CellLifeCycleAware;
import dmg.util.command.Option;

import org.dcache.restful.util.admin.CellMessagingCollector;

/**
 * <p>Services supporting restful admin resources should extend
 * this class and implement their specific service interface.</p>
 *
 * <p>Here is provided a common framework for initializing, resetting
 * and shutting down the thread which calls the collector(s) and
 * builds the cache(s).  This includes two admin shell commands
 * for checking current status and for resetting the timeout.</p>
 */
public abstract class CellDataCollectingService<D, C extends CellMessagingCollector<D>>
                implements Runnable, CellCommandListener, CellInfoProvider, CellLifeCycleAware
{
    protected static final Logger LOGGER
                    = LoggerFactory.getLogger(CellDataCollectingService.class);

    /**
     * <p>Should be applied to all admin commands with time range parameters.</p>
     */
    protected static final String DATETIME_FORMAT = "yyyy/MM/dd-HH:mm:ss";

    /**
     * <p>This is to provide uniform date-time formatting for all services.</p>
     *
     * @param datetime in the DATETIME_FORMAT above.
     * @return corresponding date object.
     * @throws ParseException if the string does not conform to the format.
     */
    protected static Date getDate(String datetime) throws ParseException {
        if (datetime == null) {
            return null;
        }

        DateFormat format = new SimpleDateFormat(DATETIME_FORMAT);
        return format.parse(datetime);
    }

    /**
     * <p>This class should be subclassed in order to provide the
     * correct command annotation and to distinguish it from
     * other similar commands in the same domain.</p>
     *
     * <p>Interrupts thread to run update immediately.</p>
     */
    protected abstract class RefreshCommand implements Callable<String> {
        @Override
        public String call() {
            CellDataCollectingService.this.interrupt();
            CellDataCollectingService.this.scheduleNext(0, timeoutUnit);
            return "Update started.";
        }
    }

    /**
     * <p>This class should be subclassed in order to provide the
     * correct command annotation and to distinguish it from
     * other similar commands in the same domain.</p>
     */
    protected abstract class SetTimeoutCommand implements Callable<String> {
        @Option(name = "timeout",
                        usage = "Length of timeout interval; must be >= 10 seconds.")
        Long timeout;

        @Option(name = "unit",
                        valueSpec = "SECONDS|MINUTES|HOURS",
                        usage = "Timeout interval unit (default is SECONDS).")
        TimeUnit unit = TimeUnit.SECONDS;

        @Override
        public String call() {
            String response = "";

            synchronized (CellDataCollectingService.this) {
                if (timeout != null) {
                    Preconditions.checkArgument(unit.toSeconds(timeout) >= 10L,
                                                "Update time must "
                                                                + "exceed 10 seconds");

                    CellDataCollectingService.this.timeout = timeout;
                    CellDataCollectingService.this.timeoutUnit = unit;
                    configure();

                    /*
                     * Time the collector communication out at half the
                     * refresh interval.
                     */
                    collector.reset(timeout / 2, timeoutUnit);
                    response = "Update time set to "
                                    + unit.toSeconds(timeout) + " seconds";
                }
            }
            return response;
        }
    }

    protected C collector;

    protected long     timeout     = 30;
    protected TimeUnit timeoutUnit = TimeUnit.SECONDS;

    private ScheduledExecutorService executorService;
    private ScheduledFuture nextCollection;

    /**
     * <p>For diagnostic information.</p>
     */
    private long timeUsed;
    private long processCounter;

    public synchronized void getInfo(PrintWriter pw) {
        pw.print("    Update Interval : ");
        pw.print(timeoutUnit.toSeconds(timeout));
        pw.println(" seconds");
        pw.print("    Counter : ");
        pw.println(processCounter);
        pw.print("    Last Time Used : ");
        pw.print(timeUsed);
        pw.println(" milliseconds");
    }

    @Override
    public void afterStart() {
        configure();
        collector.initialize(timeout, timeoutUnit);
        scheduleNext(0, timeoutUnit); // run immediately
    }

    /**
     * <p>Runs the collector and calls update with the returned data.</p>
     */
    @Override
    public void run() {
        try {
            long start = System.currentTimeMillis();
            update(collector.collectData());
            synchronized (this) {
                timeUsed = System.currentTimeMillis() - start;
                ++processCounter;
            }
        } catch (InterruptedException e) {
            LOGGER.trace(e.toString());
            /*
             * Do not reschedule here.
             */
            return;
        } catch (RuntimeException ee) {
            LOGGER.error(ee.toString(), ee);
        }

        scheduleNext(timeout, timeoutUnit);
    }

    public void setCollector(C collector) {
        this.collector = collector;
    }

    public void setExecutorService(ScheduledExecutorService service) {
        executorService = service;
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    public void setTimeoutUnit(TimeUnit timeoutUnit) {
        this.timeoutUnit = timeoutUnit;
    }

    @Override
    public void beforeStop() {
        collector.shutdown();
    }

    /**
     * <p>The implementation may need to reconfigure data structures if
     *    timing settings are changed.</p>
     */
    @GuardedBy("this")
    protected void configure() {
        // NOP
    }

    /**
     * <p>Updating needs to be protected by the same barrier as configure.</p>
     */
    @GuardedBy("this")
    protected abstract void update(D data);

    /**
     * <p>Waits for the cancellation.</p>
     */
    private void interrupt() {
        if (nextCollection != null) {
            nextCollection.cancel(true);
            try {
                nextCollection.get();
            } catch (CancellationException e) {
                LOGGER.info("Interrupt successfully canceled collection.");
            } catch (InterruptedException e) {
                LOGGER.trace("Collection was interrupted.");
            } catch (ExecutionException e) {
                LOGGER.error("Interrupt of collector failed: {} : {}.",
                             e.getMessage(), e.getCause());
            }
        }
    }

    private void scheduleNext(long delay, TimeUnit unit) {
        nextCollection = executorService.schedule(this,
                                                  delay,
                                                  unit);
    }
}
