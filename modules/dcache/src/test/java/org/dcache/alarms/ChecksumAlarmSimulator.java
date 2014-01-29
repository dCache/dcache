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
package org.dcache.alarms;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.dcache.pool.classic.ChecksumScanner;
import org.dcache.util.Args;

/**
 * This class may be used for testing a live logging server configured with the
 * AlarmEntryAppender to store alarms.<br>
 * <br>
 * The command-line options set the number of separate threads to use
 * (-threads), the max timeout interval between iterations
 * (-maxTimeoutInSeconds), and the frequency with which to produce an error (per
 * iteration per thread: -errorFrequency). The timeout is randomized within 1
 * second and max for any given thread.<br>
 * <br>
 * Each simulator thread reports on one file checksum; this is so handling of
 * duplicate errors can be tested on the back-end. <br>
 * <br>
 * If run from this module, there is a logback-test.xml in src/text/resources
 * which will output the events to stdout and/or remote server.
 *
 * @author arossi
 */
public class ChecksumAlarmSimulator {
    private static class Simulator extends Thread {
        private final long pause = Math.max(1000,
                        Math.abs(RAND.nextLong() % timeoutModulo));
        private final int id;
        private final String pnfsId = Long.toHexString(RAND.nextLong() % 256);

        private Simulator(int id) {
            this.id = id;
        }

        @Override
        public void run() {
            while (true) {
                synchronized (this) {
                    try {
                        wait(pause);
                    } catch (InterruptedException ignored) {
                    }

                    switch (RAND.nextInt() % errorModulo) {
                        case 0:
                            /*
                             * Alarm events determined by the configuration
                             * of the logback appender using the AlarmDefinitionFilter
                             */
                            logger.error("simulator {}, Checksum mismatch detected for {} - marking as BROKEN",
                                            id, pnfsId);
                            break;
                        default:
                            logger.info("no errors for simulator {}", id);
                    }
                }
            }
        }
    }

    public final static String TIMEOUT = "maxTimeoutInSeconds";
    public final static String NUM_THRDS = "threads";
    public final static String FREQUENCY = "errorFrequency";

    private final static Logger logger
        = LoggerFactory.getLogger(ChecksumScanner.class);
    private final static Random RAND = new Random(System.currentTimeMillis());

    private final static int TIMEOUT_DEFAULT = 30;
    private final static double FREQUENCY_DEFAULT = 1.0;
    private final static int NTHREADS_DEFAULT = 1;

    private static int nThreads;
    private static int errorModulo;
    private static long timeoutModulo;

    public static void main(String[] args) throws Exception {
        Args options = new Args(args);

        setTimeoutModulo(options);
        setErrorModulo(options);
        setThreads(options);

        Simulator[] sims = new Simulator[nThreads];
        for (int i = 0; i < nThreads; i++) {
            sims[i] = new Simulator(i);
            sims[i].start();
        }

        for (int i = 0; i < nThreads; i++) {
            try {
                sims[i].join();
            } catch (InterruptedException e) {
            }
        }
    }

    private static void setTimeoutModulo(Args options) {
        timeoutModulo = TimeUnit.SECONDS.toMillis
                            (options.getIntOption(TIMEOUT, TIMEOUT_DEFAULT));
        timeoutModulo = Math.max(1000L, timeoutModulo);
        logger.info("timeoutModulo: % {} (milliseconds)", timeoutModulo);
    }

    private static void setErrorModulo(Args options) {
        Double frequency = options.getDoubleOption(FREQUENCY, FREQUENCY_DEFAULT);
        frequency = Math.abs(Math.min(1.0, frequency));
        errorModulo = (int) Math.ceil(1.0 / frequency);
        logger.info("errorModulo: one out of {}", errorModulo);
    }

    private static void setThreads(Args options) {
        nThreads = options.getIntOption(NUM_THRDS, NTHREADS_DEFAULT);
        nThreads = Math.max(nThreads, NTHREADS_DEFAULT);
        logger.info("number of simulator threads {}", nThreads);
    }
}
