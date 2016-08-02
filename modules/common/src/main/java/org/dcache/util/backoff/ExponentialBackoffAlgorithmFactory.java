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
package org.dcache.util.backoff;

import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Factory implementation for creating an {@link ExponentialBackoffAlgorithm}.
 *
 * @author arossi
 */
public class ExponentialBackoffAlgorithmFactory implements
                IBackoffAlgorithmFactory {
    /**
     * Standard exponential backoff algorithm. Computes recursive function: <br>
     * <br>
     *
     * W(0) = W[min] <br>
     * W(n) = (W[min] + W(n-1) * 2)/2 <br>
     * <br>
     *
     * where W[min] is the minimum delay time, and n is the attempt number. The
     * algorithm can also be given a maximum delay (default is Long.MAX_VALUE).
     * The minimum delay must be positive; the default is 1.
     *
     * <p>
     * The default {@link TimeUnit} for both min and max is MINUTES.
     *
     * <p>
     * {@link #quitAtMaxDelay} is false by default.
     *
     * <p>
     * Note that the algorithm is stateful and therefore not thread-safe; hence
     * the factory creates a new copy at each call to
     * {@link ExponentialBackoffAlgorithmFactory#getAlgorithm()}.
     */
    private class ExponentialBackoffAlgorithm implements IBackoffAlgorithm {

        /**
         * Derived
         */
        private Long maxDelayInMillis;
        private Long minDelayInMillis;

        /**
         * State
         */
        private long previousDelay;
        private int previousAttempts;

        @Override
        public long getWaitDuration() {
            if (previousDelay == NO_WAIT) {
                return previousDelay;
            }

            if (previousDelay == 0) {
                previousDelay = minDelayInMillis;
            } else {
                previousDelay = (minDelayInMillis + 2 * previousDelay) / 2;
            }

            if (maxDelayInMillis != null) {
                /*
                 * catch potential overflow (negative value)
                 */
                if (previousDelay > maxDelayInMillis || previousDelay <= 0) {
                    previousDelay = maxDelayInMillis;
                }

                if (quitAtMaxDelay && previousDelay == maxDelayInMillis) {
                    previousDelay = NO_WAIT;
                }
            }

            if (maxAllowedAttempts != null
                            && ++previousAttempts >= maxAllowedAttempts) {
                previousDelay = NO_WAIT;
            }

            return previousDelay;
        }
    }

    /**
     * Injected
     */
    private Long maxDelay;
    private Long minDelay = 1L;
    private TimeUnit maxUnit = TimeUnit.MINUTES;
    private TimeUnit minUnit = TimeUnit.MINUTES;
    private Integer maxAllowedAttempts;
    private boolean quitAtMaxDelay;

    @Override
    public IBackoffAlgorithm getAlgorithm() {
        ExponentialBackoffAlgorithm algorithm = new ExponentialBackoffAlgorithm();
        algorithm.minDelayInMillis = minUnit.toMillis(minDelay);
        if (maxDelay != null) {
            algorithm.maxDelayInMillis = maxUnit.toMillis(maxDelay);
            checkArgument(algorithm.maxDelayInMillis  >= algorithm.minDelayInMillis);
        }

        return algorithm;
    }

    public void setMaxAllowedAttempts(Integer maxAllowedAttempts) {
        this.maxAllowedAttempts = maxAllowedAttempts;
    }

    public void setMaxDelay(Long maxDelay) {
        this.maxDelay = maxDelay;
    }

    public void setMaxUnit(TimeUnit maxUnit) {
        checkNotNull(maxUnit);
        this.maxUnit = maxUnit;
    }

    public void setMinDelay(Long minDelay) {
        checkNotNull(minDelay);
        checkArgument(minDelay > 0);
        this.minDelay = minDelay;
    }

    public void setMinUnit(TimeUnit minUnit) {
        checkNotNull(minUnit);
        this.minUnit = minUnit;
    }

    public void setQuitAtMaxDelay(boolean quitAtMaxDelay) {
        this.quitAtMaxDelay = quitAtMaxDelay;
    }
}
