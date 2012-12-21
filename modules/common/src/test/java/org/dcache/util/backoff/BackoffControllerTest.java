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

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.dcache.util.backoff.BackoffControllerBuilder.BackoffController;
import org.dcache.util.backoff.IBackoffAlgorithm.Status;
import org.junit.Test;

/**
 * Simple consistency tests for the {@link BackoffController}.
 *
 * @author arossi
 */
public class BackoffControllerTest {

    private class FailureSimulator implements
                    Callable<IBackoffAlgorithm.Status> {

        private int failFor;
        private int failed = 0;
        private int quitAfter = Integer.MAX_VALUE;

        @Override
        public Status call() throws Exception {
            delays.add(roundToNearestTenthOfASecond(System.currentTimeMillis()));

            if (delays.size() >= quitAfter) {
                return Status.SUCCESS;
            }

            if (failFor > failed) {
                failed++;
                return Status.FAILURE;
            }

            failed = 0;
            return Status.SUCCESS;
        }
    }

    private final FailureSimulator callable = new FailureSimulator();;
    private final ExponentialBackoffAlgorithmFactory factory
        = new ExponentialBackoffAlgorithmFactory();
    private final List<Long> delays = new ArrayList<Long>();

    @Test
    public void shouldNotRetry() throws Exception {
        givenFailureSimulatorWithFailFor(0);
        givenFailureSimulatorWithQuitAfter(1);
        givenAlgorithmWithMaxDelay(2);
        givenAlgorithmWithMaxUnit(TimeUnit.SECONDS);
        givenAlgorithmWithMinDelay(1);
        givenAlgorithmWithMinUnit(TimeUnit.SECONDS);
        givenAlgorithmWithQuitOnMaxDelay(false);
        whenBackoffControllerIsRun();
        assertThat(delays.size(), is(1));
    }

    @Test
    public void shouldRetryWithBackoff() throws Exception {
        givenFailureSimulatorWithFailFor(2);
        givenFailureSimulatorWithQuitAfter(3);
        givenAlgorithmWithMaxDelay(2);
        givenAlgorithmWithMaxUnit(TimeUnit.SECONDS);
        givenAlgorithmWithMinDelay(1);
        givenAlgorithmWithMinUnit(TimeUnit.SECONDS);
        givenAlgorithmWithQuitOnMaxDelay(false);
        whenBackoffControllerIsRun();
        assertThat(delays.size(), is(3));
        assertThat(delays.get(1) - delays.get(0), lessThan(delays.get(2)
                        - delays.get(1)));
    }

    @Test
    public void shouldRetryWithNoBackoff() throws Exception {
        givenFailureSimulatorWithFailFor(3);
        givenFailureSimulatorWithQuitAfter(3);
        givenAlgorithmWithMaxDelay(1);
        givenAlgorithmWithMaxUnit(TimeUnit.SECONDS);
        givenAlgorithmWithMinDelay(1);
        givenAlgorithmWithMinUnit(TimeUnit.SECONDS);
        givenAlgorithmWithQuitOnMaxDelay(false);
        whenBackoffControllerIsRun();
        assertThat(delays.size(), is(3));
        assertThat(delays.get(1) - delays.get(0),
                        is(delays.get(2) - delays.get(1)));
    }

    private void givenAlgorithmWithMaxDelay(long delay) {
        factory.setMaxDelay(delay);
    }

    private void givenAlgorithmWithMaxUnit(TimeUnit unit) {
        factory.setMaxUnit(unit);
    }

    private void givenAlgorithmWithMinDelay(long delay) {
        factory.setMinDelay(delay);
    }

    private void givenAlgorithmWithMinUnit(TimeUnit unit) {
        factory.setMinUnit(unit);
    }

    private void givenAlgorithmWithQuitOnMaxDelay(boolean quit) {
        factory.setQuitAtMaxDelay(quit);
    }

    private void givenFailureSimulatorWithFailFor(int failFor) {
        callable.failFor = failFor;
    }

    private void givenFailureSimulatorWithQuitAfter(int quitAfter) {
        callable.quitAfter = quitAfter;
    }

    private long roundToNearestTenthOfASecond(long now) {
        long r = now % 100;
        return now - r;
    }

    private void whenBackoffControllerIsRun() throws Exception {
        BackoffController controller = new BackoffControllerBuilder().using(
                        factory).build();
        controller.call(callable);
    }
}
