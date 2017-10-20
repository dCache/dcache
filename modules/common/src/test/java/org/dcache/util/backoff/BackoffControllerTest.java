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

import com.google.common.base.Throwables;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.Callable;

import org.dcache.util.backoff.IBackoffAlgorithm.Status;

import static org.dcache.util.backoff.IBackoffAlgorithm.Status.FAILURE;
import static org.dcache.util.backoff.IBackoffAlgorithm.Status.SUCCESS;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

/**
 * Test the BackoffController behaves correctly.
 *
 * For each unit-test, the target (the Callable) and the algorithm
 * (IBackoffAlgorithm) are preprogrammed to give a fixed number responses.  If
 * any additional requests are made to either then the unit-test will fail.
 */
public class BackoffControllerTest
{
    private IBackoffAlgorithmFactory factory;
    private Deque<Status> targetReplies;
    private List<Long> observedDelays;
    private Status status;

    @Before
    public void setup()
    {
        targetReplies = new ArrayDeque<>();
        observedDelays = new ArrayList<>();
    }

    @Test
    public void shouldNotRetryWhenSuccessful()
    {
        givenTargetThatReplies(SUCCESS);
        given(anAlgorithmThatSuggests());

        whenBackoffControllerIsCalled();

        assertThat(observedDelays, is(empty()));
        assertThat(status, is(SUCCESS));
    }

    @Test
    public void shouldFailIfTargetFailsAndAlgorithmIsAlwaysFail()
    {
        givenTargetThatReplies(FAILURE);
        given(anAlgorithmThatSuggests().fail());

        whenBackoffControllerIsCalled();

        assertThat(observedDelays, is(empty()));
        assertThat(status, is(FAILURE));
    }

    @Test
    public void shouldSucceedIfTargetFailsOnceWithOneSecondDelayAlgorithm()
    {
        givenTargetThatReplies(FAILURE, SUCCESS);
        given(anAlgorithmThatSuggests().delayFor(1000L));

        whenBackoffControllerIsCalled();

        assertThat(observedDelays, is(Arrays.asList(1000L)));
        assertThat(status, is(SUCCESS));
    }

    @Test
    public void shouldFailIfTargetFailsOnceWithSingleRetryAlgorithm()
    {
        givenTargetThatReplies(FAILURE, FAILURE);
        given(anAlgorithmThatSuggests().delayFor(1000L).fail());

        whenBackoffControllerIsCalled();

        assertThat(observedDelays, is(Arrays.asList(1000L)));
        assertThat(status, is(FAILURE));
    }

    @Test
    public void shouldSucceedIfTargetFailsTwiceThenSucceedsWithTwoRetryAlgorithm()
    {
        givenTargetThatReplies(FAILURE, FAILURE, SUCCESS);
        given(anAlgorithmThatSuggests().delayFor(1000L).delayFor(2000L));

        whenBackoffControllerIsCalled();

        assertThat(observedDelays, is(Arrays.asList(1000L, 2000L)));
        assertThat(status, is(SUCCESS));
    }

    private void givenTargetThatReplies(Status ...returns)
    {
        targetReplies.addAll(Arrays.asList(returns));
    }

    private void whenBackoffControllerIsCalled()
    {
        BackoffController controller = new BackoffController(factory) {
            @Override
            protected void handleWait(long wait)
            {
                observedDelays.add(wait);
            }
        };

        try {
            status = controller.call(() -> targetReplies.removeFirst());
        } catch (Exception e) {
            Throwables.propagate(e);
        }
    }

    private void given(AlgorithmFactoryBuilder builder)
    {
        factory = builder.build();
    }

    private AlgorithmFactoryBuilder anAlgorithmThatSuggests()
    {
        return new AlgorithmFactoryBuilder();
    }

    /**
     * Fluent interface for building an IBackoffAlgorithmFactory.
     */
    private static class AlgorithmFactoryBuilder
    {
        private final List<Long> delays = new ArrayList<>();

        AlgorithmFactoryBuilder delayFor(long duration)
        {
            delays.add(duration);
            return this;
        }

        AlgorithmFactoryBuilder fail()
        {
            delays.add(IBackoffAlgorithm.NO_WAIT);
            return this;
        }

        IBackoffAlgorithmFactory build()
        {
            return () -> () -> delays.remove(0);
        }
    }
}
