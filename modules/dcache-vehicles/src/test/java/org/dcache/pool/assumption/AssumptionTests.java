/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2016 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dcache.pool.assumption;

import static org.dcache.pool.assumption.Assumptions.none;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.util.OutOfDateCacheException;
import java.util.Collection;
import java.util.Collections;
import org.junit.Test;

public class AssumptionTests {

    @Test
    public void shouldProvideAssumptionThatAllowsEverything() throws OutOfDateCacheException {
        none().check(new EmptyPool());
    }

    @Test
    public void shouldConcatNoneAndNoneToNone() throws OutOfDateCacheException {
        assertThat(none().and(none()), is(none()));
    }

    @Test
    public void shouldConcatSomethingAndNoneToSomething() throws OutOfDateCacheException {
        assertThat(PerformanceCostAssumption.of(0, 1).and(none()),
              is(PerformanceCostAssumption.of(0, 1)));
    }

    @Test
    public void shouldConcatNoneAndSomethingToSomething() throws OutOfDateCacheException {
        assertThat(none().and(PerformanceCostAssumption.of(0, 1)),
              is(PerformanceCostAssumption.of(0, 1)));
    }

    @Test
    public void shouldConcatNoneAndSomethingAndSomethingToSomethingAndSomething()
          throws OutOfDateCacheException {
        assertThat(none().and(PerformanceCostAssumption.of(0, 1))
                    .and(PerformanceCostAssumption.of(0, 1)),
              is(PerformanceCostAssumption.of(0, 1).and(PerformanceCostAssumption.of(0, 1))));
    }

    @Test
    public void shouldConcatNoneAndSomethingAndSomethingToSomethingAndSomething2()
          throws OutOfDateCacheException {
        assertThat(none().and(
                    PerformanceCostAssumption.of(0, 1).and(PerformanceCostAssumption.of(0, 1))),
              is(PerformanceCostAssumption.of(0, 1).and(PerformanceCostAssumption.of(0, 1))));
    }

    @Test
    public void shouldConcatSomethingAndNoneAndSomethingToSomethingAndSomething()
          throws OutOfDateCacheException {
        assertThat(PerformanceCostAssumption.of(0, 1).and(none())
                    .and(PerformanceCostAssumption.of(0, 1)),
              is(PerformanceCostAssumption.of(0, 1).and(PerformanceCostAssumption.of(0, 1))));
    }

    @Test
    public void shouldConcatSomethingAndNoneAndSomethingToSomethingAndSomething2()
          throws OutOfDateCacheException {
        assertThat(PerformanceCostAssumption.of(0, 1)
                    .and(none().and(PerformanceCostAssumption.of(0, 1))),
              is(PerformanceCostAssumption.of(0, 1).and(PerformanceCostAssumption.of(0, 1))));
    }

    @Test
    public void shouldBeAssociative() throws OutOfDateCacheException {
        assertThat((PerformanceCostAssumption.of(0, 1).and(PerformanceCostAssumption.of(0, 1))).and(
                    PerformanceCostAssumption.of(0, 1)),
              is(PerformanceCostAssumption.of(0, 1).and(PerformanceCostAssumption.of(0, 1)
                    .and(PerformanceCostAssumption.of(0, 1)))));
    }

    private static class EmptyPool implements Assumption.Pool {

        @Override
        public String name() {
            return "pool";
        }

        @Override
        public PoolCostInfo.PoolSpaceInfo space() {
            return new PoolCostInfo.PoolSpaceInfo(0, 0, 0, 0, 0, 0, 0);
        }

        @Override
        public double moverCostFactor() {
            return 0;
        }

        @Override
        public PoolCostInfo.NamedPoolQueueInfo getMoverQueue(String name) {
            return null;
        }

        @Override
        public Collection<PoolCostInfo.NamedPoolQueueInfo> movers() {
            return Collections.emptyList();
        }

        @Override
        public PoolCostInfo.PoolQueueInfo p2PClient() {
            return new PoolCostInfo.PoolQueueInfo(0, 0, 0, 0, 0);
        }

        @Override
        public PoolCostInfo.PoolQueueInfo p2pServer() {
            return new PoolCostInfo.PoolQueueInfo(0, 0, 0, 0, 0);
        }

        @Override
        public PoolCostInfo.PoolQueueInfo restore() {
            return new PoolCostInfo.PoolQueueInfo(0, 0, 0, 0, 0);
        }

        @Override
        public PoolCostInfo.PoolQueueInfo store() {
            return new PoolCostInfo.PoolQueueInfo(0, 0, 0, 0, 0);
        }
    }
}
