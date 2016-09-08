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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * This class contains static utility methods that operate on or return objects
 * of type {@link Assumption}.
 */
public class Assumptions
{
    private static final Assumption UNRESTRICTED = new UnrestrictedAssumption();

    private Assumptions()
    {
    }

    /**
     * Returns an unrestricted assumption.
     */
    public static Assumption none()
    {
        return UNRESTRICTED;
    }

    static class CompositeAssumption implements Assumption
    {
        private static final long serialVersionUID = -4466714443555351986L;

        private final Set<Assumption> assumptions;

        public CompositeAssumption(ImmutableSet<Assumption> assumptions)
        {
            this.assumptions = checkNotNull(assumptions);
        }

        @Override
        public boolean isSatisfied(Pool pool)
        {
            return assumptions.stream().allMatch(a -> a.isSatisfied(pool));
        }

        @Override
        public Assumption and(Assumption that)
        {
            return that.and(this);
        }

        @Override
        public Assumption and(CompositeAssumption those)
        {
            return new CompositeAssumption(ImmutableSet.copyOf(Iterables.concat(this.assumptions, those.assumptions)));
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            CompositeAssumption that = (CompositeAssumption) o;
            return assumptions.equals(that.assumptions);
        }

        @Override
        public int hashCode()
        {
            return assumptions.hashCode();
        }

        @Override
        public String toString()
        {
            return assumptions.stream().map(Object::toString).collect(Collectors.joining(" and "));
        }

        Assumption unfold()
        {
            return assumptions.size() == 1 ? assumptions.iterator().next() : this;
        }
    }

    private static class UnrestrictedAssumption implements Assumption
    {
        private static final long serialVersionUID = -5962740006489865911L;

        @Override
        public boolean isSatisfied(Pool pool)
        {
            return true;
        }

        public Object readResolve()
        {
            return UNRESTRICTED;
        }

        @Override
        public Assumption and(Assumption that)
        {
            return that;
        }

        @Override
        public Assumption and(CompositeAssumption those)
        {
            return those.unfold();
        }

        @Override
        public String toString()
        {
            return "none";
        }
    }
}
