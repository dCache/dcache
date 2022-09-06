/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2022 Deutsches Elektronen-Synchrotron
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
package org.dcache.util;

import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class ResultTest
{
    // Tests focusing on Result#hashCode

    @Test
    public void shouldHaveSameHashCodeWhenBothResultsAreSuccessfulWithSameResult()
    {
        Result<String,String> r1 = Result.success("the result");
        Result<String,String> r2 = Result.success("the result");

        assertThat(r1.hashCode(), is(equalTo(r2.hashCode())));
    }

    @Test
    public void shouldHaveSameHashCodeWhenBothResultsAreFailuresWithSameResult()
    {
        Result<String,String> r1 = Result.failure("the result");
        Result<String,String> r2 = Result.failure("the result");

        assertThat(r1.hashCode(), is(equalTo(r2.hashCode())));
    }

    // Tests focusing on Result#equals

    @Test
    public void shouldEqualSuccessfulResultWithEqualValueWhenSuccessful()
    {
        Result<String,String> r1 = Result.success("the result");
        Result<String,String> r2 = Result.success("the result");

        assertThat(r1, is(equalTo(r2)));
    }

    @Test
    public void shouldNotEqualSuccessfulResultWithDifferentValueWhenSuccessful()
    {
        Result<String,String> r1 = Result.success("the result");
        Result<String,String> r2 = Result.success("another result");

        assertThat(r1, is(not(equalTo(r2))));
    }

    @Test
    public void shouldNotEqualFailedResultWhenSuccessful()
    {
        Result<String,String> r1 = Result.success("the result");
        Result<String,String> r2 = Result.failure("the result");

        assertThat(r1, is(not(equalTo(r2))));
    }

    @Test
    public void shouldEqualFailedResultWithEqualValueWhenFailed()
    {
        Result<String,String> r1 = Result.failure("the result");
        Result<String,String> r2 = Result.failure("the result");

        assertThat(r1, is(equalTo(r2)));
    }

    @Test
    public void shouldNotEqualFailedResultWithDifferentValueWhenFailed()
    {
        Result<String,String> r1 = Result.failure("the result");
        Result<String,String> r2 = Result.failure("another result");

        assertThat(r1, is(not(equalTo(r2))));
    }

    @Test
    public void shouldNotEqualSuccessfulResultWhenFailed()
    {
        Result<String,String> r1 = Result.failure("the result");
        Result<String,String> r2 = Result.success("the result");

        assertThat(r1, is(not(equalTo(r2))));
    }
}
