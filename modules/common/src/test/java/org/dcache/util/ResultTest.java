/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2022-2024 Deutsches Elektronen-Synchrotron
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

import static com.github.npathai.hamcrestopt.OptionalMatchers.isEmpty;
import static com.github.npathai.hamcrestopt.OptionalMatchers.isPresentAnd;
import java.util.List;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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
    public void shouldEqualItself()
    {
        var r = Result.success("the result");
        assertThat(r, equalTo(r));
    }

    @Test
    public void shouldNotEqualAnotherObject()
    {
        assertThat(Result.success("the result"), not(equalTo(new Object())));
    }

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

    @Test
    public void shouldBeSuccessfulWhenSuccess() {
        Result<String,String> r = Result.success("success!");

        assertTrue(r.isSuccessful());
    }

    @Test
    public void shouldNotBeFailureWhenSuccess() {
        Result<String,String> r = Result.success("success!");

        assertFalse(r.isFailure());
    }

    @Test
    public void shouldNotBeSuccessfulWhenFailure() {
        Result<String,String> r = Result.failure("it went wrong");

        assertFalse(r.isSuccessful());
    }

    @Test
    public void shouldBeFailureWhenFailure() {
        Result<String,String> r = Result.failure("it went wrong!");

        assertTrue(r.isFailure());
    }

    @Test
    public void shouldHaveSuccessResultWhenSuccess() {
        Result<String,String> r = Result.success("success!");

        assertThat(r.getSuccess(), isPresentAnd(equalTo("success!")));
    }

    @Test
    public void shouldNotHaveFailureResultWhenSuccess() {
        Result<String,String> r = Result.success("success!");

        assertThat(r.getFailure(), isEmpty());
    }

    @Test
    public void shouldNotHaveSuccessResultWhenFailure() {
        Result<String,String> r = Result.failure("it went wrong");

        assertThat(r.getSuccess(), isEmpty());
    }

    @Test
    public void shouldHaveFailureResultWhenFailure() {
        Result<String,String> r = Result.failure("it went wrong");

        assertThat(r.getFailure(), isPresentAnd(equalTo("it went wrong")));
    }

    @Test
    public void shouldNotThrowExceptionWhenSuccess() {
        Result<String,String> r = Result.success("success!");

        r.orElseThrow(IllegalArgumentException::new);
    }

    @Test(expected=IllegalArgumentException.class)
    public void shouldThrowExceptionWhenFailure() {
        Result<String,String> r = Result.failure("it went wrong");

        r.orElseThrow(IllegalArgumentException::new);
    }

    @Test
    public void shouldMapSuccess() {
        Result<List<String>,String> input = Result.success(List.of("1"));

        Result<List<Integer>,String> output = input.map(s -> s.stream()
                .map(Integer::valueOf)
                .toList());

        assertThat(output.getFailure(), isEmpty());
        assertThat(output.getSuccess(), isPresentAnd(contains(1)));
    }

    @Test
    public void shouldNotMapFailure() {
        Result<List<String>,String> input = Result.failure("Not an integer");

        Result<List<Integer>,String> output = input.map(s -> s.stream()
                .map(Integer::valueOf)
                .toList());

        assertThat(output.getSuccess(), isEmpty());
        assertThat(output.getFailure(), isPresentAnd(equalTo("Not an integer")));
    }

    @Test
    public void shouldNotFlatMapFailure() {
        Result<List<String>,String> input = Result.failure("Not an integer");

        Result<List<Integer>,String> output = input.flatMap(s -> Result.success(List.of(1)));

        assertThat(output.getSuccess(), isEmpty());
        assertThat(output.getFailure(), isPresentAnd(equalTo("Not an integer")));
    }

    @Test
    public void shouldFlatMapToSuccess() {
        Result<List<String>,String> input = Result.success(List.of("1"));

        Result<List<Integer>,String> output = input.flatMap(s -> Result.success(List.of(1)));

        assertThat(output.getFailure(), isEmpty());
        assertThat(output.getSuccess(), isPresentAnd(contains(1)));
    }

    @Test
    public void shouldFlatMapToFailure() {
        Result<List<String>,String> input = Result.success(List.of("1"));

        Result<List<Integer>,String> output = input.flatMap(s -> Result.failure("invalid value"));

        assertThat(output.getSuccess(), isEmpty());
        assertThat(output.getFailure(), isPresentAnd(equalTo("invalid value")));
    }
}
