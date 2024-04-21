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

import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * A generic type to hold the result of an operation that might fail.  This is
 * broadly similar to {@link java.util.Optional}.  The difference is that, in
 * addition to describing the output of a successful operation, this class also
 * carries a description of what went wrong should the operation fail.
 * <p>
 * The failure description could be as simple as a String (an error message) but
 * could be an Exception or a custom class that carries domain-specific
 * information.  Such information could, for example, help drive some recovery
 * procedure.
 * @param <S> The output from a successful operation.
 * @param <F> A description of what went wrong if the operation failed.
 */
public abstract class Result<S,F> {

    /**
     * Convert this result into some other type.  This other type must be able
     * to describe both successful operations and failures.
     * @param <C> The class to which this method converts.
     * @param mapFromSuccess The conversion from a successful result.
     * @param mapFromFailure The conversion from a failure.
     * @return The Result converted to some other, unified type.
     */
    public abstract <C> C map(Function<S,C> mapFromSuccess, Function<F,C> mapFromFailure);

    /**
     * Convert a successful result to another type.  This is equivalent to
     * {@link Optional#map(java.util.function.Function)}.  If this Result is
     * a failure then that failure value is returned, otherwise this Result's
     * success value is passed to <tt>mapSuccess</tt> and a new successful
     * Result is returned with the value returned by <tt>mapSuccess</tt>.
     * <p>
     * In effect, this processes a successful result using a procedure that
     * cannot fail.
     * <p>
     * Note: this is really just syntactical sugar over
     * {@link #map(java.util.function.Function, java.util.function.Function) },
     * but it helps maintain readability.
     * @param <U> The new successful type
     * @param mapSuccess The function that maps the successful result.
     */
    public <U> Result<U,F> map(Function<S,U> mapSuccess) {
        return map(s -> Result.success(mapSuccess.apply(s)), Result::failure);
    }

    /**
     * Further process a successful result with the possibility of failure.
     * This is equivalent to
     * {@link Optional#flatMap(java.util.function.Function)}. If this Result is
     * failure then this method returns a Result with that failure.
     * If this Result is a success then the Function <tt>processSuccess</tt> is
     * applied to this Result's successful result and the value returned by
     * <tt>processSuccess</tt> is returned.
     * <p>
     * In effect, this processes a successful result by a procedure that could
     * fail.
     * <p>
     * Note: this is really just syntactical sugar over
     * {@link #map(java.util.function.Function, java.util.function.Function) },
     * but it helps maintain readability.
     * @param <U> The new successful type
     * @param processSuccess The function that processes a successful result
     * @return an updated result.
     */
    public <U> Result<U,F> flatMap(Function<S,Result<U,F>> processSuccess) {
        return map(processSuccess, Result::failure);
    }

    /**
     * Consume this result.  The consumers are most likely using side-effects
     * (e.g., mutating an object's state or creating a log entry) to propagate
     * the result.
     * @param onSuccess Handle the output from a successful operation.
     * @param onFailure Handle the situation should the operation be unsuccessful.
     */
    public abstract void consume(Consumer<S> onSuccess, Consumer<F> onFailure);

    /**
     * A convenience method that returns only information about successful
     * results.  If the operation was unsuccessful then the returned object
     * provides no information on the nature of the failure.
     * @return Optionally the output from a successful operation.
     */
    public Optional<S> getSuccess() {
        return map(Optional::of, f -> Optional.empty());
    }

    /**
     * A convenience method that describes whether the operation was successful.
     * @return true if the operation was successful, false otherwise.
     */
    public boolean isSuccessful() {
        return map(s -> Boolean.TRUE, f -> Boolean.FALSE);
    }

    /**
     * A convenience method that provides access only to the failure
     * description.  If the result is successful then the corresponding object
     * is lost.
     * @return Optionally the failure description.
     */
    public Optional<F> getFailure() {
        return map(s -> Optional.empty(), Optional::of);
    }

    /**
     * A convenience method that describes whether the operation failed.
     * @return true if the operation failed, false otherwise.
     */
    public boolean isFailure() {
        return map(s -> Boolean.FALSE, f -> Boolean.TRUE);
    }

    /**
     * A convenience method that provides access to the successful result,
     * throwing an exception if the result describes a failure.
     * @param <E> The subclass of Exception that is thrown
     * @param toException Converts the failure description to the exception.
     * @return The successful result.
     * @throws E If the operation fails.
     */
    public <E extends Exception> S orElseThrow(Function<F, E> toException) throws E {
        Optional<F> maybeFailure = getFailure();
        if (maybeFailure.isPresent()) {
            F failure = maybeFailure.get();
            E exception = toException.apply(failure);
            throw exception;
        }
        return getSuccess().get();
    }

    @Override
    public final int hashCode() {
        return map(Object::hashCode, Object::hashCode);
    }

    /**
     * Return true if this Result is successful and the result of the operation
     * is equal to the supplied argument.  Return false if this result is a
     * failure or if this result is a success but the result does not match the
     * argument.
     * @param other the value to test.
     * @return true iff successful and the result matches.
     */
    private boolean successfulAndEquals(Object other) {
        return map(s -> s.equals(other), f -> Boolean.FALSE);
    }

    /**
     * Return true if this Result is a failure and the failure description
     * is equal to the supplied argument.  Return false if this result is a
     * success or if this result is a failure but the failure description does
     * not match the argument.
     * @param other the value to test.
     * @return true iff successful and the result matches.
     */
    private boolean failureAndEquals(Object other) {
        return map(s -> Boolean.FALSE, f -> f.equals(other));
    }

    @Override
    public final boolean equals(Object other) {
        if (other == this) {
            return true;
        }

        if (!(other instanceof Result)) {
            return false;
        }

        Result<?,?> otherResult = (Result<?,?>) other;

        return map(otherResult::successfulAndEquals, otherResult::failureAndEquals);
    }

    /**
     * Create and return a new Result object that represents a success.  The
     * returned object uses the argument to this method when describing this
     * success.
     * <p>
     * The {@link #map} method of the returned Result object will invoke the
     * {@literal mapFromSuccess} Function and the {@link #consume} method will
     * invoke the {@literal onSuccess} Consumer.
     * <p>
     * The {@link #isSuccessful} method will return {@literal true} and the
     * {@link #getSuccess} method will return an {@literal Optional} value
     * containing the argument.  Similarly, the {@link #isFailure} method will
     * return {@literal false} and the {@link #getFailure} method will return
     * an empty Optional.
     * <p>
     * The {@literal orElseThrow} method will return the argument.
     * @param <S> The class describing the successful result.
     * @param <F> The class describing a failed result.
     * @param value The details of this successful result.
     * @return A successful result.
     * @throws NullPointerException if {@literal value} is {@literal null}.
     */
    public static <S,F> Result<S,F> success(S value) {
        requireNonNull(value);

        return new Result<S,F>() {
            @Override
            public <C> C map(Function<S,C> mapFromSuccess, Function<F,C> mapFromFailure) {
                return mapFromSuccess.apply(value);
            }

            @Override
            public void consume(Consumer<S> onSuccess, Consumer<F> onFailure) {
                onSuccess.accept(value);
            }
        };
    }

    /**
     * Create and return a new Result object that represents a failure.  The
     * returned object uses the argument to this method when describing this
     * failure.
     * <p>
     * The {@link #map} method of the returned Result object will invoke the
     * {@literal mapFromFailure} Function and the {@link #consume} method will
     * invoke the {@literal onFailure} Consumer.
     * <p>
     * The {@link #isFailure} method will return {@literal true} and the
     * {@link #getFailure} method will return an {@literal Optional} value
     * containing the argument.  Similarly, the {@link #isSuccess} method will
     * return {@literal false} and the {@link #getSuccess} method will return
     * an empty Optional value.
     * <p>
     * The {@literal orElseThrow} method will throw an exception.
     * @param <S> The class describing the successful result.
     * @param <F> The class describing a failed result.
     * @param failure The details of this failure result.
     * @return A failed result.
     * @throws NullPointerException if {@literal value} is {@literal null}.
     */
    public static <S,F> Result<S,F> failure(F failure) {
        requireNonNull(failure);

        return new Result<S,F>() {
            @Override
            public <C> C map(Function<S,C> mapFromSuccess, Function<F,C> mapFromFailure) {
                return mapFromFailure.apply(failure);
            }

            @Override
            public void consume(Consumer<S> onSuccess, Consumer<F> onFailure) {
                onFailure.accept(failure);
            }
        };
    }
}
