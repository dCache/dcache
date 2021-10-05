/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2018 Deutsches Elektronen-Synchrotron
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

import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.LongPredicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.dcache.util.ByteUnits.Representation;

/**
 * This class provides a fluent interface for parsing String representations of byte sizes.
 * Examples include parsing {@literal "20 MiB"} to {@literal 20_971_520} and {@literal "3GB"} to
 * either {@literal 3_000_000_000} or {@literal 3_221_225_472} depending on the representation (ISO
 * or JEDEC, respectively).
 */
public class ByteSizeParser {

    /**
     * A class that holds some test that a parsed value must satisfy to be accepted along with an
     * error message that describes a failure to satisfy that requirement.
     */
    private static class Requirement {

        private final LongPredicate requirement;
        private final String error;

        private Requirement(LongPredicate requirement, String error) {
            this.requirement = requirement;
            this.error = error;
        }

        public Optional<String> rejectionMessage(long value) {
            return requirement.test(value)
                  ? Optional.empty()
                  : Optional.of(error);
        }
    }

    /**
     * Allowed input types.
     */
    public enum NumericalInput {
        INTEGER("[-+]?\\d*") {
            @Override
            protected long convert(String givenValue, ByteUnit givenUnits, ByteUnit targetUnits,
                  Coercion coersion) {
                return targetUnits.convert(Long.parseLong(givenValue), givenUnits);
            }
        },

        FLOATING_POINT("[-+]?\\d*(?:\\.\\d*)?") {
            @Override
            protected long convert(String givenValue, ByteUnit givenUnits, ByteUnit targetUnits,
                  Coercion coersion) {
                double value = Double.parseDouble(givenValue);
                return coersion.toLong(targetUnits.convert(value, givenUnits));
            }
        };

        private final String regularExpression;

        private NumericalInput(String re) {
            this.regularExpression = re;
        }

        abstract long convert(String value, ByteUnit givenUnits, ByteUnit targetUnits,
              Coercion coersion);
    }

    /**
     * How to convert a floating point value to an integer value.
     */
    public enum Coercion {
        CEIL {
            @Override
            public long toLong(double value) {
                return (long) Math.ceil(value);
            }
        },

        FLOOR {
            @Override
            public long toLong(double value) {
                return (long) Math.floor(value);
            }
        },

        ROUND {
            @Override
            public long toLong(double value) {
                return (long) Math.round(value);
            }
        };

        abstract long toLong(double value);
    }

    /**
     * Whether the units are required or are optional.  If optional, a default of ByteUnit.BYTES is
     * used.
     */
    public enum UnitPresence {
        OPTIONAL,

        REQUIRED
    }

    /**
     * How to parse whitespace between the numerical part and the units.
     */
    public enum Whitespace {
        REQUIRED(" "), OPTIONAL(" ?"), NOT_ALLOWED("");

        private final String regularExpression;

        private Whitespace(String re) {
            this.regularExpression = re;
        }
    }

    public static class Builder {

        private final List<Representation> representations;
        private final List<Requirement> requirements = new ArrayList<>();
        private NumericalInput input = NumericalInput.FLOATING_POINT;
        private Coercion coercion = Coercion.ROUND;
        private Whitespace whitespace = Whitespace.OPTIONAL;
        private UnitPresence unitPresence = UnitPresence.OPTIONAL;
        private ByteUnit defaultUnits = ByteUnit.BYTES;

        private Builder(List<Representation> representations) {
            this.representations = requireNonNull(representations);
        }

        public Builder withCoersion(Coercion coersion) {
            this.coercion = requireNonNull(coersion);
            return this;
        }

        public Builder withInput(NumericalInput input) {
            this.input = requireNonNull(input);
            return this;
        }

        public Builder withWhitespace(Whitespace whitespace) {
            this.whitespace = requireNonNull(whitespace);
            return this;
        }

        public Builder withUnits(UnitPresence presence) {
            this.unitPresence = presence;
            return this;
        }

        public Builder requiring(LongPredicate requirement, String error) {
            requirements.add(new Requirement(requirement, error));
            return this;
        }

        /**
         * Which ByteUnit to use if units are optional and the parse value does not specify any
         * units.
         */
        public Builder withDefaultUnits(ByteUnit units) {
            defaultUnits = units;
            return this;
        }

        public ByteSizeParser build() {
            return new ByteSizeParser(this);
        }

    }

    private final List<Requirement> requirements;
    private final Whitespace whitespace;
    private final UnitPresence unitPresence;
    private final NumericalInput input;
    private final List<Representation> representations;
    private final ByteUnit defaultUnits;
    private final Coercion coercion;

    private Pattern pattern;

    /**
     * Create a new Builder that parses the input with the specified representations.  If multiple
     * representations are presented then an input's units string is parsed by the supplied
     * representations in the order given.  The first representation that can successfully parse the
     * units is used.
     * <p>
     * <b>Warning</b> it is recommended not to configure a parser to accept both
     * JEDEC and ISO representations.  This is because the parsed value may depend on case (upper
     * and lower case letters different values) or be otherwise poorly defined.
     * <p>
     * By default, the parser accepts floating-point values, a space between numerical value and
     * units is optional, and the numerical value is rounded to the nearest integer value.
     *
     * @param representation How a ByteUnit is represented.
     * @return a Builder that may be configured before use in parsing a String.
     */
    public static Builder using(Representation... representation) {
        return new Builder(asList(representation));
    }

    private ByteSizeParser(Builder builder) {
        whitespace = builder.whitespace;
        unitPresence = builder.unitPresence;
        input = builder.input;
        representations = builder.representations;
        defaultUnits = builder.defaultUnits;
        coercion = builder.coercion;
        requirements = List.copyOf(builder.requirements);
    }

    private synchronized Pattern pattern() {
        if (pattern == null) {
            String whitespaceAndUnit = whitespace.regularExpression + "(?<unit>\\p{Alpha}+)";
            String whitespaceAndUnitWithPresence = unitPresence == UnitPresence.REQUIRED
                  ? whitespaceAndUnit
                  : ("(?:" + whitespaceAndUnit + ")?");
            pattern = Pattern.compile("(?<number>" + input.regularExpression + ")"
                  + whitespaceAndUnitWithPresence);
        }
        return pattern;
    }

    public long parse(String value, ByteUnit targetUnits) throws NumberFormatException {
        Matcher m = pattern().matcher(value);
        if (!m.matches()) {
            throw new NumberFormatException(
                  "Bad input \"" + value + "\" does not match " + pattern);
        }

        ByteUnit givenUnits = Optional.ofNullable(m.group("unit"))
              .map(s -> parseUnit(s).orElseThrow(
                    () -> new NumberFormatException("Unknown unit \"" + s + "\"")))
              .orElse(defaultUnits);

        long size = input.convert(m.group("number"), givenUnits, targetUnits, coercion);

        requirements.stream()
              .map(r -> r.rejectionMessage(size))
              .flatMap(Optional::stream)
              .findFirst()
              .map(IllegalArgumentException::new)
              .ifPresent(e -> {
                  throw e;
              });

        return size;
    }

    private Optional<ByteUnit> parseUnit(String unit) {
        return representations.stream()
              .map(r -> r.parse(unit))
              .filter(Optional::isPresent)
              .map(Optional::get)
              .findFirst();
    }

    public long parse(String value) throws NumberFormatException {
        return parse(value, ByteUnit.BYTES);
    }
}
