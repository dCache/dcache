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

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.dcache.util.ByteUnits.Representation;

import static java.util.Objects.requireNonNull;

/**
 * This class provides a fluent interface for parsing String representations
 * of byte sizes.  Examples include parsing {@literal "20 MiB"} to
 * {@literal 20_971_520} and {@literal "3GB"} to either {@literal 3_000_000_000}
 * or {@literal 3_221_225_472} depending on the representation (ISO or JEDEC,
 * respectively).
 */
public class ByteSizeParser
{
    /**
     * Allowed input types.
     */
    public enum NumericalInput
    {
        INTEGER("[-+]?\\d*") {
            @Override
            protected long convert(String givenValue, ByteUnit givenUnits, ByteUnit targetUnits, Coercion coersion) {
                return targetUnits.convert(Long.parseLong(givenValue), givenUnits);
            }
        },

        FLOATING_POINT("[-+]?\\d*(?:\\.\\d*)?") {
            @Override
            protected long convert(String givenValue, ByteUnit givenUnits, ByteUnit targetUnits, Coercion coersion) {
                double value = Double.parseDouble(givenValue);
                return coersion.toLong(targetUnits.convert(value, givenUnits));
            }
        };

        private final String regularExpression;

        private NumericalInput(String re)
        {
            this.regularExpression = re;
        }

        abstract long convert(String value, ByteUnit givenUnits, ByteUnit targetUnits, Coercion coersion);
    }

    /**
     * How to convert a floating point value to an integer value.
     */
    public enum Coercion
    {
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
     * Whether the units are required or are optional.  If optional, a default
     * of ByteUnit.BYTES is used.
     */
    public enum UnitPresence
    {
        OPTIONAL,

        REQUIRED
    }

    /**
     * How to parse whitespace between the numerical part and the units.
     */
    public enum Whitespace
    {
        REQUIRED(" "), OPTIONAL(" ?"), NOT_ALLOWED("");

        private final String regularExpression;

        private Whitespace(String re)
        {
            this.regularExpression = re;
        }
    }

    public static class Builder
    {
        private final Representation representation;
        private NumericalInput input = NumericalInput.FLOATING_POINT;
        private Coercion coersion = Coercion.ROUND;
        private Whitespace whitespace = Whitespace.OPTIONAL;
        private UnitPresence unitPresence = UnitPresence.OPTIONAL;
        private ByteUnit defaultUnits = ByteUnit.BYTES;

        private Builder(Representation representation)
        {
            this.representation = requireNonNull(representation);
        }

        public Builder withCoersion(Coercion coersion)
        {
            this.coersion = requireNonNull(coersion);
            return this;
        }

        public Builder withInput(NumericalInput input)
        {
            this.input = requireNonNull(input);
            return this;
        }

        public Builder withWhitespace(Whitespace whitespace)
        {
            this.whitespace = requireNonNull(whitespace);
            return this;
        }

        public Builder withUnits(UnitPresence presence)
        {
            this.unitPresence = presence;
            return this;
        }

        /**
         * Which ByteUnit to use if units are optional and the parse value
         * does not specify any units.
         */
        public Builder withDefaultUnits(ByteUnit units)
        {
            defaultUnits = units;
            return this;
        }

        public long parse(String value, ByteUnit targetUnits) throws NumberFormatException
        {
            String whitespaceAndUnit = whitespace.regularExpression + "(?<unit>\\p{Alpha}+)";
            String whitespaceAndUnitWithPresence = unitPresence == UnitPresence.REQUIRED
                    ? whitespaceAndUnit
                    : ("(?:" + whitespaceAndUnit + ")?");

            Pattern pattern = Pattern.compile("(?<number>" + input.regularExpression + ")"
                    + whitespaceAndUnitWithPresence);
            Matcher m = pattern.matcher(value);
            if (!m.matches()) {
                throw new NumberFormatException("Bad input \"" + value + "\" does not match " + pattern);
            }

            ByteUnit givenUnits = Optional.ofNullable(m.group("unit"))
                    .map(s -> representation.parse(s).orElseThrow(() -> new NumberFormatException("Unknown unit \"" + s + "\"")))
                    .orElse(defaultUnits);

            return input.convert(m.group("number"), givenUnits, targetUnits, coersion);
        }

        public long parse(String value) throws NumberFormatException
        {
            return parse(value, ByteUnit.BYTES);
        }
    }

    private ByteSizeParser()
    {
        // prevent instantiation
    }

    /**
     * Create a new Builder that parses the input with the specified
     * representation.  By default, the parser accepts floating-point values,
     * a space between numerical value and units is optional, and the numerical
     * value is rounded to the nearest integer value.
     * @param representation How a ByteUnit is represented.
     * @return a Builder that may be configured before use in parsing a String.
     */
    public static Builder using(Representation representation)
    {
        return new Builder(representation);
    }
}
