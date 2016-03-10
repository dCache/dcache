/* dCache - http://www.dcache.org/
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
package org.dcache.util;

import com.google.common.math.LongMath;
import com.google.common.primitives.Ints;

import static org.dcache.util.ByteUnit.Type.BINARY;
import static org.dcache.util.ByteUnit.Type.DECIMAL;

/**
 * A ByteUnit represents a scaling factor applied to some number of bytes.
 * Such scaling factors are intended to make it easier for humans to understand
 * large (and small) numbers and understand how two numbers compare; as such,
 * ByteUnit is often used with interfacing with humans.
 * <p>
 * There are two types of ByteUnit: ones that use decimal (base-10) prefixes and
 * ones that use binary (base-2) prefixes.  Decimal prefixes represent scaling
 * factors of the form 10^3n, where n is some non-negative integer.  Binary
 * prefixes represent scaling factors of the form 2^10n, where n is some
 * non-negative integer.  ByteUnit.BYTES is considered both a decimal and
 * binary scaling factor (i.e., with n=0 in both cases).
 * <p>
 * To avoid potential ambiguity, the ByteUnit enum values use ISO symbols.  It
 * contains both binary and decimal prefixes in strict ascending order.
 * <p>
 * Conversions are supported for int, long, float and double.  For the two
 * integer types (int and long) the maximum value is taken as a special case:
 * any conversion is always the same MAX_VALUE.  For integer numbers less than
 * the maximum value, the conversion could overflow (be less than MIN_VALUE or
 * more than MAX_VALUE).  If a conversion would overflow then an
 * ArithmeticException is thrown.
 * <p>
 * The maximum int and long values (Integer.MAX_VALUE and Long.MAX_VALUE
 * respectively) are accepted as a special case: any conversion will return the
 * same value.
 */
public enum ByteUnit
{
    /**
     * Represents no scaling has been applied.
     */
    BYTES {
        @Override
        public boolean hasType(Type type)
        {
            return true;
        }

        @Override
        public long toBytes(long d)
        {
            return d;
        }

        @Override
        public double toBytes(double d)
        {
            return d;
        }

        @Override
        public long toKB(long d)
        {
            return divideKeepingSaturation(d, 1_000L);
        }

        @Override
        public double toKB(double d)
        {
            return d / 1_000d;
        }

        @Override
        public long toKiB(long d)
        {
            return divideKeepingSaturation(d, 1L << 10);
        }

        @Override
        public double toKiB(double d)
        {
            return d / (1L << 10);
        }

        @Override
        public long toMB(long d)
        {
            return divideKeepingSaturation(d, 1_000_000L);
        }

        @Override
        public double toMB(double d)
        {
            return d / 1_000_000;
        }

        @Override
        public long toMiB(long d)
        {
            return divideKeepingSaturation(d, 1L << 20);
        }

        @Override
        public double toMiB(double d)
        {
            return d / (1L << 20);
        }

        @Override
        public long toGB(long d)
        {
            return divideKeepingSaturation(d, 1_000_000_000L);
        }

        @Override
        public double toGB(double d)
        {
            return d / 1_000_000_000L;
        }

        @Override
        public long toGiB(long d)
        {
            return divideKeepingSaturation(d, 1L << 30);
        }

        @Override
        public double toGiB(double d)
        {
            return d / (1L << 30);
        }

        @Override
        public long toTB(long d)
        {
            return divideKeepingSaturation(d, 1_000_000_000_000L);
        }

        @Override
        public double toTB(double d)
        {
            return d / 1_000_000_000_000L;
        }

        @Override
        public long toTiB(long d)
        {
            return divideKeepingSaturation(d, 1L << 40);
        }

        @Override
        public double toTiB(double d)
        {
            return d / (1L << 40);
        }

        @Override
        public long toPB(long d)
        {
            return divideKeepingSaturation(d, 1_000_000_000_000_000L);
        }

        @Override
        public double toPB(double d)
        {
            return d / 1_000_000_000_000_000L;
        }

        @Override
        public long toPiB(long d)
        {
            return divideKeepingSaturation(d, 1L << 50);
        }

        @Override
        public double toPiB(double d)
        {
            return d / (1L << 50);
        }

        @Override
        public long convert(long value, ByteUnit units)
        {
            return units.toBytes(value);
        }

        @Override
        public double convert(double value, ByteUnit units)
        {
            return units.toBytes(value);
        }
    },

    KB {
        @Override
        public boolean hasType(Type type)
        {
            return type == DECIMAL;
        }

        @Override
        public long toBytes(long d)
        {
            return multiplyKeepingSaturation(d, 1_000L);
        }

        @Override
        public double toBytes(double d)
        {
            return d * 1_000d;
        }

        @Override
        public long toKB(long d)
        {
            return d;
        }

        @Override
        public double toKB(double d)
        {
            return d;
        }

        @Override
        public long toMB(long d)
        {
            return divideKeepingSaturation(d, 1_000L);
        }

        @Override
        public long toGB(long d)
        {
            return divideKeepingSaturation(d, 1_000_000L);
        }

        @Override
        public long toTB(long d)
        {
            return divideKeepingSaturation(d, 1_000_000_000L);
        }

        @Override
        public long toPB(long d)
        {
            return divideKeepingSaturation(d, 1_000_000_000_000L);
        }

        @Override
        public long convert(long value, ByteUnit units)
        {
            return units.toKB(value);
        }

        @Override
        public double convert(double value, ByteUnit units)
        {
            return units.toKB(value);
        }
    },

    KiB {
        @Override
        public boolean hasType(Type type)
        {
            return type == BINARY;
        }

        @Override
        public long toBytes(long d)
        {
            return multiplyKeepingSaturation(d, 1L << 10);
        }

        @Override
        public double toBytes(double d)
        {
            return d * (1L << 10);
        }

        @Override
        public long toKiB(long d)
        {
            return d;
        }

        @Override
        public double toKiB(double d)
        {
            return d;
        }

        @Override
        public long toMiB(long d)
        {
            return divideKeepingSaturation(d, 1L << 10);
        }

        @Override
        public long toGiB(long d)
        {
            return divideKeepingSaturation(d, 1L << 20);
        }

        @Override
        public long toTiB(long d)
        {
            return divideKeepingSaturation(d, 1L << 30);
        }

        @Override
        public long toPiB(long d)
        {
            return divideKeepingSaturation(d, 1L << 40);
        }

        @Override
        public long convert(long value, ByteUnit units)
        {
            return units.toKiB(value);
        }

        @Override
        public double convert(double value, ByteUnit units)
        {
            return units.toKiB(value);
        }
    },

    MB {
        @Override
        public boolean hasType(Type type)
        {
            return type == DECIMAL;
        }

        @Override
        public long toBytes(long d)
        {
            return multiplyKeepingSaturation(d, 1_000_000L);
        }

        @Override
        public double toBytes(double d)
        {
            return d * 1_000_000d;
        }

        @Override
        public long toKB(long d)
        {
            return multiplyKeepingSaturation(d, 1_000L);
        }

        @Override
        public long toMB(long d)
        {
            return d;
        }

        @Override
        public double toMB(double d)
        {
            return d;
        }

        @Override
        public long toGB(long d)
        {
            return divideKeepingSaturation(d, 1_000L);
        }

        @Override
        public long toTB(long d)
        {
            return divideKeepingSaturation(d, 1_000_000L);
        }

        @Override
        public long toPB(long d)
        {
            return divideKeepingSaturation(d, 1_000_000_000L);
        }

        @Override
        public long convert(long value, ByteUnit units)
        {
            return units.toMB(value);
        }

        @Override
        public double convert(double value, ByteUnit units)
        {
            return units.toMB(value);
        }
    },

    MiB {
        @Override
        public boolean hasType(Type type)
        {
            return type == BINARY;
        }

        @Override
        public long toBytes(long d)
        {
            return multiplyKeepingSaturation(d, 1L << 20);
        }

        @Override
        public double toBytes(double d)
        {
            return d * (1L << 20);
        }

        @Override
        public long toKiB(long d)
        {
            return multiplyKeepingSaturation(d, 1L << 10);
        }

        @Override
        public long toMiB(long d)
        {
            return d;
        }

        @Override
        public double toMiB(double d)
        {
            return d;
        }

        @Override
        public long toGiB(long d)
        {
            return divideKeepingSaturation(d, 1L << 10);
        }

        @Override
        public long toTiB(long d)
        {
            return divideKeepingSaturation(d, 1L << 20);
        }

        @Override
        public long toPiB(long d)
        {
            return divideKeepingSaturation(d, 1L << 30);
        }

        @Override
        public long convert(long value, ByteUnit units)
        {
            return units.toMiB(value);
        }

        @Override
        public double convert(double value, ByteUnit units)
        {
            return units.toMiB(value);
        }
    },

    GB {
        @Override
        public boolean hasType(Type type)
        {
            return type == DECIMAL;
        }

        @Override
        public long toBytes(long d)
        {
            return multiplyKeepingSaturation(d, 1_000_000_000L);
        }

        @Override
        public double toBytes(double d)
        {
            return d * 1_000_000_000L;
        }

        @Override
        public long toKB(long d)
        {
            return multiplyKeepingSaturation(d, 1_000_000L);
        }

        @Override
        public long toMB(long d)
        {
            return multiplyKeepingSaturation(d, 1_000L);
        }

        @Override
        public long toGB(long d)
        {
            return d;
        }

        @Override
        public double toGB(double d)
        {
            return d;
        }

        @Override
        public long toTB(long d)
        {
            return divideKeepingSaturation(d, 1_000L);
        }

        @Override
        public long toPB(long d)
        {
            return divideKeepingSaturation(d, 1_000_000L);
        }

        @Override
        public long convert(long value, ByteUnit units)
        {
            return units.toGB(value);
        }

        @Override
        public double convert(double value, ByteUnit units)
        {
            return units.toGB(value);
        }
    },

    GiB {
        @Override
        public boolean hasType(Type type)
        {
            return type == BINARY;
        }

        @Override
        public long toBytes(long d)
        {
            return multiplyKeepingSaturation(d, 1L << 30);
        }

        @Override
        public double toBytes(double d)
        {
            return d * (1L << 30);
        }

        @Override
        public long toKiB(long d)
        {
            return multiplyKeepingSaturation(d, 1L << 20);
        }

        @Override
        public long toMiB(long d)
        {
            return multiplyKeepingSaturation(d, 1L << 10);
        }

        @Override
        public long toGiB(long d)
        {
            return d;
        }

        @Override
        public double toGiB(double d)
        {
            return d;
        }

        @Override
        public long toTiB(long d)
        {
            return divideKeepingSaturation(d, 1L << 10);
        }

        @Override
        public long toPiB(long d)
        {
            return divideKeepingSaturation(d, 1L << 20);
        }

        @Override
        public long convert(long value, ByteUnit units)
        {
            return units.toGiB(value);
        }

        @Override
        public double convert(double value, ByteUnit units)
        {
            return units.toGiB(value);
        }
    },

    TB {
        @Override
        public boolean hasType(Type type)
        {
            return type == DECIMAL;
        }

        @Override
        public long toBytes(long d)
        {
            return multiplyKeepingSaturation(d, 1_000_000_000_000L);
        }

        @Override
        public double toBytes(double d)
        {
            return d * 1_000_000_000_000d;
        }

        @Override
        public long toKB(long d)
        {
            return multiplyKeepingSaturation(d, 1_000_000_000L);
        }

        @Override
        public long toMB(long d)
        {
            return multiplyKeepingSaturation(d, 1_000_000L);
        }

        @Override
        public long toGB(long d)
        {
            return multiplyKeepingSaturation(d, 1_000L);
        }

        @Override
        public long toTB(long d)
        {
            return d;
        }

        @Override
        public double toTB(double d)
        {
            return d;
        }

        @Override
        public long toPB(long d)
        {
            return divideKeepingSaturation(d, 1_000L);
        }

        @Override
        public long convert(long value, ByteUnit units)
        {
            return units.toTB(value);
        }

        @Override
        public double convert(double value, ByteUnit units)
        {
            return units.toTB(value);
        }
    },

    TiB {
        @Override
        public boolean hasType(Type type)
        {
            return type == BINARY;
        }

        @Override
        public long toBytes(long d)
        {
            return multiplyKeepingSaturation(d, 1L << 40);
        }

        @Override
        public double toBytes(double d)
        {
            return d * (1L << 40);
        }

        @Override
        public long toKiB(long d)
        {
            return multiplyKeepingSaturation(d, 1L << 30);
        }

        @Override
        public long toMiB(long d)
        {
            return multiplyKeepingSaturation(d, 1L << 20);
        }

        @Override
        public long toGiB(long d)
        {
            return multiplyKeepingSaturation(d, 1L << 10);
        }

        @Override
        public long toTiB(long d)
        {
            return d;
        }

        @Override
        public double toTiB(double d)
        {
            return d;
        }

        @Override
        public long toPiB(long d)
        {
            return divideKeepingSaturation(d, 1L << 10);
        }

        @Override
        public long convert(long value, ByteUnit units)
        {
            return units.toTiB(value);
        }

        @Override
        public double convert(double value, ByteUnit units)
        {
            return units.toTiB(value);
        }
    },

    PB {
        @Override
        public boolean hasType(Type type)
        {
            return type == DECIMAL;
        }

        @Override
        public long toBytes(long d)
        {
            return multiplyKeepingSaturation(d, 1_000_000_000_000_000L);
        }

        @Override
        public double toBytes(double d)
        {
            return d * 1_000_000_000_000_000d;
        }

        @Override
        public long toKB(long d)
        {
            return multiplyKeepingSaturation(d, 1_000_000_000_000L);
        }

        @Override
        public long toMB(long d)
        {
            return multiplyKeepingSaturation(d, 1_000_000_000L);
        }

        @Override
        public long toGB(long d)
        {
            return multiplyKeepingSaturation(d, 1_000_000L);
        }

        @Override
        public long toTB(long d)
        {
            return multiplyKeepingSaturation(d, 1_000L);
        }

        @Override
        public long toPB(long d)
        {
            return d;
        }

        @Override
        public double toPB(double d)
        {
            return d;
        }

        @Override
        public long convert(long value, ByteUnit units)
        {
            return units.toPB(value);
        }

        @Override
        public double convert(double value, ByteUnit units)
        {
            return units.toPB(value);
        }
    },

    PiB {
        @Override
        public boolean hasType(Type type)
        {
            return type == BINARY;
        }

        @Override
        public long toBytes(long d)
        {
            return multiplyKeepingSaturation(d, 1L << 50);
        }

        @Override
        public double toBytes(double d)
        {
            return d * (1L << 50);
        }

        @Override
        public long toKiB(long d)
        {
            return multiplyKeepingSaturation(d, 1L << 40);
        }

        @Override
        public long toMiB(long d)
        {
            return multiplyKeepingSaturation(d, 1L << 30);
        }

        @Override
        public long toGiB(long d)
        {
            return multiplyKeepingSaturation(d, 1L << 20);
        }

        @Override
        public long toTiB(long d)
        {
            return multiplyKeepingSaturation(d, 1L << 10);
        }

        @Override
        public long toPiB(long d)
        {
            return d;
        }

        @Override
        public double toPiB(double d)
        {
            return d;
        }

        @Override
        public long convert(long value, ByteUnit units)
        {
            return units.toPiB(value);
        }

        @Override
        public double convert(double value, ByteUnit units)
        {
            return units.toPiB(value);
        }
    };

    /**
     * In which numerical base a BaseUnit is founded: base-10 or base-2.
     * Most BaseUnit values are either base-10 or base-2 with the exception of
     * BYTES which is considered both.
     */
    public enum Type
    {
        /**
         * A ByteUnit that is base-10.  Stated values must be multiplied by
         * 10^3n for some non-negative integer n: BYTES for n=0, KILOBYTES for
         * n=1 and so on.
         */
        DECIMAL {
            @Override
            public ByteUnit unitsOf(long value, long minValue)
            {
                long absValue = value == Long.MIN_VALUE ? Long.MAX_VALUE : Math.abs(value);
                if (absValue < KB.toBytes(minValue)) {
                    return BYTES;
                }
                if (absValue < MB.toBytes(minValue)) {
                    return KB;
                }
                if (absValue < GB.toBytes(minValue)) {
                    return MB;
                }
                if (absValue < TB.toBytes(minValue)) {
                    return GB;
                }
                if (absValue < PB.toBytes(minValue)) {
                    return TB;
                }
                return PB;
            }

            @Override
            public ByteUnit unitsOf(double value, double minValue)
            {
                double absValue = Math.abs(value);
                if (absValue < KB.toBytes(minValue)) {
                    return BYTES;
                }
                if (absValue < MB.toBytes(minValue)) {
                    return KB;
                }
                if (absValue < GB.toBytes(minValue)) {
                    return MB;
                }
                if (absValue < TB.toBytes(minValue)) {
                    return GB;
                }
                if (absValue < PB.toBytes(minValue)) {
                    return TB;
                }
                return PB;
            }

            @Override
            public ByteUnit exactUnitsOf(long value)
            {
                long absValue = Math.abs(value);
                if (absValue >= PB.toBytes(1L) && absValue % PB.toBytes(1L) == 0) {
                    return PB;
                }
                if (absValue >= TB.toBytes(1L) && absValue % TB.toBytes(1L) == 0) {
                    return TB;
                }
                if (absValue >= GB.toBytes(1L) && absValue % GB.toBytes(1L) == 0) {
                    return GB;
                }
                if (absValue >= MB.toBytes(1L) && absValue % MB.toBytes(1L) == 0) {
                    return MB;
                }
                if (absValue >= KB.toBytes(1L) && absValue % KB.toBytes(1L) == 0) {
                    return KB;
                }
                return BYTES;
            }
        },

        /**
         * A ByteUnit that is base-2.  Stated values must be multiplied by
         * 2^10n for some non-negative integer n: BYTES for n=0, KIBIBYTES for
         * n=1 and so on.
         */
        BINARY {
            @Override
            public ByteUnit unitsOf(long value, long minValue)
            {
                long absValue = value == Long.MIN_VALUE ? Long.MAX_VALUE : Math.abs(value);
                if (absValue < KiB.toBytes(minValue)) {
                    return BYTES;
                }
                if (absValue < MiB.toBytes(minValue)) {
                    return KiB;
                }
                if (absValue < GiB.toBytes(minValue)) {
                    return MiB;
                }
                if (absValue < TiB.toBytes(minValue)) {
                    return GiB;
                }
                if (absValue < PiB.toBytes(minValue)) {
                    return TiB;
                }
                return PiB;
            }

            @Override
            public ByteUnit unitsOf(double value, double minValue)
            {
                double absValue = Math.abs(value);
                if (absValue < KiB.toBytes(minValue)) {
                    return BYTES;
                }
                if (absValue < MiB.toBytes(minValue)) {
                    return KiB;
                }
                if (absValue < GiB.toBytes(minValue)) {
                    return MiB;
                }
                if (absValue < TiB.toBytes(minValue)) {
                    return GiB;
                }
                if (absValue < PiB.toBytes(minValue)) {
                    return TiB;
                }
                return PiB;
            }

            @Override
            public ByteUnit exactUnitsOf(long value)
            {
                long absValue = Math.abs(value);
                if (absValue >= PiB.toBytes(1L) && absValue % PiB.toBytes(1L) == 0) {
                    return PiB;
                }
                if (absValue >= TiB.toBytes(1L) && absValue % TiB.toBytes(1L) == 0) {
                    return TiB;
                }
                if (absValue >= GiB.toBytes(1L) && absValue % GiB.toBytes(1L) == 0) {
                    return GiB;
                }
                if (absValue >= MiB.toBytes(1L) && absValue % MiB.toBytes(1L) == 0) {
                    return MiB;
                }
                if (absValue >= KiB.toBytes(1L) && absValue % KiB.toBytes(1L) == 0) {
                    return KiB;
                }
                return BYTES;
            }
        };

        /**
         * Returns which units to use when describing this value.  More
         * specifically, it returns the ByteUnit prefix that, when
         * {@code convert(value, ByteUnit.BYTES);} is called, returns the
         * smallest absolute non-zero value.
         * If {@literal value} is zero then ByteUnit.BYTES is returned.
         */
        public ByteUnit unitsOf(long value)
        {
            return unitsOf(value, 1L);
        }

        /**
         * Return which units to use when describing this value.  More
         * specifically, it returns the largest ByteUnit that, when calling
         * {@code convert(value, ByteUnit.BYTES);}, returns a value of at least
         * {@literal minValue}.
         * If {@literal value} is zero then ByteUnit.BYTES is returned.
         */
        public abstract ByteUnit unitsOf(long value, long minValue);

        /**
         * Returns which units to use when describing this value.  More
         * specifically, it returns the ByteUnit prefix that, when
         * {@code convert(value, ByteUnit.BYTES);} is called, returns at least
         * 1. If {@literal value} is zero then ByteUnit.BYTES is returned.
         */
        public ByteUnit unitsOf(double value)
        {
            return unitsOf(value, 1d);
        }

        /**
         * Return which units to use when describing this value.  More
         * specifically, it returns the largest ByteUnit that, when calling
         * {@code convert(value, ByteUnit.BYTES);}, returns a value of at least
         * {@literal minValue}. If {@literal value} is zero then
         * ByteUnit.BYTES is returned.
         */
        public abstract ByteUnit unitsOf(double value, double minValue);

        /**
         * Provide the largest ByteUnit where the supplied value can be
         * represented exactly.  If zero is supplied then ByteUnit.BYTES is
         * returned.
         */
        public abstract ByteUnit exactUnitsOf(long value);
    }

    /**
     * Return whether this Prefix has the supplied type.  Note that
     * this method always returns true for ByteUnit.BYTES
     */
    public abstract boolean hasType(Type type);

    /**
     * Return Long.MAX_VALUE if either argument is Long.MAX_VALUE, otherwise
     * return the result of multiplying the two arguments or throw an exception
     * if the result overflows.
     */
    private static long multiplyKeepingSaturation(long a, long b) throws ArithmeticException
    {
        if (a == Long.MAX_VALUE || b == Long.MAX_VALUE) {
            return Long.MAX_VALUE;
        }

        return LongMath.checkedMultiply(a, b);
    }

    /** Return Long.MAX_VALUE if first value is same, otherwise do integer division. */
    private static long divideKeepingSaturation(long dividend, long divisor)
    {
        if (dividend == Long.MAX_VALUE) {
            return Long.MAX_VALUE;
        }

        return dividend / divisor;
    }

    /** A version of Guava's Ints.checkedCast that throws ArithmeticException. */
    private static int checkedCast(long value) throws ArithmeticException
    {
        try {
            return Ints.checkedCast(value);
        } catch (IllegalArgumentException inner) {
            ArithmeticException e = new ArithmeticException(inner.getMessage());
            e.addSuppressed(inner);
            throw e;
        }
    }

    /**
     * Converts the given value in the given prefix to this prefix.
     * Conversions from smaller to larger prefix truncate, so lose
     * precision. For example, converting 999 KILO to MEGA results in 0.
     * Conversions from larger to smaller granularities with arguments that
     * would numerically overflow saturate to Long.MIN_VALUE if negative or
     * Long.MAX_VALUE if positive.
     * For example, to convert 10 PEBI to MEBI, use: Prefix.MEBI.convert(10L, Prefix.PEBI)
     * @param sourceValue the value in some other prefix
     * @param sourcePrefix the prefix of sourceValue
     * @return the value in this prefix.
     */
    abstract public long convert(long sourceValue, ByteUnit sourcePrefix);

    abstract public double convert(double sourceValue, ByteUnit sourcePrefix);

    public int convert(int sourceValue, ByteUnit sourcePrefix)
    {
        if (sourceValue == Integer.MAX_VALUE) {
            return sourceValue;
        }
        return checkedCast(convert((long) sourceValue, sourcePrefix));
    }

    public int toBytes(int d)
    {
        if (d == Integer.MAX_VALUE) {
            return d;
        }
        return checkedCast(toBytes((long)d));
    }

    abstract public long toBytes(long d);

    public float toBytes(float d)
    {
        return (float) toBytes((double)d);
    }

    abstract public double toBytes(double d);

    public int toKB(int d)
    {
        if (d == Integer.MAX_VALUE) {
            return d;
        }
        return checkedCast(toKB((long)d));
    }

    public long toKB(long d)
    {
        return BYTES.toKB(toBytes(d));
    }

    public float toKB(float d)
    {
        return (float) toKB((double) d);
    }

    public double toKB(double d)
    {
        return BYTES.toKB(toBytes(d));
    }

    public int toKiB(int d)
    {
        if (d == Integer.MAX_VALUE) {
            return d;
        }
        return checkedCast(toKiB((long) d));
    }

    public long toKiB(long d)
    {
        return BYTES.toKiB(toBytes(d));
    }

    public float toKiB(float d)
    {
        return (float) toKiB((double) d);
    }

    public double toKiB(double d)
    {
        return BYTES.toKiB(toBytes(d));
    }

    public int toMB(int d)
    {
        if (d == Integer.MAX_VALUE) {
            return d;
        }
        return checkedCast(toMB((long) d));
    }

    public long toMB(long d)
    {
        return BYTES.toMB(toBytes(d));
    }

    public float toMB(float d)
    {
        return (float) toMB((double) d);
    }

    public double toMB(double d)
    {
        return BYTES.toMB(toBytes(d));
    }

    public int toMiB(int d)
    {
        if (d == Integer.MAX_VALUE) {
            return d;
        }
        return checkedCast(toMiB((long) d));
    }

    public long toMiB(long d)
    {
        return BYTES.toMiB(toBytes(d));
    }

    public float toMiB(float d)
    {
        return (float) toMiB((double) d);
    }

    public double toMiB(double d)
    {
        return BYTES.toMiB(toBytes(d));
    }

    public long toGB(long d)
    {
        return BYTES.toGB(toBytes(d));
    }

    public int toGB(int d)
    {
        if (d == Integer.MAX_VALUE) {
            return d;
        }
        return checkedCast(toGB((long) d));
    }

    public float toGB(float d)
    {
        return (float) toGB((double) d);
    }

    public double toGB(double d)
    {
        return BYTES.toGB(toBytes(d));
    }

    public long toGiB(long d)
    {
        return BYTES.toGiB(toBytes(d));
    }

    public int toGiB(int d)
    {
        if (d == Integer.MAX_VALUE) {
            return d;
        }
        return checkedCast(toGiB((long) d));
    }

    public float toGiB(float d)
    {
        return (float) toGiB((double) d);
    }

    public double toGiB(double d)
    {
        return BYTES.toGiB(toBytes(d));
    }

    public long toTB(long d)
    {
        return BYTES.toTB(toBytes(d));
    }

    public int toTB(int d)
    {
        if (d == Integer.MAX_VALUE) {
            return d;
        }
        return checkedCast(toTB((long)d));
    }

    public float toTB(float d)
    {
        return (float) toTB((double) d);
    }

    public double toTB(double d)
    {
        return BYTES.toTB(toBytes(d));
    }

    public long toTiB(long d)
    {
        return BYTES.toTiB(toBytes(d));
    }

    public int toTiB(int d)
    {
        if (d == Integer.MAX_VALUE) {
            return d;
        }
        return checkedCast(toTiB((long) d));
    }

    public float toTiB(float d)
    {
        return (float) toTiB((double) d);
    }

    public double toTiB(double d)
    {
        return BYTES.toTiB(toBytes(d));
    }

    public long toPB(long d)
    {
        return BYTES.toPB(toBytes(d));
    }

    public int toPB(int d)
    {
        if (d == Integer.MAX_VALUE) {
            return d;
        }
        return checkedCast(toPB((long) d));
    }

    public float toPB(float d)
    {
        return (float) toPB((double) d);
    }

    public double toPB(double d)
    {
        return BYTES.toPB(toBytes(d));
    }

    public long toPiB(long d)
    {
        return BYTES.toPiB(toBytes(d));
    }

    public int toPiB(int d)
    {
        if (d == Integer.MAX_VALUE) {
            return d;
        }
        return checkedCast(toPiB((long) d));
    }

    public float toPiB(float d)
    {
        return (float) toPiB((double) d);
    }

    public double toPiB(double d)
    {
        return BYTES.toPiB(toBytes(d));
    }
}
