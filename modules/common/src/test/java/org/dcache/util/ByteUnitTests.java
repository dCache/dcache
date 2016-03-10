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

import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class ByteUnitTests
{
    @Test
    public void shouldYieldUnitsOfBytes()
    {
        assertThat(ByteUnit.Type.BINARY.unitsOf(-1023L), is(ByteUnit.BYTES));
        assertThat(ByteUnit.Type.BINARY.unitsOf(-1L), is(ByteUnit.BYTES));
        assertThat(ByteUnit.Type.BINARY.unitsOf(0L), is(ByteUnit.BYTES));
        assertThat(ByteUnit.Type.BINARY.unitsOf(1L), is(ByteUnit.BYTES));
        assertThat(ByteUnit.Type.BINARY.unitsOf(1023L), is(ByteUnit.BYTES));

        assertThat(ByteUnit.Type.BINARY.unitsOf(-1023), is(ByteUnit.BYTES));
        assertThat(ByteUnit.Type.BINARY.unitsOf(-1), is(ByteUnit.BYTES));
        assertThat(ByteUnit.Type.BINARY.unitsOf(0), is(ByteUnit.BYTES));
        assertThat(ByteUnit.Type.BINARY.unitsOf(1), is(ByteUnit.BYTES));
        assertThat(ByteUnit.Type.BINARY.unitsOf(1023), is(ByteUnit.BYTES));

        assertThat(ByteUnit.Type.DECIMAL.unitsOf(-999L), is(ByteUnit.BYTES));
        assertThat(ByteUnit.Type.DECIMAL.unitsOf(-1L), is(ByteUnit.BYTES));
        assertThat(ByteUnit.Type.DECIMAL.unitsOf(0L), is(ByteUnit.BYTES));
        assertThat(ByteUnit.Type.DECIMAL.unitsOf(1L), is(ByteUnit.BYTES));
        assertThat(ByteUnit.Type.DECIMAL.unitsOf(999L), is(ByteUnit.BYTES));

        assertThat(ByteUnit.Type.DECIMAL.unitsOf(-999), is(ByteUnit.BYTES));
        assertThat(ByteUnit.Type.DECIMAL.unitsOf(-1), is(ByteUnit.BYTES));
        assertThat(ByteUnit.Type.DECIMAL.unitsOf(0), is(ByteUnit.BYTES));
        assertThat(ByteUnit.Type.DECIMAL.unitsOf(1), is(ByteUnit.BYTES));
        assertThat(ByteUnit.Type.DECIMAL.unitsOf(999), is(ByteUnit.BYTES));
    }

    @Test
    public void shouldYieldExactUnitsOfBytes()
    {
        assertThat(ByteUnit.Type.BINARY.exactUnitsOf(-(1L+1125899906842624L)), is(ByteUnit.BYTES));
        assertThat(ByteUnit.Type.BINARY.exactUnitsOf(-(1L+1099511627776L)), is(ByteUnit.BYTES));
        assertThat(ByteUnit.Type.BINARY.exactUnitsOf(-(1L+1073741824L)), is(ByteUnit.BYTES));
        assertThat(ByteUnit.Type.BINARY.exactUnitsOf(-(1L+1048576L)), is(ByteUnit.BYTES));
        assertThat(ByteUnit.Type.BINARY.exactUnitsOf(-(1L+1024L)), is(ByteUnit.BYTES));
        assertThat(ByteUnit.Type.BINARY.exactUnitsOf(-2L), is(ByteUnit.BYTES));
        assertThat(ByteUnit.Type.BINARY.exactUnitsOf(-1L), is(ByteUnit.BYTES));
        assertThat(ByteUnit.Type.BINARY.exactUnitsOf(0L), is(ByteUnit.BYTES));
        assertThat(ByteUnit.Type.BINARY.exactUnitsOf(1L), is(ByteUnit.BYTES));
        assertThat(ByteUnit.Type.BINARY.exactUnitsOf(2L), is(ByteUnit.BYTES));
        assertThat(ByteUnit.Type.BINARY.exactUnitsOf(1L+1024L), is(ByteUnit.BYTES));
        assertThat(ByteUnit.Type.BINARY.exactUnitsOf(1L+1048576L), is(ByteUnit.BYTES));
        assertThat(ByteUnit.Type.BINARY.exactUnitsOf(1L+1073741824L), is(ByteUnit.BYTES));
        assertThat(ByteUnit.Type.BINARY.exactUnitsOf(1L+1099511627776L), is(ByteUnit.BYTES));
        assertThat(ByteUnit.Type.BINARY.exactUnitsOf(1L+1125899906842624L), is(ByteUnit.BYTES));


        assertThat(ByteUnit.Type.DECIMAL.exactUnitsOf(-1_000_000_000_000_001L), is(ByteUnit.BYTES));
        assertThat(ByteUnit.Type.DECIMAL.exactUnitsOf(-1_000_000_000_001L), is(ByteUnit.BYTES));
        assertThat(ByteUnit.Type.DECIMAL.exactUnitsOf(-1_000_000_001L), is(ByteUnit.BYTES));
        assertThat(ByteUnit.Type.DECIMAL.exactUnitsOf(-1_000_001L), is(ByteUnit.BYTES));
        assertThat(ByteUnit.Type.DECIMAL.exactUnitsOf(-1_001L), is(ByteUnit.BYTES));
        assertThat(ByteUnit.Type.DECIMAL.exactUnitsOf(-1L), is(ByteUnit.BYTES));
        assertThat(ByteUnit.Type.DECIMAL.exactUnitsOf(0L), is(ByteUnit.BYTES));
        assertThat(ByteUnit.Type.DECIMAL.exactUnitsOf(1L), is(ByteUnit.BYTES));
        assertThat(ByteUnit.Type.DECIMAL.exactUnitsOf(1_001L), is(ByteUnit.BYTES));
        assertThat(ByteUnit.Type.DECIMAL.exactUnitsOf(1_000_001L), is(ByteUnit.BYTES));
        assertThat(ByteUnit.Type.DECIMAL.exactUnitsOf(1_000_000_001L), is(ByteUnit.BYTES));
        assertThat(ByteUnit.Type.DECIMAL.exactUnitsOf(1_000_000_000_001L), is(ByteUnit.BYTES));
        assertThat(ByteUnit.Type.DECIMAL.exactUnitsOf(1_000_000_000_000_001L), is(ByteUnit.BYTES));
    }

    @Test
    public void shouldClaimBytesType()
    {
        assertThat(ByteUnit.BYTES.hasType(ByteUnit.Type.BINARY), equalTo(true));
        assertThat(ByteUnit.BYTES.hasType(ByteUnit.Type.DECIMAL), equalTo(true));
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertToBytes1()
    {
        ByteUnit.BYTES.convert(1, ByteUnit.TB);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertToBytes2()
    {
        ByteUnit.BYTES.convert(1, ByteUnit.PB);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertToBytes3()
    {
        ByteUnit.BYTES.convert(1, ByteUnit.TiB);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertToBytes4()
    {
        ByteUnit.BYTES.convert(1, ByteUnit.PiB);
    }

    @Test
    public void shouldConvertToBytes()
    {
        assertThat(ByteUnit.BYTES.convert(5L, ByteUnit.BYTES), equalTo(5L));
        assertThat(ByteUnit.BYTES.convert(1L, ByteUnit.KB), equalTo(1_000L));
        assertThat(ByteUnit.BYTES.convert(1L, ByteUnit.MB), equalTo(1_000_000L));
        assertThat(ByteUnit.BYTES.convert(1L, ByteUnit.GB), equalTo(1_000_000_000L));
        assertThat(ByteUnit.BYTES.convert(1L, ByteUnit.TB), equalTo(1_000_000_000_000L));
        assertThat(ByteUnit.BYTES.convert(1L, ByteUnit.PB), equalTo(1_000_000_000_000_000L));

        assertThat(ByteUnit.BYTES.convert(1L, ByteUnit.KiB), equalTo(1024L));
        assertThat(ByteUnit.BYTES.convert(1L, ByteUnit.MiB), equalTo(1048576L));
        assertThat(ByteUnit.BYTES.convert(1L, ByteUnit.GiB), equalTo(1073741824L));
        assertThat(ByteUnit.BYTES.convert(1L, ByteUnit.TiB), equalTo(1099511627776L));
        assertThat(ByteUnit.BYTES.convert(1L, ByteUnit.PiB), equalTo(1125899906842624L));

        assertThat(ByteUnit.BYTES.convert(5, ByteUnit.BYTES), equalTo(5));
        assertThat(ByteUnit.BYTES.convert(1, ByteUnit.KB), equalTo(1_000));
        assertThat(ByteUnit.BYTES.convert(1, ByteUnit.MB), equalTo(1_000_000));
        assertThat(ByteUnit.BYTES.convert(1, ByteUnit.GB), equalTo(1_000_000_000));

        assertThat(ByteUnit.BYTES.convert(1, ByteUnit.KiB), equalTo(1024));
        assertThat(ByteUnit.BYTES.convert(1, ByteUnit.MiB), equalTo(1048576));
        assertThat(ByteUnit.BYTES.convert(1, ByteUnit.GiB), equalTo(1073741824));

        assertThat(ByteUnit.BYTES.convert(1d, ByteUnit.BYTES), equalTo(1d));
        assertThat(ByteUnit.BYTES.convert(1d, ByteUnit.KB), equalTo(1_000d));
        assertThat(ByteUnit.BYTES.convert(1d, ByteUnit.MB), equalTo(1_000_000d));
        assertThat(ByteUnit.BYTES.convert(1d, ByteUnit.GB), equalTo(1_000_000_000d));
        assertThat(ByteUnit.BYTES.convert(1d, ByteUnit.TB), equalTo(1_000_000_000_000d));
        assertThat(ByteUnit.BYTES.convert(1d, ByteUnit.PB), equalTo(1_000_000_000_000_000d));

        assertThat(ByteUnit.BYTES.convert(1d, ByteUnit.KiB), equalTo(1024d));
        assertThat(ByteUnit.BYTES.convert(1d, ByteUnit.MiB), equalTo(1048576d));
        assertThat(ByteUnit.BYTES.convert(1d, ByteUnit.GiB), equalTo(1073741824d));
        assertThat(ByteUnit.BYTES.convert(1d, ByteUnit.TiB), equalTo(1099511627776d));
        assertThat(ByteUnit.BYTES.convert(1d, ByteUnit.PiB), equalTo(1125899906842624d));
    }

    @Test
    public void shouldConvertFromBytesToBytes()
    {
        assertThat(ByteUnit.BYTES.toBytes(Integer.MIN_VALUE), equalTo(Integer.MIN_VALUE));
        assertThat(ByteUnit.BYTES.toBytes(-1), equalTo(-1));
        assertThat(ByteUnit.BYTES.toBytes(0), equalTo(0));
        assertThat(ByteUnit.BYTES.toBytes(1), equalTo(1));
        assertThat(ByteUnit.BYTES.toBytes(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.BYTES.toBytes(Long.MIN_VALUE), equalTo(Long.MIN_VALUE));
        assertThat(ByteUnit.BYTES.toBytes(-1L), equalTo(-1L));
        assertThat(ByteUnit.BYTES.toBytes(0L), equalTo(0L));
        assertThat(ByteUnit.BYTES.toBytes(1L), equalTo(1L));
        assertThat(ByteUnit.BYTES.toBytes(Long.MAX_VALUE), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.BYTES.toBytes(-1f), equalTo(-1f));
        assertThat(ByteUnit.BYTES.toBytes(0f), equalTo(0f));
        assertThat(ByteUnit.BYTES.toBytes(1f), equalTo(1f));

        assertThat(ByteUnit.BYTES.toBytes(-1d), equalTo(-1d));
        assertThat(ByteUnit.BYTES.toBytes(0d), equalTo(0d));
        assertThat(ByteUnit.BYTES.toBytes(1d), equalTo(1d));
    }

    @Test
    public void shouldConvertFromBytesToKilo()
    {
        assertThat(ByteUnit.BYTES.toKB(Integer.MIN_VALUE), equalTo(-2147483));
        assertThat(ByteUnit.BYTES.toKB(-1_000), equalTo(-1));
        assertThat(ByteUnit.BYTES.toKB(-999), equalTo(0));
        assertThat(ByteUnit.BYTES.toKB(-1), equalTo(0));
        assertThat(ByteUnit.BYTES.toKB(0), equalTo(0));
        assertThat(ByteUnit.BYTES.toKB(1), equalTo(0));
        assertThat(ByteUnit.BYTES.toKB(999), equalTo(0));
        assertThat(ByteUnit.BYTES.toKB(1_000), equalTo(1));
        assertThat(ByteUnit.BYTES.toKB(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.BYTES.toKB(Long.MIN_VALUE), equalTo(-9223372036854775L));
        assertThat(ByteUnit.BYTES.toKB(-1_000L), equalTo(-1L));
        assertThat(ByteUnit.BYTES.toKB(-999L), equalTo(0L));
        assertThat(ByteUnit.BYTES.toKB(-1L), equalTo(0L));
        assertThat(ByteUnit.BYTES.toKB(0L), equalTo(0L));
        assertThat(ByteUnit.BYTES.toKB(1L), equalTo(0L));
        assertThat(ByteUnit.BYTES.toKB(999L), equalTo(0L));
        assertThat(ByteUnit.BYTES.toKB(1_000L), equalTo(1L));
        assertThat(ByteUnit.BYTES.toKB(Long.MAX_VALUE), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.BYTES.toKB(-1_000f), equalTo(-1f));
        assertThat(ByteUnit.BYTES.toKB(-999f), equalTo(-.999f));
        assertThat(ByteUnit.BYTES.toKB(-1f), equalTo(-0.001f));
        assertThat(ByteUnit.BYTES.toKB(0f), equalTo(0f));
        assertThat(ByteUnit.BYTES.toKB(1f), equalTo(0.001f));
        assertThat(ByteUnit.BYTES.toKB(999f), equalTo(0.999f));
        assertThat(ByteUnit.BYTES.toKB(1_000f), equalTo(1f));

        assertThat(ByteUnit.BYTES.toKB(-1_000d), equalTo(-1d));
        assertThat(ByteUnit.BYTES.toKB(-999d), equalTo(-0.999d));
        assertThat(ByteUnit.BYTES.toKB(-1d), equalTo(-0.001d));
        assertThat(ByteUnit.BYTES.toKB(0d), equalTo(0d));
        assertThat(ByteUnit.BYTES.toKB(1d), equalTo(0.001d));
        assertThat(ByteUnit.BYTES.toKB(999d), equalTo(0.999d));
        assertThat(ByteUnit.BYTES.toKB(1_000d), equalTo(1d));
    }

    @Test
    public void shouldConvertFromBytesToKibi()
    {
        assertThat(ByteUnit.BYTES.toKiB(Integer.MIN_VALUE), equalTo(-2097152));
        assertThat(ByteUnit.BYTES.toKiB(-1024), equalTo(-1));
        assertThat(ByteUnit.BYTES.toKiB(-1023), equalTo(0));
        assertThat(ByteUnit.BYTES.toKiB(-1), equalTo(0));
        assertThat(ByteUnit.BYTES.toKiB(0), equalTo(0));
        assertThat(ByteUnit.BYTES.toKiB(1), equalTo(0));
        assertThat(ByteUnit.BYTES.toKiB(1023), equalTo(0));
        assertThat(ByteUnit.BYTES.toKiB(1024), equalTo(1));
        assertThat(ByteUnit.BYTES.toKiB(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.BYTES.toKiB(Long.MIN_VALUE), equalTo(-9007199254740992L));
        assertThat(ByteUnit.BYTES.toKiB(-1024L), equalTo(-1L));
        assertThat(ByteUnit.BYTES.toKiB(-1023L), equalTo(0L));
        assertThat(ByteUnit.BYTES.toKiB(-1L), equalTo(0L));
        assertThat(ByteUnit.BYTES.toKiB(0L), equalTo(0L));
        assertThat(ByteUnit.BYTES.toKiB(1L), equalTo(0L));
        assertThat(ByteUnit.BYTES.toKiB(1023L), equalTo(0L));
        assertThat(ByteUnit.BYTES.toKiB(1024L), equalTo(1L));
        assertThat(ByteUnit.BYTES.toKiB(Long.MAX_VALUE), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.BYTES.toKiB(-1024f), equalTo(-1f));
        assertThat(ByteUnit.BYTES.toKiB(-1023f), equalTo(-0.99902344f));
        assertThat(ByteUnit.BYTES.toKiB(-1f), equalTo(-0.0009765625f));
        assertThat(ByteUnit.BYTES.toKiB(0f), equalTo(0f));
        assertThat(ByteUnit.BYTES.toKiB(1f), equalTo(0.0009765625f));
        assertThat(ByteUnit.BYTES.toKiB(1023f), equalTo(0.99902344f));
        assertThat(ByteUnit.BYTES.toKiB(1024f), equalTo(1f));

        assertThat(ByteUnit.BYTES.toKiB(-1024d), equalTo(-1d));
        assertThat(ByteUnit.BYTES.toKiB(-1023d), equalTo(-0.9990234375d));
        assertThat(ByteUnit.BYTES.toKiB(-1d), equalTo(-0.0009765625d));
        assertThat(ByteUnit.BYTES.toKiB(0d), equalTo(0d));
        assertThat(ByteUnit.BYTES.toKiB(1d), equalTo(0.0009765625d));
        assertThat(ByteUnit.BYTES.toKiB(1023d), equalTo(0.9990234375d));
        assertThat(ByteUnit.BYTES.toKiB(1024d), equalTo(1d));
    }


    @Test
    public void shouldConvertFromBytesToMega()
    {
        assertThat(ByteUnit.BYTES.toMB(Integer.MIN_VALUE), equalTo(-2147));
        assertThat(ByteUnit.BYTES.toMB(-1_000_000), equalTo(-1));
        assertThat(ByteUnit.BYTES.toMB(-999_999), equalTo(0));
        assertThat(ByteUnit.BYTES.toMB(-1), equalTo(0));
        assertThat(ByteUnit.BYTES.toMB(0), equalTo(0));
        assertThat(ByteUnit.BYTES.toMB(1), equalTo(0));
        assertThat(ByteUnit.BYTES.toMB(999_999), equalTo(0));
        assertThat(ByteUnit.BYTES.toMB(1_000_000), equalTo(1));
        assertThat(ByteUnit.BYTES.toMB(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.BYTES.toMB(Long.MIN_VALUE), equalTo(-9223372036854L));
        assertThat(ByteUnit.BYTES.toMB(-1_000_000L), equalTo(-1L));
        assertThat(ByteUnit.BYTES.toMB(-999_999L), equalTo(0L));
        assertThat(ByteUnit.BYTES.toMB(-1L), equalTo(0L));
        assertThat(ByteUnit.BYTES.toMB(0L), equalTo(0L));
        assertThat(ByteUnit.BYTES.toMB(1L), equalTo(0L));
        assertThat(ByteUnit.BYTES.toMB(999_999L), equalTo(0L));
        assertThat(ByteUnit.BYTES.toMB(1_000_000L), equalTo(1L));
        assertThat(ByteUnit.BYTES.toMB(Long.MAX_VALUE), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.BYTES.toMB(-1_000_000f), equalTo(-1f));
        assertThat(ByteUnit.BYTES.toMB(-999_999f), equalTo(-0.999_999f));
        assertThat(ByteUnit.BYTES.toMB(-1f), equalTo(-0.000_001f));
        assertThat(ByteUnit.BYTES.toMB(0f), equalTo(0f));
        assertThat(ByteUnit.BYTES.toMB(1f), equalTo(0.000_001f));
        assertThat(ByteUnit.BYTES.toMB(999_999f), equalTo(0.999_999f));
        assertThat(ByteUnit.BYTES.toMB(1_000_000f), equalTo(1f));

        assertThat(ByteUnit.BYTES.toMB(-1_000_000d), equalTo(-1d));
        assertThat(ByteUnit.BYTES.toMB(-999_999d), equalTo(-0.999_999d));
        assertThat(ByteUnit.BYTES.toMB(-1d), equalTo(-0.000_001d));
        assertThat(ByteUnit.BYTES.toMB(0d), equalTo(0d));
        assertThat(ByteUnit.BYTES.toMB(1d), equalTo(0.000_001d));
        assertThat(ByteUnit.BYTES.toMB(999_999d), equalTo(0.999_999d));
        assertThat(ByteUnit.BYTES.toMB(1_000_000d), equalTo(1d));
    }


    @Test
    public void shouldConvertFromBytesToMebi()
    {
        assertThat(ByteUnit.BYTES.toMiB(Integer.MIN_VALUE), equalTo(-2048));
        assertThat(ByteUnit.BYTES.toMiB(-1_048_576), equalTo(-1));
        assertThat(ByteUnit.BYTES.toMiB(-1_048_575), equalTo(0));
        assertThat(ByteUnit.BYTES.toMiB(-1), equalTo(0));
        assertThat(ByteUnit.BYTES.toMiB(0), equalTo(0));
        assertThat(ByteUnit.BYTES.toMiB(1), equalTo(0));
        assertThat(ByteUnit.BYTES.toMiB(1_048_575), equalTo(0));
        assertThat(ByteUnit.BYTES.toMiB(1_048_576), equalTo(1));
        assertThat(ByteUnit.BYTES.toMiB(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.BYTES.toMiB(Long.MIN_VALUE), equalTo(-8796093022208L));
        assertThat(ByteUnit.BYTES.toMiB(-1_048_576L), equalTo(-1L));
        assertThat(ByteUnit.BYTES.toMiB(-1_048_575L), equalTo(0L));
        assertThat(ByteUnit.BYTES.toMiB(-1L), equalTo(0L));
        assertThat(ByteUnit.BYTES.toMiB(0L), equalTo(0L));
        assertThat(ByteUnit.BYTES.toMiB(1L), equalTo(0L));
        assertThat(ByteUnit.BYTES.toMiB(1_048_575L), equalTo(0L));
        assertThat(ByteUnit.BYTES.toMiB(1_048_576L), equalTo(1L));
        assertThat(ByteUnit.BYTES.toMiB(Long.MAX_VALUE), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.BYTES.toMiB(-1_048_576f), equalTo(-1f));
        assertThat(ByteUnit.BYTES.toMiB(-1_048_575f), equalTo(-0.99999905f));
        assertThat(ByteUnit.BYTES.toMiB(-1f), equalTo(-0.00000095367432f));
        assertThat(ByteUnit.BYTES.toMiB(0f), equalTo(0f));
        assertThat(ByteUnit.BYTES.toMiB(1f), equalTo(0.00000095367432f));
        assertThat(ByteUnit.BYTES.toMiB(1_048_575f), equalTo(0.99999905f));
        assertThat(ByteUnit.BYTES.toMiB(1_048_576f), equalTo(1f));
    }

    @Test
    public void shouldConvertFromBytesToGiga()
    {
        assertThat(ByteUnit.BYTES.toGB(Integer.MIN_VALUE), equalTo(-2));
        assertThat(ByteUnit.BYTES.toGB(-1_000_000_000), equalTo(-1));
        assertThat(ByteUnit.BYTES.toGB(-999_999_999), equalTo(0));
        assertThat(ByteUnit.BYTES.toGB(-1), equalTo(0));
        assertThat(ByteUnit.BYTES.toGB(0), equalTo(0));
        assertThat(ByteUnit.BYTES.toGB(1), equalTo(0));
        assertThat(ByteUnit.BYTES.toGB(999_999_999), equalTo(0));
        assertThat(ByteUnit.BYTES.toGB(1_000_000_000), equalTo(1));
        assertThat(ByteUnit.BYTES.toGB(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.BYTES.toGB(Long.MIN_VALUE), equalTo(-9223372036L));
        assertThat(ByteUnit.BYTES.toGB(-1_000_000_000L), equalTo(-1L));
        assertThat(ByteUnit.BYTES.toGB(-999_999_999L), equalTo(0L));
        assertThat(ByteUnit.BYTES.toGB(-1L), equalTo(0L));
        assertThat(ByteUnit.BYTES.toGB(0L), equalTo(0L));
        assertThat(ByteUnit.BYTES.toGB(1L), equalTo(0L));
        assertThat(ByteUnit.BYTES.toGB(999_999_999L), equalTo(0L));
        assertThat(ByteUnit.BYTES.toGB(1_000_000_000L), equalTo(1L));
        assertThat(ByteUnit.BYTES.toGB(Long.MAX_VALUE), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.BYTES.toGB(-1_000_000_000f), equalTo(-1f));
        assertThat(ByteUnit.BYTES.toGB(-999_999_999f), equalTo(-0.999_999_999f));
        assertThat(ByteUnit.BYTES.toGB(-1f), equalTo(-0.000_000_001f));
        assertThat(ByteUnit.BYTES.toGB(0f), equalTo(0f));
        assertThat(ByteUnit.BYTES.toGB(1f), equalTo(0.000_000_001f));
        assertThat(ByteUnit.BYTES.toGB(999_999_999f), equalTo(0.999_999_999f));
        assertThat(ByteUnit.BYTES.toGB(1_000_000_000f), equalTo(1f));

        assertThat(ByteUnit.BYTES.toGB(-1_000_000_000d), equalTo(-1d));
        assertThat(ByteUnit.BYTES.toGB(-999_999_999d), equalTo(-0.999_999_999d));
        assertThat(ByteUnit.BYTES.toGB(-1d), equalTo(-0.000_000_001d));
        assertThat(ByteUnit.BYTES.toGB(0d), equalTo(0d));
        assertThat(ByteUnit.BYTES.toGB(1d), equalTo(0.000_000_001d));
        assertThat(ByteUnit.BYTES.toGB(999_999_999d), equalTo(0.999_999_999d));
        assertThat(ByteUnit.BYTES.toGB(1_000_000_000d), equalTo(1d));
    }

    @Test
    public void shouldConvertFromBytesToGibi()
    {
        assertThat(ByteUnit.BYTES.toGiB(Integer.MIN_VALUE), equalTo(-2));
        assertThat(ByteUnit.BYTES.toGiB(Integer.MIN_VALUE+1), equalTo(-1));
        assertThat(ByteUnit.BYTES.toGiB(-1_073_741_824), equalTo(-1));
        assertThat(ByteUnit.BYTES.toGiB(-1_073_741_823), equalTo(0));
        assertThat(ByteUnit.BYTES.toGiB(-1), equalTo(0));
        assertThat(ByteUnit.BYTES.toGiB(0), equalTo(0));
        assertThat(ByteUnit.BYTES.toGiB(1), equalTo(0));
        assertThat(ByteUnit.BYTES.toGiB(1_073_741_823), equalTo(0));
        assertThat(ByteUnit.BYTES.toGiB(1_073_741_824), equalTo(1));
        assertThat(ByteUnit.BYTES.toGiB(Integer.MAX_VALUE-1), equalTo(1));
        assertThat(ByteUnit.BYTES.toGiB(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.BYTES.toGiB(Long.MIN_VALUE), equalTo(-8589934592L));
        assertThat(ByteUnit.BYTES.toGiB(-1_073_741_824L), equalTo(-1L));
        assertThat(ByteUnit.BYTES.toGiB(-1_073_741_823L), equalTo(0L));
        assertThat(ByteUnit.BYTES.toGiB(-1L), equalTo(0L));
        assertThat(ByteUnit.BYTES.toGiB(0L), equalTo(0L));
        assertThat(ByteUnit.BYTES.toGiB(1L), equalTo(0L));
        assertThat(ByteUnit.BYTES.toGiB(1_073_741_823L), equalTo(0L));
        assertThat(ByteUnit.BYTES.toGiB(1_073_741_824L), equalTo(1L));
        assertThat(ByteUnit.BYTES.toGiB(2_147_483_648L), equalTo(2L));
        assertThat(ByteUnit.BYTES.toGiB(Long.MAX_VALUE), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.BYTES.toGiB(-1_073_741_824f), equalTo(-1f));
        assertThat(ByteUnit.BYTES.toGiB(-1_073_741_823f), equalTo(-0.99999999906f));
        assertThat(ByteUnit.BYTES.toGiB(-1f), equalTo(-9.3132257461e-10f));
        assertThat(ByteUnit.BYTES.toGiB(0f), equalTo(0f));
        assertThat(ByteUnit.BYTES.toGiB(1f), equalTo(9.3132257461e-10f));
        assertThat(ByteUnit.BYTES.toGiB(1_073_741_823f), equalTo(0.99999999906f));
        assertThat(ByteUnit.BYTES.toGiB(1_073_741_824f), equalTo(1f));

        assertThat(ByteUnit.BYTES.toGiB(-1_073_741_824d), equalTo(-1d));
        assertThat(ByteUnit.BYTES.toGiB(-1_073_741_823d), equalTo(-0.9999999990686774d));
        assertThat(ByteUnit.BYTES.toGiB(-1d), equalTo(-9.313225746154785e-10d));
        assertThat(ByteUnit.BYTES.toGiB(0d), equalTo(0d));
        assertThat(ByteUnit.BYTES.toGiB(1d), equalTo(9.313225746154785e-10d));
        assertThat(ByteUnit.BYTES.toGiB(1_073_741_823d), equalTo(0.9999999990686774d));
        assertThat(ByteUnit.BYTES.toGiB(1_073_741_824d), equalTo(1d));
    }

    @Test
    public void shouldConvertFromBytesToTera()
    {
        assertThat(ByteUnit.BYTES.toTB(Integer.MIN_VALUE), equalTo(0));
        assertThat(ByteUnit.BYTES.toTB(Integer.MIN_VALUE+1), equalTo(0));
        assertThat(ByteUnit.BYTES.toTB(0), equalTo(0));
        assertThat(ByteUnit.BYTES.toTB(Integer.MAX_VALUE-1), equalTo(0));
        assertThat(ByteUnit.BYTES.toTB(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.BYTES.toTB(Long.MIN_VALUE), equalTo(-9223372L));
        assertThat(ByteUnit.BYTES.toTB(-1_000_000_000_000L), equalTo(-1L));
        assertThat(ByteUnit.BYTES.toTB(-999_999_999_999L), equalTo(0L));
        assertThat(ByteUnit.BYTES.toTB(-1L), equalTo(0L));
        assertThat(ByteUnit.BYTES.toTB(0L), equalTo(0L));
        assertThat(ByteUnit.BYTES.toTB(1L), equalTo(0L));
        assertThat(ByteUnit.BYTES.toTB(999_999_999_999L), equalTo(0L));
        assertThat(ByteUnit.BYTES.toTB(1_000_000_000_000L), equalTo(1L));
        assertThat(ByteUnit.BYTES.toTB(Long.MAX_VALUE), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.BYTES.toTB(-1_000_000_000_000f), equalTo(-1f));
        assertThat(ByteUnit.BYTES.toTB(-999_999_999_999f), equalTo(-0.999_999_999_999f));
        assertThat(ByteUnit.BYTES.toTB(-1f), equalTo(-0.000_000_000_001f));
        assertThat(ByteUnit.BYTES.toTB(0f), equalTo(0f));
        assertThat(ByteUnit.BYTES.toTB(1f), equalTo(0.000_000_000_001f));
        assertThat(ByteUnit.BYTES.toTB(999_999_999_999f), equalTo(0.999_999_999_999f));
        assertThat(ByteUnit.BYTES.toTB(1_000_000_000_000f), equalTo(1f));

        assertThat(ByteUnit.BYTES.toTB(-1_000_000_000_000d), equalTo(-1d));
        assertThat(ByteUnit.BYTES.toTB(-999_999_999_999d), equalTo(-0.999_999_999_999d));
        assertThat(ByteUnit.BYTES.toTB(-1d), equalTo(-0.000_000_000_001d));
        assertThat(ByteUnit.BYTES.toTB(0d), equalTo(0d));
        assertThat(ByteUnit.BYTES.toTB(1d), equalTo(0.000_000_000_001d));
        assertThat(ByteUnit.BYTES.toTB(999_999_999_999d), equalTo(0.999_999_999_999d));
        assertThat(ByteUnit.BYTES.toTB(1_000_000_000_000d), equalTo(1d));
    }

    @Test
    public void shouldConvertFromBytesToTebi()
    {
        assertThat(ByteUnit.BYTES.toTiB(Integer.MIN_VALUE), equalTo(0));
        assertThat(ByteUnit.BYTES.toTiB(Integer.MIN_VALUE+1), equalTo(0));
        assertThat(ByteUnit.BYTES.toTiB(0), equalTo(0));
        assertThat(ByteUnit.BYTES.toTiB(Integer.MAX_VALUE-1), equalTo(0));
        assertThat(ByteUnit.BYTES.toTiB(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.BYTES.toTiB(Long.MIN_VALUE), equalTo(-8388608L));
        assertThat(ByteUnit.BYTES.toTiB(-1_099_511_627_776L), equalTo(-1L));
        assertThat(ByteUnit.BYTES.toTiB(-1_099_511_627_775L), equalTo(0L));
        assertThat(ByteUnit.BYTES.toTiB(-1L), equalTo(0L));
        assertThat(ByteUnit.BYTES.toTiB(0L), equalTo(0L));
        assertThat(ByteUnit.BYTES.toTiB(1L), equalTo(0L));
        assertThat(ByteUnit.BYTES.toTiB(1_099_511_627_775L), equalTo(0L));
        assertThat(ByteUnit.BYTES.toTiB(1_099_511_627_776L), equalTo(1L));
        assertThat(ByteUnit.BYTES.toTiB(Long.MAX_VALUE), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.BYTES.toTiB(-1_099_511_627_776f), equalTo(-1f));
        assertThat(ByteUnit.BYTES.toTiB(-1_099_511_627_775f), equalTo(-1f)); // limits of single precision
        assertThat(ByteUnit.BYTES.toTiB(-1f), equalTo(-9.094947e-13f));
        assertThat(ByteUnit.BYTES.toTiB(0f), equalTo(0f));
        assertThat(ByteUnit.BYTES.toTiB(1f), equalTo(9.094947e-13f));
        assertThat(ByteUnit.BYTES.toTiB(1_099_511_627_775f), equalTo(1f)); // limits of single precision
        assertThat(ByteUnit.BYTES.toTiB(1_099_511_627_776f), equalTo(1f));

        assertThat(ByteUnit.BYTES.toTiB(-1_099_511_627_776d), equalTo(-1d));
        assertThat(ByteUnit.BYTES.toTiB(-1_099_511_627_775d), equalTo(-0.9999999999990905d));
        assertThat(ByteUnit.BYTES.toTiB(-1d), equalTo(-9.094947017729282e-13d));
        assertThat(ByteUnit.BYTES.toTiB(0d), equalTo(0d));
        assertThat(ByteUnit.BYTES.toTiB(1d), equalTo(9.094947017729282e-13d));
        assertThat(ByteUnit.BYTES.toTiB(1_099_511_627_775d), equalTo(0.9999999999990905d));
        assertThat(ByteUnit.BYTES.toTiB(1_099_511_627_776d), equalTo(1d));
    }

    @Test
    public void shouldConvertFromBytesToPeta()
    {
        assertThat(ByteUnit.BYTES.toPB(Integer.MIN_VALUE), equalTo(0));
        assertThat(ByteUnit.BYTES.toPB(Integer.MIN_VALUE+1), equalTo(0));
        assertThat(ByteUnit.BYTES.toPB(0), equalTo(0));
        assertThat(ByteUnit.BYTES.toPB(Integer.MAX_VALUE-1), equalTo(0));
        assertThat(ByteUnit.BYTES.toPB(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.BYTES.toPB(Long.MIN_VALUE), equalTo(-9223L));
        assertThat(ByteUnit.BYTES.toPB(-1_000_000_000_000_000L), equalTo(-1L));
        assertThat(ByteUnit.BYTES.toPB(-999_999_999_999_999L), equalTo(0L));
        assertThat(ByteUnit.BYTES.toPB(-1L), equalTo(0L));
        assertThat(ByteUnit.BYTES.toPB(0L), equalTo(0L));
        assertThat(ByteUnit.BYTES.toPB(1L), equalTo(0L));
        assertThat(ByteUnit.BYTES.toPB(999_999_999_999_999L), equalTo(0L));
        assertThat(ByteUnit.BYTES.toPB(1_000_000_000_000_000L), equalTo(1L));
        assertThat(ByteUnit.BYTES.toPB(Long.MAX_VALUE), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.BYTES.toPB(-1_000_000_000_000_000f), equalTo(-1f));
        assertThat(ByteUnit.BYTES.toPB(-999_999_999_999_999f), equalTo(-1f)); // Limits of single precision
        assertThat(ByteUnit.BYTES.toPB(-1f), equalTo(-0.000_000_000_000_001f));
        assertThat(ByteUnit.BYTES.toPB(0f), equalTo(0f));
        assertThat(ByteUnit.BYTES.toPB(1f), equalTo(0.000_000_000_000_001f));
        assertThat(ByteUnit.BYTES.toPB(999_999_999_999_999f), equalTo(1f));
        assertThat(ByteUnit.BYTES.toPB(1_000_000_000_000_000f), equalTo(1f));

        assertThat(ByteUnit.BYTES.toPB(-1_000_000_000_000_000d), equalTo(-1d));
        assertThat(ByteUnit.BYTES.toPB(-999_999_999_999_999d), equalTo(-0.999_999_999_999_999d));
        assertThat(ByteUnit.BYTES.toPB(-1d), equalTo(-0.000_000_000_000_001d));
        assertThat(ByteUnit.BYTES.toPB(0d), equalTo(0d));
        assertThat(ByteUnit.BYTES.toPB(1d), equalTo(0.000_000_000_000_001d));
        assertThat(ByteUnit.BYTES.toPB(999_999_999_999_999d), equalTo(0.999_999_999_999_999d));
        assertThat(ByteUnit.BYTES.toPB(1_000_000_000_000_000d), equalTo(1d));
    }

    @Test
    public void shouldConvertFromBytesToPebi()
    {
        assertThat(ByteUnit.BYTES.toPiB(Integer.MIN_VALUE), equalTo(0));
        assertThat(ByteUnit.BYTES.toPiB(Integer.MIN_VALUE+1), equalTo(0));
        assertThat(ByteUnit.BYTES.toPiB(0), equalTo(0));
        assertThat(ByteUnit.BYTES.toPiB(Integer.MAX_VALUE-1), equalTo(0));
        assertThat(ByteUnit.BYTES.toPiB(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.BYTES.toPiB(Long.MIN_VALUE), equalTo(-8192L));
        assertThat(ByteUnit.BYTES.toPiB(-1_125_899_906_842_624L), equalTo(-1L));
        assertThat(ByteUnit.BYTES.toPiB(-1_125_899_906_842_623L), equalTo(0L));
        assertThat(ByteUnit.BYTES.toPiB(-1L), equalTo(0L));
        assertThat(ByteUnit.BYTES.toPiB(0L), equalTo(0L));
        assertThat(ByteUnit.BYTES.toPiB(1L), equalTo(0L));
        assertThat(ByteUnit.BYTES.toPiB(1_125_899_906_842_623L), equalTo(0L));
        assertThat(ByteUnit.BYTES.toPiB(1_125_899_906_842_624L), equalTo(1L));
        assertThat(ByteUnit.BYTES.toPiB(Long.MAX_VALUE), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.BYTES.toPiB(-1_125_899_906_842_624f), equalTo(-1f));
        assertThat(ByteUnit.BYTES.toPiB(-1_125_899_906_842_623f), equalTo(-1f)); // limits of single precision
        assertThat(ByteUnit.BYTES.toPiB(-1f), equalTo(-8.8817842e-16f));
        assertThat(ByteUnit.BYTES.toPiB(0f), equalTo(0f));
        assertThat(ByteUnit.BYTES.toPiB(1f), equalTo(8.8817842e-16f));
        assertThat(ByteUnit.BYTES.toPiB(1_125_899_906_842_623f), equalTo(1f)); // limits of single precision
        assertThat(ByteUnit.BYTES.toPiB(1_125_899_906_842_624f), equalTo(1f));

        assertThat(ByteUnit.BYTES.toPiB(-1_125_899_906_842_624d), equalTo(-1d));
        assertThat(ByteUnit.BYTES.toPiB(-1_125_899_906_842_623d), equalTo(-0.9999999999999991d));
        assertThat(ByteUnit.BYTES.toPiB(-1d), equalTo(-8.881784197001252e-16d));
        assertThat(ByteUnit.BYTES.toPiB(0d), equalTo(0d));
        assertThat(ByteUnit.BYTES.toPiB(1d), equalTo(8.881784197001252e-16d));
        assertThat(ByteUnit.BYTES.toPiB(1_125_899_906_842_623d), equalTo(0.9999999999999991d));
        assertThat(ByteUnit.BYTES.toPiB(1_125_899_906_842_624d), equalTo(1d));
    }

    @Test
    public void shouldYieldPrefixOfKilo()
    {
        assertThat(ByteUnit.Type.DECIMAL.unitsOf(-999_999L), is(ByteUnit.KB));
        assertThat(ByteUnit.Type.DECIMAL.unitsOf(-1_000L), is(ByteUnit.KB));
        assertThat(ByteUnit.Type.DECIMAL.unitsOf(1_000L), is(ByteUnit.KB));
        assertThat(ByteUnit.Type.DECIMAL.unitsOf(999_999L), is(ByteUnit.KB));

        assertThat(ByteUnit.Type.DECIMAL.unitsOf(-999_999), is(ByteUnit.KB));
        assertThat(ByteUnit.Type.DECIMAL.unitsOf(-1_000), is(ByteUnit.KB));
        assertThat(ByteUnit.Type.DECIMAL.unitsOf(1_000), is(ByteUnit.KB));
        assertThat(ByteUnit.Type.DECIMAL.unitsOf(999_999), is(ByteUnit.KB));
    }

    @Test
    public void shouldYieldExactUnitsOfKilo()
    {
        assertThat(ByteUnit.Type.DECIMAL.exactUnitsOf(-1_000_000_000_001_000L), is(ByteUnit.KB));
        assertThat(ByteUnit.Type.DECIMAL.exactUnitsOf(-1_000_000_001_000L), is(ByteUnit.KB));
        assertThat(ByteUnit.Type.DECIMAL.exactUnitsOf(-1_000_001_000L), is(ByteUnit.KB));
        assertThat(ByteUnit.Type.DECIMAL.exactUnitsOf(-1_001_000L), is(ByteUnit.KB));
        assertThat(ByteUnit.Type.DECIMAL.exactUnitsOf(-2_000L), is(ByteUnit.KB));
        assertThat(ByteUnit.Type.DECIMAL.exactUnitsOf(-1_000L), is(ByteUnit.KB));
        assertThat(ByteUnit.Type.DECIMAL.exactUnitsOf(1_000L), is(ByteUnit.KB));
        assertThat(ByteUnit.Type.DECIMAL.exactUnitsOf(2_000L), is(ByteUnit.KB));
        assertThat(ByteUnit.Type.DECIMAL.exactUnitsOf(1_001_000L), is(ByteUnit.KB));
        assertThat(ByteUnit.Type.DECIMAL.exactUnitsOf(1_000_001_000L), is(ByteUnit.KB));
        assertThat(ByteUnit.Type.DECIMAL.exactUnitsOf(1_000_000_001_000L), is(ByteUnit.KB));
        assertThat(ByteUnit.Type.DECIMAL.exactUnitsOf(1_000_000_000_001_000L), is(ByteUnit.KB));
    }

    @Test
    public void shouldClaimKiloType()
    {
        assertThat(ByteUnit.KB.hasType(ByteUnit.Type.BINARY), equalTo(false));
        assertThat(ByteUnit.KB.hasType(ByteUnit.Type.DECIMAL), equalTo(true));
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertToKilo()
    {
        ByteUnit.KB.convert(1, ByteUnit.PB);
    }

    @Test
    public void shouldConvertToKilo()
    {
        assertThat(ByteUnit.KB.convert(5_000L, ByteUnit.BYTES), equalTo(5L));
        assertThat(ByteUnit.KB.convert(5L, ByteUnit.KB), equalTo(5L));
        assertThat(ByteUnit.KB.convert(1L, ByteUnit.MB), equalTo(1_000L));
        assertThat(ByteUnit.KB.convert(1L, ByteUnit.GB), equalTo(1_000_000L));
        assertThat(ByteUnit.KB.convert(1L, ByteUnit.TB), equalTo(1_000_000_000L));
        assertThat(ByteUnit.KB.convert(1L, ByteUnit.PB), equalTo(1_000_000_000_000L));

        assertThat(ByteUnit.KB.convert(5_000, ByteUnit.BYTES), equalTo(5));
        assertThat(ByteUnit.KB.convert(5, ByteUnit.KB), equalTo(5));
        assertThat(ByteUnit.KB.convert(1, ByteUnit.MB), equalTo(1_000));
        assertThat(ByteUnit.KB.convert(1, ByteUnit.GB), equalTo(1_000_000));
        assertThat(ByteUnit.KB.convert(1, ByteUnit.TB), equalTo(1_000_000_000));

        assertThat(ByteUnit.KB.convert(1_000d, ByteUnit.BYTES), equalTo(1d));
        assertThat(ByteUnit.KB.convert(1d, ByteUnit.KB), equalTo(1d));
        assertThat(ByteUnit.KB.convert(1d, ByteUnit.MB), equalTo(1_000d));
        assertThat(ByteUnit.KB.convert(1d, ByteUnit.GB), equalTo(1_000_000d));
        assertThat(ByteUnit.KB.convert(1d, ByteUnit.TB), equalTo(1_000_000_000d));
        assertThat(ByteUnit.KB.convert(1d, ByteUnit.PB), equalTo(1_000_000_000_000d));
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertKiloToBytes1()
    {
        ByteUnit.KB.toBytes(Integer.MIN_VALUE+1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertKiloToBytes2()
    {
        ByteUnit.KB.toBytes(-2_147_484);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertKiloToBytes3()
    {
        ByteUnit.KB.toBytes(2_147_484);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertKiloToBytes4()
    {
        ByteUnit.KB.toBytes(Integer.MAX_VALUE-1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertKiloToBytes5()
    {
        ByteUnit.KB.toBytes(Long.MIN_VALUE+1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertKiloToBytes6()
    {
        ByteUnit.KB.toBytes(-9_223_372_036_854_776L);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertKiloToBytes7()
    {
        ByteUnit.KB.toBytes(9_223_372_036_854_776L);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertKiloToBytes8()
    {
        ByteUnit.KB.toBytes(Long.MAX_VALUE-1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertKiloToBytes9()
    {
        ByteUnit.KB.toBytes(Integer.MIN_VALUE);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertKiloToBytes10()
    {
        ByteUnit.KB.toBytes(Long.MIN_VALUE);
    }

    @Test
    public void shouldConvertKiloToBytes()
    {
        assertThat(ByteUnit.KB.toBytes(-2_147_483), equalTo(-2_147_483_000));
        assertThat(ByteUnit.KB.toBytes(-1), equalTo(-1_000));
        assertThat(ByteUnit.KB.toBytes(0), equalTo(0));
        assertThat(ByteUnit.KB.toBytes(1), equalTo(1_000));
        assertThat(ByteUnit.KB.toBytes(2_147_483), equalTo(2_147_483_000));
        assertThat(ByteUnit.KB.toBytes(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.KB.toBytes(-9_223_372_036_854_775L), equalTo(-9_223_372_036_854_775_000L));
        assertThat(ByteUnit.KB.toBytes(-1L), equalTo(-1_000L));
        assertThat(ByteUnit.KB.toBytes(0L), equalTo(0L));
        assertThat(ByteUnit.KB.toBytes(1L), equalTo(1_000L));
        assertThat(ByteUnit.KB.toBytes(9_223_372_036_854_775L), equalTo(9_223_372_036_854_775_000L));
        assertThat(ByteUnit.KB.toBytes(Long.MAX_VALUE), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.KB.toBytes(-1f), equalTo(-1_000f));
        assertThat(ByteUnit.KB.toBytes(0f), equalTo(0f));
        assertThat(ByteUnit.KB.toBytes(1f), equalTo(1_000f));

        assertThat(ByteUnit.KB.toBytes(-1d), equalTo(-1_000d));
        assertThat(ByteUnit.KB.toBytes(0d), equalTo(0d));
        assertThat(ByteUnit.KB.toBytes(1d), equalTo(1_000d));
    }

    @Test
    public void shouldConvertKiloToKilo()
    {
        assertThat(ByteUnit.KB.toKB(Integer.MIN_VALUE), equalTo(Integer.MIN_VALUE));
        assertThat(ByteUnit.KB.toKB(-1), equalTo(-1));
        assertThat(ByteUnit.KB.toKB(0), equalTo(0));
        assertThat(ByteUnit.KB.toKB(1), equalTo(1));
        assertThat(ByteUnit.KB.toKB(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.KB.toKB(Long.MIN_VALUE), equalTo(Long.MIN_VALUE));
        assertThat(ByteUnit.KB.toKB(-1L), equalTo(-1L));
        assertThat(ByteUnit.KB.toKB(0L), equalTo(0L));
        assertThat(ByteUnit.KB.toKB(1L), equalTo(1L));
        assertThat(ByteUnit.KB.toKB(Long.MAX_VALUE), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.KB.toKB(-1f), equalTo(-1f));
        assertThat(ByteUnit.KB.toKB(0f), equalTo(0f));
        assertThat(ByteUnit.KB.toKB(1f), equalTo(1f));

        assertThat(ByteUnit.KB.toKB(-1d), equalTo(-1d));
        assertThat(ByteUnit.KB.toKB(0d), equalTo(0d));
        assertThat(ByteUnit.KB.toKB(1d), equalTo(1d));
    }

    @Test
    public void shouldConvertKiloToKibi()
    {
        assertThat(ByteUnit.KB.toKiB(Integer.MIN_VALUE), equalTo(-2097152000));
        assertThat(ByteUnit.KB.toKiB(-1024), equalTo(-1_000));
        assertThat(ByteUnit.KB.toKiB(-1), equalTo(0));
        assertThat(ByteUnit.KB.toKiB(0), equalTo(0));
        assertThat(ByteUnit.KB.toKiB(1), equalTo(0));
        assertThat(ByteUnit.KB.toKiB(1024), equalTo(1_000));
        assertThat(ByteUnit.KB.toKiB(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.KB.toKiB(-1024L), equalTo(-1_000L));
        assertThat(ByteUnit.KB.toKiB(-1L), equalTo(0L));
        assertThat(ByteUnit.KB.toKiB(0L), equalTo(0L));
        assertThat(ByteUnit.KB.toKiB(1L), equalTo(0L));
        assertThat(ByteUnit.KB.toKiB(1024L), equalTo(1_000L));
        assertThat(ByteUnit.KB.toKiB(Long.MAX_VALUE), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.KB.toKiB(-1f), equalTo(-0.9765625f));
        assertThat(ByteUnit.KB.toKiB(0f), equalTo(0f));
        assertThat(ByteUnit.KB.toKiB(1f), equalTo(0.9765625f));

        assertThat(ByteUnit.KB.toKiB(-1d), equalTo(-0.9765625d));
        assertThat(ByteUnit.KB.toKiB(0d), equalTo(0d));
        assertThat(ByteUnit.KB.toKiB(1d), equalTo(0.9765625d));
    }

    @Test
    public void shouldConvertKiloToMega()
    {
        assertThat(ByteUnit.KB.toMB(Integer.MIN_VALUE), equalTo(-2147483));
        assertThat(ByteUnit.KB.toMB(-1_000), equalTo(-1));
        assertThat(ByteUnit.KB.toMB(-999), equalTo(0));
        assertThat(ByteUnit.KB.toMB(0), equalTo(0));
        assertThat(ByteUnit.KB.toMB(999), equalTo(0));
        assertThat(ByteUnit.KB.toMB(1_000), equalTo(1));
        assertThat(ByteUnit.KB.toMB(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.KB.toMB(Long.MIN_VALUE), equalTo(-9223372036854775L));
        assertThat(ByteUnit.KB.toMB(-1_000L), equalTo(-1L));
        assertThat(ByteUnit.KB.toMB(-999L), equalTo(0L));
        assertThat(ByteUnit.KB.toMB(0L), equalTo(0L));
        assertThat(ByteUnit.KB.toMB(999L), equalTo(0L));
        assertThat(ByteUnit.KB.toMB(1_000L), equalTo(1L));
        assertThat(ByteUnit.KB.toMB(Long.MAX_VALUE), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.KB.toMB(-1f), equalTo(-0.001f));
        assertThat(ByteUnit.KB.toMB(0f), equalTo(0f));
        assertThat(ByteUnit.KB.toMB(1f), equalTo(0.001f));

        assertThat(ByteUnit.KB.toMB(-1d), equalTo(-0.001d));
        assertThat(ByteUnit.KB.toMB(0d), equalTo(0d));
        assertThat(ByteUnit.KB.toMB(1d), equalTo(0.001d));
    }

    @Test
    public void shouldConvertKiloToGiga()
    {
        assertThat(ByteUnit.KB.toGB(Integer.MIN_VALUE), equalTo(-2147));
        assertThat(ByteUnit.KB.toGB(-1_000_000), equalTo(-1));
        assertThat(ByteUnit.KB.toGB(-999_999), equalTo(0));
        assertThat(ByteUnit.KB.toGB(0), equalTo(0));
        assertThat(ByteUnit.KB.toGB(999_999), equalTo(0));
        assertThat(ByteUnit.KB.toGB(1_000_000), equalTo(1));
        assertThat(ByteUnit.KB.toGB(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.KB.toGB(Long.MIN_VALUE), equalTo(-9223372036854L));
        assertThat(ByteUnit.KB.toGB(-1_000_000L), equalTo(-1L));
        assertThat(ByteUnit.KB.toGB(-999_999L), equalTo(0L));
        assertThat(ByteUnit.KB.toGB(0L), equalTo(0L));
        assertThat(ByteUnit.KB.toGB(999_999L), equalTo(0L));
        assertThat(ByteUnit.KB.toGB(1_000_000L), equalTo(1L));
        assertThat(ByteUnit.KB.toGB(Long.MAX_VALUE), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.KB.toGB(-1f), equalTo(-0.000_001f));
        assertThat(ByteUnit.KB.toGB(0f), equalTo(0f));
        assertThat(ByteUnit.KB.toGB(1f), equalTo(0.000_001f));

        assertThat(ByteUnit.KB.toGB(-1d), equalTo(-0.000_001d));
        assertThat(ByteUnit.KB.toGB(0d), equalTo(0d));
        assertThat(ByteUnit.KB.toGB(1d), equalTo(0.000_001d));
    }

    @Test
    public void shouldConvertKiloToGibi()
    {
        assertThat(ByteUnit.KB.toGiB(Integer.MIN_VALUE), equalTo(-2_000));
        assertThat(ByteUnit.KB.toGiB(-1_073_741_824), equalTo(-1_000));
        assertThat(ByteUnit.KB.toGiB(-1), equalTo(0));
        assertThat(ByteUnit.KB.toGiB(0), equalTo(0));
        assertThat(ByteUnit.KB.toGiB(1), equalTo(0));
        assertThat(ByteUnit.KB.toGiB(1_073_741_824), equalTo(1_000));
        assertThat(ByteUnit.KB.toGiB(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.KB.toGiB(-1_073_741_824L), equalTo(-1_000L));
        assertThat(ByteUnit.KB.toGiB(-1L), equalTo(0L));
        assertThat(ByteUnit.KB.toGiB(0L), equalTo(0L));
        assertThat(ByteUnit.KB.toGiB(1L), equalTo(0L));
        assertThat(ByteUnit.KB.toGiB(1_073_741_824L), equalTo(1_000L));
        assertThat(ByteUnit.KB.toGiB(Long.MAX_VALUE), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.KB.toGiB(-1f), equalTo(-0.000_000_931_322_57f));
        assertThat(ByteUnit.KB.toGiB(0f), equalTo(0f));
        assertThat(ByteUnit.KB.toGiB(1f), equalTo(0.000_000_931_322_57f));

        assertThat(ByteUnit.KB.toGiB(-1d), equalTo(-0.000_000_931_322_574_615_478_5d));
        assertThat(ByteUnit.KB.toGiB(0d), equalTo(0d));
        assertThat(ByteUnit.KB.toGiB(1d), equalTo(0.000_000_931_322_574_615_478_5d));
    }

    @Test
    public void shouldConvertKiloToTera()
    {
        assertThat(ByteUnit.KB.toTB(Integer.MIN_VALUE), equalTo(-2));
        assertThat(ByteUnit.KB.toTB(-1_000_000_000), equalTo(-1));
        assertThat(ByteUnit.KB.toTB(-999_999_999), equalTo(0));
        assertThat(ByteUnit.KB.toTB(0), equalTo(0));
        assertThat(ByteUnit.KB.toTB(999_999_999), equalTo(0));
        assertThat(ByteUnit.KB.toTB(1_000_000_000), equalTo(1));
        assertThat(ByteUnit.KB.toTB(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.KB.toTB(Long.MIN_VALUE), equalTo(-9223372036L));
        assertThat(ByteUnit.KB.toTB(-1_000_000_000L), equalTo(-1L));
        assertThat(ByteUnit.KB.toTB(-999_999_999L), equalTo(0L));
        assertThat(ByteUnit.KB.toTB(0L), equalTo(0L));
        assertThat(ByteUnit.KB.toTB(999_999_999L), equalTo(0L));
        assertThat(ByteUnit.KB.toTB(1_000_000_000L), equalTo(1L));
        assertThat(ByteUnit.KB.toTB(Long.MAX_VALUE), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.KB.toTB(-1f), equalTo(-0.000_000_001f));
        assertThat(ByteUnit.KB.toTB(0f), equalTo(0f));
        assertThat(ByteUnit.KB.toTB(1f), equalTo(0.000_000_001f));

        assertThat(ByteUnit.KB.toTB(-1d), equalTo(-0.000_000_001d));
        assertThat(ByteUnit.KB.toTB(0d), equalTo(0d));
        assertThat(ByteUnit.KB.toTB(1d), equalTo(0.000_000_001d));
    }

    @Test
    public void shouldConvertKiloToTebi()
    {
        assertThat(ByteUnit.KB.toTiB(Integer.MIN_VALUE), equalTo(-1));
        assertThat(ByteUnit.KB.toTiB(-1099511628), equalTo(-1));
        assertThat(ByteUnit.KB.toTiB(-1099511627), equalTo(0));
        assertThat(ByteUnit.KB.toTiB(0), equalTo(0));
        assertThat(ByteUnit.KB.toTiB(1099511627), equalTo(0));
        assertThat(ByteUnit.KB.toTiB(1099511628), equalTo(1));
        assertThat(ByteUnit.KB.toTiB(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.KB.toTiB(-1_099_511_627_776L), equalTo(-1_000L));
        assertThat(ByteUnit.KB.toTiB(-1099511628L), equalTo(-1L));
        assertThat(ByteUnit.KB.toTiB(-1099511627L), equalTo(0L));
        assertThat(ByteUnit.KB.toTiB(0L), equalTo(0L));
        assertThat(ByteUnit.KB.toTiB(1099511627L), equalTo(0L));
        assertThat(ByteUnit.KB.toTiB(1099511628L), equalTo(1L));
        assertThat(ByteUnit.KB.toTiB(1_099_511_627_776L), equalTo(1_000L));
        assertThat(ByteUnit.KB.toTiB(Long.MAX_VALUE), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.KB.toTiB(-1f), equalTo(-9.094947e-10f));
        assertThat(ByteUnit.KB.toTiB(0f), equalTo(0f));
        assertThat(ByteUnit.KB.toTiB(1f), equalTo(9.094947e-10f));

        assertThat(ByteUnit.KB.toTiB(-1d), equalTo(-9.094947017729282e-10d));
        assertThat(ByteUnit.KB.toTiB(0d), equalTo(0d));
        assertThat(ByteUnit.KB.toTiB(1d), equalTo(9.094947017729282e-10d));
    }

    @Test
    public void shouldConvertKiloToPeta()
    {
        assertThat(ByteUnit.KB.toPB(Integer.MIN_VALUE), equalTo(0));
        assertThat(ByteUnit.KB.toPB(Integer.MIN_VALUE+1), equalTo(0));
        assertThat(ByteUnit.KB.toPB(0), equalTo(0));
        assertThat(ByteUnit.KB.toPB(Integer.MAX_VALUE-1), equalTo(0));
        assertThat(ByteUnit.KB.toPB(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.KB.toPB(Long.MIN_VALUE), equalTo(-9223372L));
        assertThat(ByteUnit.KB.toPB(-1_000_000_000_000L), equalTo(-1L));
        assertThat(ByteUnit.KB.toPB(-999_999_999_999L), equalTo(0L));
        assertThat(ByteUnit.KB.toPB(0L), equalTo(0L));
        assertThat(ByteUnit.KB.toPB(999_999_999_999L), equalTo(0L));
        assertThat(ByteUnit.KB.toPB(1_000_000_000_000L), equalTo(1L));
        assertThat(ByteUnit.KB.toPB(Long.MAX_VALUE), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.KB.toPB(-1f), equalTo(-0.000_000_000_001f));
        assertThat(ByteUnit.KB.toPB(0f), equalTo(0f));
        assertThat(ByteUnit.KB.toPB(1f), equalTo(0.000_000_000_001f));

        assertThat(ByteUnit.KB.toPB(-1d), equalTo(-0.000_000_000_001d));
        assertThat(ByteUnit.KB.toPB(0d), equalTo(0d));
        assertThat(ByteUnit.KB.toPB(1d), equalTo(0.000_000_000_001d));
    }

    @Test
    public void shouldConvertKiloToPebi()
    {
        assertThat(ByteUnit.KB.toPiB(Integer.MIN_VALUE), equalTo(0));
        assertThat(ByteUnit.KB.toPiB(Integer.MIN_VALUE+1), equalTo(0));
        assertThat(ByteUnit.KB.toPiB(0), equalTo(0));
        assertThat(ByteUnit.KB.toPiB(Integer.MAX_VALUE-1), equalTo(0));
        assertThat(ByteUnit.KB.toPiB(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.KB.toPiB(-1_125_899_906_842_624L), equalTo(-1_000L));
        assertThat(ByteUnit.KB.toPiB(-1L), equalTo(0L));
        assertThat(ByteUnit.KB.toPiB(0L), equalTo(0L));
        assertThat(ByteUnit.KB.toPiB(1L), equalTo(0L));
        assertThat(ByteUnit.KB.toPiB(1_125_899_906_842_624L), equalTo(1_000L));
        assertThat(ByteUnit.KB.toPiB(Long.MAX_VALUE), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.KB.toPiB(-1f), equalTo(-8.8817842e-13f));
        assertThat(ByteUnit.KB.toPiB(0f), equalTo(0f));
        assertThat(ByteUnit.KB.toPiB(1f), equalTo(8.8817842e-13f));

        assertThat(ByteUnit.KB.toPiB(-1d), equalTo(-8.881784197001252e-13d));
        assertThat(ByteUnit.KB.toPiB(0d), equalTo(0d));
        assertThat(ByteUnit.KB.toPiB(1d), equalTo(8.881784197001252e-13d));
    }


    @Test
    public void shouldYieldPrefixOfKibi()
    {
        assertThat(ByteUnit.Type.BINARY.unitsOf(-1048575L), is(ByteUnit.KiB));
        assertThat(ByteUnit.Type.BINARY.unitsOf(-1024L), is(ByteUnit.KiB));
        assertThat(ByteUnit.Type.BINARY.unitsOf(1024L), is(ByteUnit.KiB));
        assertThat(ByteUnit.Type.BINARY.unitsOf(1048575L), is(ByteUnit.KiB));

        assertThat(ByteUnit.Type.BINARY.unitsOf(-1048575), is(ByteUnit.KiB));
        assertThat(ByteUnit.Type.BINARY.unitsOf(-1024), is(ByteUnit.KiB));
        assertThat(ByteUnit.Type.BINARY.unitsOf(1024), is(ByteUnit.KiB));
        assertThat(ByteUnit.Type.BINARY.unitsOf(1048575), is(ByteUnit.KiB));
    }

    @Test
    public void shouldYieldExactUnitsOfKibi()
    {
        assertThat(ByteUnit.Type.BINARY.exactUnitsOf(-(1024L+1125899906842624L)), is(ByteUnit.KiB));
        assertThat(ByteUnit.Type.BINARY.exactUnitsOf(-(1024L+1099511627776L)), is(ByteUnit.KiB));
        assertThat(ByteUnit.Type.BINARY.exactUnitsOf(-(1024L+1073741824L)), is(ByteUnit.KiB));
        assertThat(ByteUnit.Type.BINARY.exactUnitsOf(-(1024L+1048576L)), is(ByteUnit.KiB));
        assertThat(ByteUnit.Type.BINARY.exactUnitsOf(-2*1024L), is(ByteUnit.KiB));
        assertThat(ByteUnit.Type.BINARY.exactUnitsOf(-1024L), is(ByteUnit.KiB));
        assertThat(ByteUnit.Type.BINARY.exactUnitsOf(1024L), is(ByteUnit.KiB));
        assertThat(ByteUnit.Type.BINARY.exactUnitsOf(2*1024L), is(ByteUnit.KiB));
        assertThat(ByteUnit.Type.BINARY.exactUnitsOf(1024L+1048576L), is(ByteUnit.KiB));
        assertThat(ByteUnit.Type.BINARY.exactUnitsOf(1024L+1073741824L), is(ByteUnit.KiB));
        assertThat(ByteUnit.Type.BINARY.exactUnitsOf(1024L+1099511627776L), is(ByteUnit.KiB));
        assertThat(ByteUnit.Type.BINARY.exactUnitsOf(1024L+1125899906842624L), is(ByteUnit.KiB));
    }

    @Test
    public void shouldClaimKibiType()
    {
        assertThat(ByteUnit.KiB.hasType(ByteUnit.Type.BINARY), equalTo(true));
        assertThat(ByteUnit.KiB.hasType(ByteUnit.Type.DECIMAL), equalTo(false));
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertToKibi()
    {
        ByteUnit.KiB.convert(1, ByteUnit.PiB);
    }

    @Test
    public void shouldConvertToKibi()
    {
        assertThat(ByteUnit.KiB.convert(5L * 1024L, ByteUnit.BYTES), equalTo(5L));
        assertThat(ByteUnit.KiB.convert(5L, ByteUnit.KiB), equalTo(5L));
        assertThat(ByteUnit.KiB.convert(1L, ByteUnit.MiB), equalTo(1024L));
        assertThat(ByteUnit.KiB.convert(1L, ByteUnit.GiB), equalTo(1048576L));
        assertThat(ByteUnit.KiB.convert(1L, ByteUnit.TiB), equalTo(1073741824L));
        assertThat(ByteUnit.KiB.convert(1L, ByteUnit.PiB), equalTo(1099511627776L));

        assertThat(ByteUnit.KiB.convert(5 * 1024, ByteUnit.BYTES), equalTo(5));
        assertThat(ByteUnit.KiB.convert(5, ByteUnit.KiB), equalTo(5));
        assertThat(ByteUnit.KiB.convert(1, ByteUnit.MiB), equalTo(1024));
        assertThat(ByteUnit.KiB.convert(1, ByteUnit.GiB), equalTo(1048576));
        assertThat(ByteUnit.KiB.convert(1, ByteUnit.TiB), equalTo(1073741824));

        assertThat(ByteUnit.KiB.convert(1024d, ByteUnit.BYTES), equalTo(1d));
        assertThat(ByteUnit.KiB.convert(1d, ByteUnit.KiB), equalTo(1d));
        assertThat(ByteUnit.KiB.convert(1d, ByteUnit.MiB), equalTo(1024d));
        assertThat(ByteUnit.KiB.convert(1d, ByteUnit.GiB), equalTo(1048576d));
        assertThat(ByteUnit.KiB.convert(1d, ByteUnit.TiB), equalTo(1073741824d));
        assertThat(ByteUnit.KiB.convert(1d, ByteUnit.PiB), equalTo(1099511627776d));
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertKibiToBytes1()
    {
        ByteUnit.KiB.toBytes(Integer.MIN_VALUE+1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertKibiToBytes3()
    {
        ByteUnit.KiB.toBytes(2_097_152);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertKibiToBytes4()
    {
        ByteUnit.KiB.toBytes(Integer.MAX_VALUE-1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertKibiToBytes5()
    {
        ByteUnit.KiB.toBytes(Long.MIN_VALUE+1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertKibiToBytes7()
    {
        ByteUnit.KiB.toBytes(9_007_199_254_740_992L);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertKibiToBytes8()
    {
        ByteUnit.KiB.toBytes(Long.MAX_VALUE-1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertKibiToBytes9()
    {
        ByteUnit.KiB.toBytes(Integer.MIN_VALUE);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertKibiToBytes10()
    {
        ByteUnit.KiB.toBytes(Long.MIN_VALUE);
    }

    @Test
    public void shouldConvertKibiToBytes()
    {
        assertThat(ByteUnit.KiB.toBytes(-2_097_152), equalTo(Integer.MIN_VALUE));
        assertThat(ByteUnit.KiB.toBytes(-2_097_151), equalTo(-2_147_482_624));
        assertThat(ByteUnit.KiB.toBytes(-1), equalTo(-1024));
        assertThat(ByteUnit.KiB.toBytes(0), equalTo(0));
        assertThat(ByteUnit.KiB.toBytes(1), equalTo(1024));
        assertThat(ByteUnit.KiB.toBytes(2_097_151), equalTo(2_147_482_624));
        assertThat(ByteUnit.KiB.toBytes(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.KiB.toBytes(-9_007_199_254_740_992L), equalTo(Long.MIN_VALUE));
        assertThat(ByteUnit.KiB.toBytes(-9_007_199_254_740_991L), equalTo(-9_223_372_036_854_774_784L));
        assertThat(ByteUnit.KiB.toBytes(-1L), equalTo(-1024L));
        assertThat(ByteUnit.KiB.toBytes(0L), equalTo(0L));
        assertThat(ByteUnit.KiB.toBytes(1L), equalTo(1024L));
        assertThat(ByteUnit.KiB.toBytes(9_007_199_254_740_991L), equalTo(9_223_372_036_854_774_784L));
        assertThat(ByteUnit.KiB.toBytes(Long.MAX_VALUE), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.KiB.toBytes(-1f), equalTo(-1024f));
        assertThat(ByteUnit.KiB.toBytes(0f), equalTo(0f));
        assertThat(ByteUnit.KiB.toBytes(1f), equalTo(1024f));

        assertThat(ByteUnit.KiB.toBytes(-1d), equalTo(-1024d));
        assertThat(ByteUnit.KiB.toBytes(0d), equalTo(0d));
        assertThat(ByteUnit.KiB.toBytes(1d), equalTo(1024d));
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertKibiToKilo1()
    {
        ByteUnit.KiB.toKB(Integer.MIN_VALUE);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertKibiToKilo2()
    {
        ByteUnit.KiB.toKB(Long.MIN_VALUE);
    }

    @Test
    public void shouldConvertKibiToKilo()
    {
        assertThat(ByteUnit.KiB.toKB(-1_000), equalTo(-1024));
        assertThat(ByteUnit.KiB.toKB(0), equalTo(0));
        assertThat(ByteUnit.KiB.toKB(1_000), equalTo(1024));
        assertThat(ByteUnit.KiB.toKB(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.KiB.toKB(-1_000L), equalTo(-1024L));
        assertThat(ByteUnit.KiB.toKB(0L), equalTo(0L));
        assertThat(ByteUnit.KiB.toKB(1_000L), equalTo(1024L));
        assertThat(ByteUnit.KiB.toKB(Long.MAX_VALUE), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.KiB.toKB(-1f), equalTo(-1.024f));
        assertThat(ByteUnit.KiB.toKB(0f), equalTo(0f));
        assertThat(ByteUnit.KiB.toKB(1f), equalTo(1.024f));

        assertThat(ByteUnit.KiB.toKB(-1d), equalTo(-1.024d));
        assertThat(ByteUnit.KiB.toKB(0d), equalTo(0d));
        assertThat(ByteUnit.KiB.toKB(1d), equalTo(1.024d));
    }

    @Test
    public void shouldConvertFromKibiToKibi()
    {
        assertThat(ByteUnit.KiB.toKiB(Integer.MIN_VALUE), equalTo(Integer.MIN_VALUE));
        assertThat(ByteUnit.KiB.toKiB(-1), equalTo(-1));
        assertThat(ByteUnit.KiB.toKiB(0), equalTo(0));
        assertThat(ByteUnit.KiB.toKiB(1), equalTo(1));
        assertThat(ByteUnit.KiB.toKiB(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.KiB.toKiB(Long.MIN_VALUE), equalTo(Long.MIN_VALUE));
        assertThat(ByteUnit.KiB.toKiB(-1L), equalTo(-1L));
        assertThat(ByteUnit.KiB.toKiB(0L), equalTo(0L));
        assertThat(ByteUnit.KiB.toKiB(1L), equalTo(1L));
        assertThat(ByteUnit.KiB.toKiB(Long.MAX_VALUE), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.KiB.toKiB(-1f), equalTo(-1f));
        assertThat(ByteUnit.KiB.toKiB(0f), equalTo(0f));
        assertThat(ByteUnit.KiB.toKiB(1f), equalTo(1f));

        assertThat(ByteUnit.KiB.toKiB(-1d), equalTo(-1d));
        assertThat(ByteUnit.KiB.toKiB(0d), equalTo(0d));
        assertThat(ByteUnit.KiB.toKiB(1d), equalTo(1d));
    }

    @Test
    public void shouldConvertFromKibiToMebi()
    {
        assertThat(ByteUnit.KiB.toMiB(Integer.MIN_VALUE), equalTo(-2097152));
        assertThat(ByteUnit.KiB.toMiB(-1024), equalTo(-1));
        assertThat(ByteUnit.KiB.toMiB(-1023), equalTo(0));
        assertThat(ByteUnit.KiB.toMiB(0), equalTo(0));
        assertThat(ByteUnit.KiB.toMiB(1023), equalTo(0));
        assertThat(ByteUnit.KiB.toMiB(1024), equalTo(1));
        assertThat(ByteUnit.KiB.toMiB(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.KiB.toMiB(Long.MIN_VALUE), equalTo(-9007199254740992L));
        assertThat(ByteUnit.KiB.toMiB(-1024L), equalTo(-1L));
        assertThat(ByteUnit.KiB.toMiB(-1023L), equalTo(0L));
        assertThat(ByteUnit.KiB.toMiB(0L), equalTo(0L));
        assertThat(ByteUnit.KiB.toMiB(1023L), equalTo(0L));
        assertThat(ByteUnit.KiB.toMiB(1024L), equalTo(1L));
        assertThat(ByteUnit.KiB.toMiB(Long.MAX_VALUE), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.KiB.toMiB(-1f), equalTo(-0.0009765625f));
        assertThat(ByteUnit.KiB.toMiB(0f), equalTo(0f));
        assertThat(ByteUnit.KiB.toMiB(1f), equalTo(0.0009765625f));

        assertThat(ByteUnit.KiB.toMiB(-1d), equalTo(-0.0009765625d));
        assertThat(ByteUnit.KiB.toMiB(0d), equalTo(0d));
        assertThat(ByteUnit.KiB.toMiB(1d), equalTo(0.0009765625d));
    }

    @Test
    public void shouldConvertFromKibiToGibi()
    {
        assertThat(ByteUnit.KiB.toGiB(Integer.MIN_VALUE), equalTo(-2048));
        assertThat(ByteUnit.KiB.toGiB(-1_048_576), equalTo(-1));
        assertThat(ByteUnit.KiB.toGiB(-1_048_575), equalTo(0));
        assertThat(ByteUnit.KiB.toGiB(0), equalTo(0));
        assertThat(ByteUnit.KiB.toGiB(1_048_575), equalTo(0));
        assertThat(ByteUnit.KiB.toGiB(1_048_576), equalTo(1));
        assertThat(ByteUnit.KiB.toGiB(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.KiB.toGiB(Long.MIN_VALUE), equalTo(-8796093022208L));
        assertThat(ByteUnit.KiB.toGiB(-1_048_576L), equalTo(-1L));
        assertThat(ByteUnit.KiB.toGiB(-1_048_575L), equalTo(0L));
        assertThat(ByteUnit.KiB.toGiB(0L), equalTo(0L));
        assertThat(ByteUnit.KiB.toGiB(1_048_575L), equalTo(0L));
        assertThat(ByteUnit.KiB.toGiB(1_048_576L), equalTo(1L));
        assertThat(ByteUnit.KiB.toGiB(Long.MAX_VALUE), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.KiB.toGiB(-1f), equalTo(-9.536743e-7f));
        assertThat(ByteUnit.KiB.toGiB(0f), equalTo(0f));
        assertThat(ByteUnit.KiB.toGiB(1f), equalTo(9.536743e-7f));

        assertThat(ByteUnit.KiB.toGiB(-1d), equalTo(-9.5367431640625e-7d));
        assertThat(ByteUnit.KiB.toGiB(0d), equalTo(0d));
        assertThat(ByteUnit.KiB.toGiB(1d), equalTo(9.5367431640625e-7d));
    }

    @Test
    public void shouldConvertFromKibiToTebi()
    {
        assertThat(ByteUnit.KiB.toTiB(Integer.MIN_VALUE), equalTo(-2));
        assertThat(ByteUnit.KiB.toTiB(-1_073_741_824), equalTo(-1));
        assertThat(ByteUnit.KiB.toTiB(-1_073_741_823), equalTo(0));
        assertThat(ByteUnit.KiB.toTiB(-1), equalTo(0));
        assertThat(ByteUnit.KiB.toTiB(0), equalTo(0));
        assertThat(ByteUnit.KiB.toTiB(1), equalTo(0));
        assertThat(ByteUnit.KiB.toTiB(1_073_741_823), equalTo(0));
        assertThat(ByteUnit.KiB.toTiB(1_073_741_824), equalTo(1));
        assertThat(ByteUnit.KiB.toTiB(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.KiB.toTiB(Long.MIN_VALUE), equalTo(-8589934592L));
        assertThat(ByteUnit.KiB.toTiB(-1_073_741_824L), equalTo(-1L));
        assertThat(ByteUnit.KiB.toTiB(-1_073_741_823L), equalTo(0L));
        assertThat(ByteUnit.KiB.toTiB(-1L), equalTo(0L));
        assertThat(ByteUnit.KiB.toTiB(0L), equalTo(0L));
        assertThat(ByteUnit.KiB.toTiB(1L), equalTo(0L));
        assertThat(ByteUnit.KiB.toTiB(1_073_741_823L), equalTo(0L));
        assertThat(ByteUnit.KiB.toTiB(1_073_741_824L), equalTo(1L));
        assertThat(ByteUnit.KiB.toTiB(Long.MAX_VALUE), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.KiB.toTiB(-1_073_741_824f), equalTo(-1f));
        assertThat(ByteUnit.KiB.toTiB(-1_073_741_823f), equalTo(-0.99999999906f));
        assertThat(ByteUnit.KiB.toTiB(-1f), equalTo(-9.3132257461e-10f));
        assertThat(ByteUnit.KiB.toTiB(0f), equalTo(0f));
        assertThat(ByteUnit.KiB.toTiB(1f), equalTo(9.3132257461e-10f));
        assertThat(ByteUnit.KiB.toTiB(1_073_741_823f), equalTo(0.99999999906f));
        assertThat(ByteUnit.KiB.toTiB(1_073_741_824f), equalTo(1f));

        assertThat(ByteUnit.KiB.toTiB(-1_073_741_824d), equalTo(-1d));
        assertThat(ByteUnit.KiB.toTiB(-1_073_741_823d), equalTo(-0.9999999990686774d));
        assertThat(ByteUnit.KiB.toTiB(-1d), equalTo(-9.313225746154785e-10d));
        assertThat(ByteUnit.KiB.toTiB(0d), equalTo(0d));
        assertThat(ByteUnit.KiB.toTiB(1d), equalTo(9.313225746154785e-10d));
        assertThat(ByteUnit.KiB.toTiB(1_073_741_823d), equalTo(0.9999999990686774d));
        assertThat(ByteUnit.KiB.toTiB(1_073_741_824d), equalTo(1d));
    }

    @Test
    public void shouldConvertFromKibiToPebi()
    {
        assertThat(ByteUnit.KiB.toPiB(Integer.MIN_VALUE), equalTo(0));
        assertThat(ByteUnit.KiB.toPiB(0), equalTo(0));
        assertThat(ByteUnit.KiB.toPiB(Integer.MAX_VALUE-1), equalTo(0));
        assertThat(ByteUnit.KiB.toPiB(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.KiB.toPiB(Long.MIN_VALUE), equalTo(-8388608L));
        assertThat(ByteUnit.KiB.toPiB(-1_099_511_627_776L), equalTo(-1L));
        assertThat(ByteUnit.KiB.toPiB(-1_099_511_627_775L), equalTo(0L));
        assertThat(ByteUnit.KiB.toPiB(-1L), equalTo(0L));
        assertThat(ByteUnit.KiB.toPiB(0L), equalTo(0L));
        assertThat(ByteUnit.KiB.toPiB(1L), equalTo(0L));
        assertThat(ByteUnit.KiB.toPiB(1_099_511_627_775L), equalTo(0L));
        assertThat(ByteUnit.KiB.toPiB(1_099_511_627_776L), equalTo(1L));
        assertThat(ByteUnit.KiB.toPiB(Long.MAX_VALUE), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.KiB.toPiB(-1_099_511_627_776f), equalTo(-1f));
        assertThat(ByteUnit.KiB.toPiB(-1_099_511_627_775f), equalTo(-1f)); // limits of single precision
        assertThat(ByteUnit.KiB.toPiB(-1f), equalTo(-9.094947e-13f));
        assertThat(ByteUnit.KiB.toPiB(0f), equalTo(0f));
        assertThat(ByteUnit.KiB.toPiB(1f), equalTo(9.094947e-13f));
        assertThat(ByteUnit.KiB.toPiB(1_099_511_627_775f), equalTo(1f)); // limits of single precision
        assertThat(ByteUnit.KiB.toPiB(1_099_511_627_776f), equalTo(1f));

        assertThat(ByteUnit.KiB.toPiB(-1_099_511_627_776d), equalTo(-1d));
        assertThat(ByteUnit.KiB.toPiB(-1_099_511_627_775d), equalTo(-0.9999999999990905d));
        assertThat(ByteUnit.KiB.toPiB(-1d), equalTo(-9.094947017729282e-13d));
        assertThat(ByteUnit.KiB.toPiB(0d), equalTo(0d));
        assertThat(ByteUnit.KiB.toPiB(1d), equalTo(9.094947017729282e-13d));
        assertThat(ByteUnit.KiB.toPiB(1_099_511_627_775d), equalTo(0.9999999999990905d));
        assertThat(ByteUnit.KiB.toPiB(1_099_511_627_776d), equalTo(1d));
    }


    @Test
    public void shouldYieldPrefixOfMega()
    {
        assertThat(ByteUnit.Type.DECIMAL.unitsOf(-999_999_999L), is(ByteUnit.MB));
        assertThat(ByteUnit.Type.DECIMAL.unitsOf(-1_000_000L), is(ByteUnit.MB));
        assertThat(ByteUnit.Type.DECIMAL.unitsOf(1_000_000L), is(ByteUnit.MB));
        assertThat(ByteUnit.Type.DECIMAL.unitsOf(999_999_999L), is(ByteUnit.MB));

        assertThat(ByteUnit.Type.DECIMAL.unitsOf(-999_999_999), is(ByteUnit.MB));
        assertThat(ByteUnit.Type.DECIMAL.unitsOf(-1_000_000), is(ByteUnit.MB));
        assertThat(ByteUnit.Type.DECIMAL.unitsOf(1_000_000), is(ByteUnit.MB));
        assertThat(ByteUnit.Type.DECIMAL.unitsOf(999_999_999), is(ByteUnit.MB));
    }

    @Test
    public void shouldYieldExactUnitsOfMega()
    {
        assertThat(ByteUnit.Type.DECIMAL.exactUnitsOf(-1_000_000_001_000_000L), is(ByteUnit.MB));
        assertThat(ByteUnit.Type.DECIMAL.exactUnitsOf(-1_000_001_000_000L), is(ByteUnit.MB));
        assertThat(ByteUnit.Type.DECIMAL.exactUnitsOf(-1_001_000_000L), is(ByteUnit.MB));
        assertThat(ByteUnit.Type.DECIMAL.exactUnitsOf(-2_000_000L), is(ByteUnit.MB));
        assertThat(ByteUnit.Type.DECIMAL.exactUnitsOf(-1_000_000L), is(ByteUnit.MB));
        assertThat(ByteUnit.Type.DECIMAL.exactUnitsOf(1_000_000L), is(ByteUnit.MB));
        assertThat(ByteUnit.Type.DECIMAL.exactUnitsOf(2_000_000L), is(ByteUnit.MB));
        assertThat(ByteUnit.Type.DECIMAL.exactUnitsOf(1_001_000_000L), is(ByteUnit.MB));
        assertThat(ByteUnit.Type.DECIMAL.exactUnitsOf(1_000_001_000_000L), is(ByteUnit.MB));
        assertThat(ByteUnit.Type.DECIMAL.exactUnitsOf(1_000_000_001_000_000L), is(ByteUnit.MB));
    }

    @Test
    public void shouldClaimMegaType()
    {
        assertThat(ByteUnit.MB.hasType(ByteUnit.Type.BINARY), equalTo(false));
        assertThat(ByteUnit.MB.hasType(ByteUnit.Type.DECIMAL), equalTo(true));
    }

    @Test
    public void shouldConvertToMega()
    {
        assertThat(ByteUnit.MB.convert(5_000_000L, ByteUnit.BYTES), equalTo(5L));
        assertThat(ByteUnit.MB.convert(5_000L, ByteUnit.KB), equalTo(5L));
        assertThat(ByteUnit.MB.convert(5L, ByteUnit.MB), equalTo(5L));
        assertThat(ByteUnit.MB.convert(1L, ByteUnit.GB), equalTo(1_000L));
        assertThat(ByteUnit.MB.convert(1L, ByteUnit.TB), equalTo(1_000_000L));
        assertThat(ByteUnit.MB.convert(1L, ByteUnit.PB), equalTo(1_000_000_000L));

        assertThat(ByteUnit.MB.convert(5_000_000, ByteUnit.BYTES), equalTo(5));
        assertThat(ByteUnit.MB.convert(5_000, ByteUnit.KB), equalTo(5));
        assertThat(ByteUnit.MB.convert(5, ByteUnit.MB), equalTo(5));
        assertThat(ByteUnit.MB.convert(1, ByteUnit.GB), equalTo(1_000));
        assertThat(ByteUnit.MB.convert(1, ByteUnit.TB), equalTo(1_000_000));
        assertThat(ByteUnit.MB.convert(1, ByteUnit.PB), equalTo(1_000_000_000));

        assertThat(ByteUnit.MB.convert(1_000_000d, ByteUnit.BYTES), equalTo(1d));
        assertThat(ByteUnit.MB.convert(1_000d, ByteUnit.KB), equalTo(1d));
        assertThat(ByteUnit.MB.convert(1d, ByteUnit.MB), equalTo(1d));
        assertThat(ByteUnit.MB.convert(1d, ByteUnit.GB), equalTo(1_000d));
        assertThat(ByteUnit.MB.convert(1d, ByteUnit.TB), equalTo(1_000_000d));
        assertThat(ByteUnit.MB.convert(1d, ByteUnit.PB), equalTo(1_000_000_000d));
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertMegaToBytes1()
    {
        ByteUnit.MB.toBytes(Integer.MIN_VALUE+1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertMegaToBytes2()
    {
        ByteUnit.MB.toBytes(-2_148);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertMegaToBytes3()
    {
        ByteUnit.MB.toBytes(2_148);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertMegaToBytes4()
    {
        ByteUnit.MB.toBytes(Integer.MAX_VALUE-1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertMegaToBytes5()
    {
        ByteUnit.MB.toBytes(Long.MIN_VALUE+1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertMegaToBytes6()
    {
        ByteUnit.MB.toBytes(-9_223_372_036_855L);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertMegaToBytes7()
    {
        ByteUnit.MB.toBytes(9_223_372_036_855L);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertMegaToBytes8()
    {
        ByteUnit.MB.toBytes(Long.MAX_VALUE-1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertMegaToBytes9()
    {
        ByteUnit.MB.toBytes(Integer.MIN_VALUE);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertMegaToBytes10()
    {
        ByteUnit.MB.toBytes(Long.MIN_VALUE);
    }

    @Test
    public void shouldConvertMegaToBytes()
    {
        assertThat(ByteUnit.MB.toBytes(-2_147), equalTo(-2_147_000_000));
        assertThat(ByteUnit.MB.toBytes(-1), equalTo(-1_000_000));
        assertThat(ByteUnit.MB.toBytes(0), equalTo(0));
        assertThat(ByteUnit.MB.toBytes(1), equalTo(1_000_000));
        assertThat(ByteUnit.MB.toBytes(2_147), equalTo(2_147_000_000));
        assertThat(ByteUnit.MB.toBytes(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.MB.toBytes(-9_223_372_036_854L), equalTo(-9_223_372_036_854_000_000L));
        assertThat(ByteUnit.MB.toBytes(-1L), equalTo(-1_000_000L));
        assertThat(ByteUnit.MB.toBytes(0L), equalTo(0L));
        assertThat(ByteUnit.MB.toBytes(1L), equalTo(1_000_000L));
        assertThat(ByteUnit.MB.toBytes(9_223_372_036_854L), equalTo(9_223_372_036_854_000_000L));
        assertThat(ByteUnit.MB.toBytes(Long.MAX_VALUE), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.MB.toBytes(-1f), equalTo(-1e6f));
        assertThat(ByteUnit.MB.toBytes(0f), equalTo(0f));
        assertThat(ByteUnit.MB.toBytes(1f), equalTo(1e6f));

        assertThat(ByteUnit.MB.toBytes(-1d), equalTo(-1e6d));
        assertThat(ByteUnit.MB.toBytes(0d), equalTo(0d));
        assertThat(ByteUnit.MB.toBytes(1d), equalTo(1e6d));
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertMegaToKilo1()
    {
        ByteUnit.MB.toKB(Integer.MIN_VALUE+1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertMegaToKilo2()
    {
        ByteUnit.MB.toKB(-2_147_484);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertMegaToKilo3()
    {
        ByteUnit.MB.toKB(2_147_484);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertMegaToKilo4()
    {
        ByteUnit.MB.toKB(Integer.MAX_VALUE-1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertMegaToKilo5()
    {
        ByteUnit.MB.toKB(Long.MIN_VALUE+1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertMegaToKilo6()
    {
        ByteUnit.MB.toKB(-9_223_372_036_854_776L);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertMegaToKilo7()
    {
        ByteUnit.MB.toKB(9_223_372_036_854_776L);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertMegaToKilo8()
    {
        ByteUnit.MB.toKB(Long.MAX_VALUE-1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertMegaToKilo9()
    {
        ByteUnit.MB.toKB(Integer.MIN_VALUE);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertMegaToKilo10()
    {
        ByteUnit.MB.toKB(Long.MIN_VALUE);
    }

    @Test
    public void shouldConvertMegaToKilo()
    {
        assertThat(ByteUnit.MB.toKB(-2_147_483), equalTo(-2_147_483_000));
        assertThat(ByteUnit.MB.toKB(-1), equalTo(-1_000));
        assertThat(ByteUnit.MB.toKB(0), equalTo(0));
        assertThat(ByteUnit.MB.toKB(1), equalTo(1_000));
        assertThat(ByteUnit.MB.toKB(2_147_483), equalTo(2_147_483_000));
        assertThat(ByteUnit.MB.toKB(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.MB.toKB(-9_223_372_036_854_775L), equalTo(-9_223_372_036_854_775_000L));
        assertThat(ByteUnit.MB.toKB(-1L), equalTo(-1_000L));
        assertThat(ByteUnit.MB.toKB(0L), equalTo(0L));
        assertThat(ByteUnit.MB.toKB(1L), equalTo(1_000L));
        assertThat(ByteUnit.MB.toKB(9_223_372_036_854_775L), equalTo(9_223_372_036_854_775_000L));
        assertThat(ByteUnit.MB.toKB(Long.MAX_VALUE), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.MB.toKB(-1f), equalTo(-1_000f));
        assertThat(ByteUnit.MB.toKB(0f), equalTo(0f));
        assertThat(ByteUnit.MB.toKB(1f), equalTo(1_000f));

        assertThat(ByteUnit.MB.toKB(-1d), equalTo(-1_000d));
        assertThat(ByteUnit.MB.toKB(0d), equalTo(0d));
        assertThat(ByteUnit.MB.toKB(1d), equalTo(1_000d));
    }

    @Test
    public void shouldConvertFromMegaToMega()
    {
        assertThat(ByteUnit.MB.toMB(Integer.MIN_VALUE), equalTo(Integer.MIN_VALUE));
        assertThat(ByteUnit.MB.toMB(-1), equalTo(-1));
        assertThat(ByteUnit.MB.toMB(0), equalTo(0));
        assertThat(ByteUnit.MB.toMB(1), equalTo(1));
        assertThat(ByteUnit.MB.toMB(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.MB.toMB(Long.MIN_VALUE), equalTo(Long.MIN_VALUE));
        assertThat(ByteUnit.MB.toMB(-1L), equalTo(-1L));
        assertThat(ByteUnit.MB.toMB(0L), equalTo(0L));
        assertThat(ByteUnit.MB.toMB(1L), equalTo(1L));
        assertThat(ByteUnit.MB.toMB(Long.MAX_VALUE), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.MB.toMB(-1f), equalTo(-1f));
        assertThat(ByteUnit.MB.toMB(0f), equalTo(0f));
        assertThat(ByteUnit.MB.toMB(1f), equalTo(1f));

        assertThat(ByteUnit.MB.toMB(-1d), equalTo(-1d));
        assertThat(ByteUnit.MB.toMB(0d), equalTo(0d));
        assertThat(ByteUnit.MB.toMB(1d), equalTo(1d));
    }

    @Test
    public void shouldConvertMegaToGiga()
    {
        assertThat(ByteUnit.MB.toGB(Integer.MIN_VALUE), equalTo(-2147483));
        assertThat(ByteUnit.MB.toGB(-1_000), equalTo(-1));
        assertThat(ByteUnit.MB.toGB(-999), equalTo(0));
        assertThat(ByteUnit.MB.toGB(0), equalTo(0));
        assertThat(ByteUnit.MB.toGB(999), equalTo(0));
        assertThat(ByteUnit.MB.toGB(1_000), equalTo(1));
        assertThat(ByteUnit.MB.toGB(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.MB.toGB(Long.MIN_VALUE), equalTo(-9223372036854775L));
        assertThat(ByteUnit.MB.toGB(-1_000L), equalTo(-1L));
        assertThat(ByteUnit.MB.toGB(-999L), equalTo(0L));
        assertThat(ByteUnit.MB.toGB(0L), equalTo(0L));
        assertThat(ByteUnit.MB.toGB(999L), equalTo(0L));
        assertThat(ByteUnit.MB.toGB(1_000L), equalTo(1L));
        assertThat(ByteUnit.MB.toGB(Long.MAX_VALUE), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.MB.toGB(-1f), equalTo(-0.001f));
        assertThat(ByteUnit.MB.toGB(0f), equalTo(0f));
        assertThat(ByteUnit.MB.toGB(1f), equalTo(0.001f));

        assertThat(ByteUnit.MB.toGB(-1d), equalTo(-0.001d));
        assertThat(ByteUnit.MB.toGB(0d), equalTo(0d));
        assertThat(ByteUnit.MB.toGB(1d), equalTo(0.001d));
    }

    @Test
    public void shouldConvertMegaToTera()
    {
        assertThat(ByteUnit.MB.toTB(Integer.MIN_VALUE), equalTo(-2147));
        assertThat(ByteUnit.MB.toTB(-1_000_000), equalTo(-1));
        assertThat(ByteUnit.MB.toTB(-999_999), equalTo(0));
        assertThat(ByteUnit.MB.toTB(0), equalTo(0));
        assertThat(ByteUnit.MB.toTB(999_999), equalTo(0));
        assertThat(ByteUnit.MB.toTB(1_000_000), equalTo(1));
        assertThat(ByteUnit.MB.toTB(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.MB.toTB(Long.MIN_VALUE), equalTo(-9223372036854L));
        assertThat(ByteUnit.MB.toTB(-1_000_000L), equalTo(-1L));
        assertThat(ByteUnit.MB.toTB(-999_999L), equalTo(0L));
        assertThat(ByteUnit.MB.toTB(0L), equalTo(0L));
        assertThat(ByteUnit.MB.toTB(999_999L), equalTo(0L));
        assertThat(ByteUnit.MB.toTB(1_000_000L), equalTo(1L));
        assertThat(ByteUnit.MB.toTB(Long.MAX_VALUE), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.MB.toTB(-1f), equalTo(-0.000_001f));
        assertThat(ByteUnit.MB.toTB(0f), equalTo(0f));
        assertThat(ByteUnit.MB.toTB(1f), equalTo(0.000_001f));

        assertThat(ByteUnit.MB.toTB(-1d), equalTo(-0.000_001d));
        assertThat(ByteUnit.MB.toTB(0d), equalTo(0d));
        assertThat(ByteUnit.MB.toTB(1d), equalTo(0.000_001d));
    }

    @Test
    public void shouldConvertMegaToPeta()
    {
        assertThat(ByteUnit.MB.toPB(Integer.MIN_VALUE), equalTo(-2));
        assertThat(ByteUnit.MB.toPB(-1_000_000_000), equalTo(-1));
        assertThat(ByteUnit.MB.toPB(-999_999_999), equalTo(0));
        assertThat(ByteUnit.MB.toPB(0), equalTo(0));
        assertThat(ByteUnit.MB.toPB(999_999_999), equalTo(0));
        assertThat(ByteUnit.MB.toPB(1_000_000_000), equalTo(1));
        assertThat(ByteUnit.MB.toPB(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.MB.toPB(Long.MIN_VALUE), equalTo(-9223372036L));
        assertThat(ByteUnit.MB.toPB(-1_000_000_000L), equalTo(-1L));
        assertThat(ByteUnit.MB.toPB(-999_999_999L), equalTo(0L));
        assertThat(ByteUnit.MB.toPB(0L), equalTo(0L));
        assertThat(ByteUnit.MB.toPB(999_999_999L), equalTo(0L));
        assertThat(ByteUnit.MB.toPB(1_000_000_000L), equalTo(1L));
        assertThat(ByteUnit.MB.toPB(Long.MAX_VALUE), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.MB.toPB(-1f), equalTo(-0.000_000_001f));
        assertThat(ByteUnit.MB.toPB(0f), equalTo(0f));
        assertThat(ByteUnit.MB.toPB(1f), equalTo(0.000_000_001f));

        assertThat(ByteUnit.MB.toPB(-1d), equalTo(-0.000_000_001d));
        assertThat(ByteUnit.MB.toPB(0d), equalTo(0d));
        assertThat(ByteUnit.MB.toPB(1d), equalTo(0.000_000_001d));
    }


    @Test
    public void shouldYieldPrefixOfMebi()
    {
        assertThat(ByteUnit.Type.BINARY.unitsOf(-1073741823L), is(ByteUnit.MiB));
        assertThat(ByteUnit.Type.BINARY.unitsOf(-1048576L), is(ByteUnit.MiB));
        assertThat(ByteUnit.Type.BINARY.unitsOf(1048576L), is(ByteUnit.MiB));
        assertThat(ByteUnit.Type.BINARY.unitsOf(1073741823L), is(ByteUnit.MiB));

        assertThat(ByteUnit.Type.BINARY.unitsOf(-1073741823), is(ByteUnit.MiB));
        assertThat(ByteUnit.Type.BINARY.unitsOf(-1048576), is(ByteUnit.MiB));
        assertThat(ByteUnit.Type.BINARY.unitsOf(1048576), is(ByteUnit.MiB));
        assertThat(ByteUnit.Type.BINARY.unitsOf(1073741823), is(ByteUnit.MiB));
    }

    @Test
    public void shouldYieldExactUnitsOfMebi()
    {
        assertThat(ByteUnit.Type.BINARY.exactUnitsOf(-(1048576L+1125899906842624L)), is(ByteUnit.MiB));
        assertThat(ByteUnit.Type.BINARY.exactUnitsOf(-(1048576L+1099511627776L)), is(ByteUnit.MiB));
        assertThat(ByteUnit.Type.BINARY.exactUnitsOf(-(1048576L+1073741824L)), is(ByteUnit.MiB));
        assertThat(ByteUnit.Type.BINARY.exactUnitsOf(-2*1048576L), is(ByteUnit.MiB));
        assertThat(ByteUnit.Type.BINARY.exactUnitsOf(-1048576L), is(ByteUnit.MiB));
        assertThat(ByteUnit.Type.BINARY.exactUnitsOf(1048576L), is(ByteUnit.MiB));
        assertThat(ByteUnit.Type.BINARY.exactUnitsOf(2*1048576L), is(ByteUnit.MiB));
        assertThat(ByteUnit.Type.BINARY.exactUnitsOf(1048576L+1073741824L), is(ByteUnit.MiB));
        assertThat(ByteUnit.Type.BINARY.exactUnitsOf(1048576L+1099511627776L), is(ByteUnit.MiB));
        assertThat(ByteUnit.Type.BINARY.exactUnitsOf(1048576L+1125899906842624L), is(ByteUnit.MiB));
    }

    @Test
    public void shouldClaimMebiType()
    {
        assertThat(ByteUnit.MiB.hasType(ByteUnit.Type.BINARY), equalTo(true));
        assertThat(ByteUnit.MiB.hasType(ByteUnit.Type.DECIMAL), equalTo(false));
    }

    @Test
    public void shouldConvertToMebi()
    {
        assertThat(ByteUnit.MiB.convert(5L * 1048576L, ByteUnit.BYTES), equalTo(5L));
        assertThat(ByteUnit.MiB.convert(5L * 1024L, ByteUnit.KiB), equalTo(5L));
        assertThat(ByteUnit.MiB.convert(5L, ByteUnit.MiB), equalTo(5L));
        assertThat(ByteUnit.MiB.convert(1L, ByteUnit.GiB), equalTo(1024L));
        assertThat(ByteUnit.MiB.convert(1L, ByteUnit.TiB), equalTo(1048576L));
        assertThat(ByteUnit.MiB.convert(1L, ByteUnit.PiB), equalTo(1073741824L));

        assertThat(ByteUnit.MiB.convert(5 * 1048576, ByteUnit.BYTES), equalTo(5));
        assertThat(ByteUnit.MiB.convert(5 * 1024, ByteUnit.KiB), equalTo(5));
        assertThat(ByteUnit.MiB.convert(5, ByteUnit.MiB), equalTo(5));
        assertThat(ByteUnit.MiB.convert(1, ByteUnit.GiB), equalTo(1024));
        assertThat(ByteUnit.MiB.convert(1, ByteUnit.TiB), equalTo(1048576));
        assertThat(ByteUnit.MiB.convert(1, ByteUnit.PiB), equalTo(1073741824));

        assertThat(ByteUnit.MiB.convert(1048576d, ByteUnit.BYTES), equalTo(1d));
        assertThat(ByteUnit.MiB.convert(1024d, ByteUnit.KiB), equalTo(1d));
        assertThat(ByteUnit.MiB.convert(1d, ByteUnit.MiB), equalTo(1d));
        assertThat(ByteUnit.MiB.convert(1d, ByteUnit.GiB), equalTo(1024d));
        assertThat(ByteUnit.MiB.convert(1d, ByteUnit.TiB), equalTo(1048576d));
        assertThat(ByteUnit.MiB.convert(1d, ByteUnit.PiB), equalTo(1073741824d));
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertMebiToBytes1()
    {
        assertThat(ByteUnit.MiB.toBytes(Integer.MIN_VALUE+1), equalTo(Integer.MIN_VALUE));
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertMebiToBytes2()
    {
        assertThat(ByteUnit.MiB.toBytes(Integer.MAX_VALUE-1), equalTo(Integer.MAX_VALUE));
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertMebiToBytes3()
    {
        ByteUnit.MiB.toBytes(-2049);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertMebiToBytes4()
    {
        ByteUnit.MiB.toBytes(2048);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertMebiToBytes5()
    {
        ByteUnit.MiB.toBytes(2049);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertMebiToBytes6()
    {
        ByteUnit.MiB.toBytes(Long.MIN_VALUE+1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertMebiToBytes7()
    {
        ByteUnit.MiB.toBytes(8_796_093_022_208L);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertMebiToBytes8()
    {
        ByteUnit.MiB.toBytes(Long.MAX_VALUE-1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertMebiToBytes9()
    {
        ByteUnit.MiB.toBytes(Integer.MIN_VALUE);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertMebiToBytes10()
    {
        ByteUnit.MiB.toBytes(Long.MIN_VALUE);
    }

    @Test
    public void shouldConvertMebiToBytes()
    {
        assertThat(ByteUnit.MiB.toBytes(-2048), equalTo(Integer.MIN_VALUE));
        assertThat(ByteUnit.MiB.toBytes(-2047), equalTo(-2_146_435_072));
        assertThat(ByteUnit.MiB.toBytes(-1), equalTo(-1_048_576));
        assertThat(ByteUnit.MiB.toBytes(0), equalTo(0));
        assertThat(ByteUnit.MiB.toBytes(1), equalTo(1_048_576));
        assertThat(ByteUnit.MiB.toBytes(2047), equalTo(2_146_435_072));
        assertThat(ByteUnit.MiB.toBytes(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.MiB.toBytes(-8_796_093_022_208L), equalTo(Long.MIN_VALUE));
        assertThat(ByteUnit.MiB.toBytes(-8_796_093_022_207L), equalTo(-9_223_372_036_853_727_232L));
        assertThat(ByteUnit.MiB.toBytes(-1L), equalTo(-1048576L));
        assertThat(ByteUnit.MiB.toBytes(0L), equalTo(0L));
        assertThat(ByteUnit.MiB.toBytes(1L), equalTo(1048576L));
        assertThat(ByteUnit.MiB.toBytes(8_796_093_022_207L), equalTo(9_223_372_036_853_727_232L));
        assertThat(ByteUnit.MiB.toBytes(Long.MAX_VALUE), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.MiB.toBytes(-1f), equalTo(-1048576f));
        assertThat(ByteUnit.MiB.toBytes(0f), equalTo(0f));
        assertThat(ByteUnit.MiB.toBytes(1f), equalTo(1048576f));

        assertThat(ByteUnit.MiB.toBytes(-1d), equalTo(-1048576d));
        assertThat(ByteUnit.MiB.toBytes(0d), equalTo(0d));
        assertThat(ByteUnit.MiB.toBytes(1d), equalTo(1048576d));
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertMebiToKibi()
    {
        ByteUnit.MiB.toKiB(Integer.MIN_VALUE+1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertMebiToKibi2()
    {
        ByteUnit.MiB.toKiB(2_097_152);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertMebiToKibi3()
    {
        ByteUnit.MiB.toKiB(Integer.MAX_VALUE-1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertMebiToKibi4()
    {
        ByteUnit.MiB.toKiB(Long.MIN_VALUE+1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertMebiToKibi5()
    {
        ByteUnit.MiB.toKiB(9_007_199_254_740_992L);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertMebiToKibi6()
    {
        ByteUnit.MiB.toKiB(Long.MAX_VALUE-1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertMebiToKibi7()
    {
        ByteUnit.MiB.toKiB(Integer.MIN_VALUE);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertMebiToKibi8()
    {
        ByteUnit.MiB.toKiB(Long.MIN_VALUE);
    }

    @Test
    public void shouldConvertMebiToKibi()
    {
        assertThat(ByteUnit.MiB.toKiB(-2_097_152), equalTo(Integer.MIN_VALUE));
        assertThat(ByteUnit.MiB.toKiB(-2_097_151), equalTo(-2_147_482_624));
        assertThat(ByteUnit.MiB.toKiB(-1), equalTo(-1024));
        assertThat(ByteUnit.MiB.toKiB(0), equalTo(0));
        assertThat(ByteUnit.MiB.toKiB(1), equalTo(1024));
        assertThat(ByteUnit.MiB.toKiB(2_097_151), equalTo(2_147_482_624));
        assertThat(ByteUnit.MiB.toKiB(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.MiB.toKiB(-9_007_199_254_740_992L), equalTo(Long.MIN_VALUE));
        assertThat(ByteUnit.MiB.toKiB(-9_007_199_254_740_991L), equalTo(-9_223_372_036_854_774_784L));
        assertThat(ByteUnit.MiB.toKiB(-1L), equalTo(-1024L));
        assertThat(ByteUnit.MiB.toKiB(0L), equalTo(0L));
        assertThat(ByteUnit.MiB.toKiB(1L), equalTo(1024L));
        assertThat(ByteUnit.MiB.toKiB(9_007_199_254_740_991L), equalTo(9_223_372_036_854_774_784L));
        assertThat(ByteUnit.MiB.toKiB(Long.MAX_VALUE), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.MiB.toKiB(-1f), equalTo(-1024f));
        assertThat(ByteUnit.MiB.toKiB(0f), equalTo(0f));
        assertThat(ByteUnit.MiB.toKiB(1f), equalTo(1024f));

        assertThat(ByteUnit.MiB.toKiB(-1d), equalTo(-1024d));
        assertThat(ByteUnit.MiB.toKiB(0d), equalTo(0d));
        assertThat(ByteUnit.MiB.toKiB(1d), equalTo(1024d));
    }

    @Test
    public void shouldConvertFromMebiToMebi()
    {
        assertThat(ByteUnit.MiB.toMiB(Integer.MIN_VALUE), equalTo(Integer.MIN_VALUE));
        assertThat(ByteUnit.MiB.toMiB(-1), equalTo(-1));
        assertThat(ByteUnit.MiB.toMiB(0), equalTo(0));
        assertThat(ByteUnit.MiB.toMiB(1), equalTo(1));
        assertThat(ByteUnit.MiB.toMiB(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.MiB.toMiB(Long.MIN_VALUE), equalTo(Long.MIN_VALUE));
        assertThat(ByteUnit.MiB.toMiB(-1L), equalTo(-1L));
        assertThat(ByteUnit.MiB.toMiB(0L), equalTo(0L));
        assertThat(ByteUnit.MiB.toMiB(1L), equalTo(1L));
        assertThat(ByteUnit.MiB.toMiB(Long.MAX_VALUE), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.MiB.toMiB(-1f), equalTo(-1f));
        assertThat(ByteUnit.MiB.toMiB(0f), equalTo(0f));
        assertThat(ByteUnit.MiB.toMiB(1f), equalTo(1f));

        assertThat(ByteUnit.MiB.toMiB(-1d), equalTo(-1d));
        assertThat(ByteUnit.MiB.toMiB(0d), equalTo(0d));
        assertThat(ByteUnit.MiB.toMiB(1d), equalTo(1d));
    }

    @Test
    public void shouldConvertFromMebiToGibi()
    {
        assertThat(ByteUnit.MiB.toGiB(Integer.MIN_VALUE), equalTo(-2097152));
        assertThat(ByteUnit.MiB.toGiB(-1024), equalTo(-1));
        assertThat(ByteUnit.MiB.toGiB(-1023), equalTo(0));
        assertThat(ByteUnit.MiB.toGiB(0), equalTo(0));
        assertThat(ByteUnit.MiB.toGiB(1023), equalTo(0));
        assertThat(ByteUnit.MiB.toGiB(1024), equalTo(1));
        assertThat(ByteUnit.MiB.toGiB(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.MiB.toGiB(Long.MIN_VALUE), equalTo(-9007199254740992L));
        assertThat(ByteUnit.MiB.toGiB(-1024L), equalTo(-1L));
        assertThat(ByteUnit.MiB.toGiB(-1023L), equalTo(0L));
        assertThat(ByteUnit.MiB.toGiB(0L), equalTo(0L));
        assertThat(ByteUnit.MiB.toGiB(1023L), equalTo(0L));
        assertThat(ByteUnit.MiB.toGiB(1024L), equalTo(1L));
        assertThat(ByteUnit.MiB.toGiB(Long.MAX_VALUE), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.MiB.toGiB(-1f), equalTo(-0.0009765625f));
        assertThat(ByteUnit.MiB.toGiB(0f), equalTo(0f));
        assertThat(ByteUnit.MiB.toGiB(1f), equalTo(0.0009765625f));

        assertThat(ByteUnit.MiB.toGiB(-1d), equalTo(-0.0009765625d));
        assertThat(ByteUnit.MiB.toGiB(0d), equalTo(0d));
        assertThat(ByteUnit.MiB.toGiB(1d), equalTo(0.0009765625d));
    }

    @Test
    public void shouldConvertFromMebiToTebi()
    {
        assertThat(ByteUnit.MiB.toTiB(Integer.MIN_VALUE), equalTo(-2048));
        assertThat(ByteUnit.MiB.toTiB(-1_048_576), equalTo(-1));
        assertThat(ByteUnit.MiB.toTiB(-1_048_575), equalTo(0));
        assertThat(ByteUnit.MiB.toTiB(0), equalTo(0));
        assertThat(ByteUnit.MiB.toTiB(1_048_575), equalTo(0));
        assertThat(ByteUnit.MiB.toTiB(1_048_576), equalTo(1));
        assertThat(ByteUnit.MiB.toTiB(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.MiB.toTiB(Long.MIN_VALUE), equalTo(-8796093022208L));
        assertThat(ByteUnit.MiB.toTiB(-1_048_576L), equalTo(-1L));
        assertThat(ByteUnit.MiB.toTiB(-1_048_575L), equalTo(0L));
        assertThat(ByteUnit.MiB.toTiB(0L), equalTo(0L));
        assertThat(ByteUnit.MiB.toTiB(1_048_575L), equalTo(0L));
        assertThat(ByteUnit.MiB.toTiB(1_048_576L), equalTo(1L));
        assertThat(ByteUnit.MiB.toTiB(Long.MAX_VALUE), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.MiB.toTiB(-1f), equalTo(-9.536743e-7f));
        assertThat(ByteUnit.MiB.toTiB(0f), equalTo(0f));
        assertThat(ByteUnit.MiB.toTiB(1f), equalTo(9.536743e-7f));

        assertThat(ByteUnit.MiB.toTiB(-1d), equalTo(-9.5367431640625e-7d));
        assertThat(ByteUnit.MiB.toTiB(0d), equalTo(0d));
        assertThat(ByteUnit.MiB.toTiB(1d), equalTo(9.5367431640625e-7d));
    }

    @Test
    public void shouldConvertFromMebiToPebi()
    {
        assertThat(ByteUnit.MiB.toPiB(Integer.MIN_VALUE), equalTo(-2));
        assertThat(ByteUnit.MiB.toPiB(-1_073_741_824), equalTo(-1));
        assertThat(ByteUnit.MiB.toPiB(-1_073_741_823), equalTo(0));
        assertThat(ByteUnit.MiB.toPiB(-1), equalTo(0));
        assertThat(ByteUnit.MiB.toPiB(0), equalTo(0));
        assertThat(ByteUnit.MiB.toPiB(1), equalTo(0));
        assertThat(ByteUnit.MiB.toPiB(1_073_741_823), equalTo(0));
        assertThat(ByteUnit.MiB.toPiB(1_073_741_824), equalTo(1));
        assertThat(ByteUnit.MiB.toPiB(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.MiB.toPiB(Long.MIN_VALUE), equalTo(-8589934592L));
        assertThat(ByteUnit.MiB.toPiB(-1_073_741_824L), equalTo(-1L));
        assertThat(ByteUnit.MiB.toPiB(-1_073_741_823L), equalTo(0L));
        assertThat(ByteUnit.MiB.toPiB(-1L), equalTo(0L));
        assertThat(ByteUnit.MiB.toPiB(0L), equalTo(0L));
        assertThat(ByteUnit.MiB.toPiB(1L), equalTo(0L));
        assertThat(ByteUnit.MiB.toPiB(1_073_741_823L), equalTo(0L));
        assertThat(ByteUnit.MiB.toPiB(1_073_741_824L), equalTo(1L));
        assertThat(ByteUnit.MiB.toPiB(Long.MAX_VALUE), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.MiB.toPiB(-1_073_741_824f), equalTo(-1f));
        assertThat(ByteUnit.MiB.toPiB(-1_073_741_823f), equalTo(-0.99999999906f));
        assertThat(ByteUnit.MiB.toPiB(-1f), equalTo(-9.3132257461e-10f));
        assertThat(ByteUnit.MiB.toPiB(0f), equalTo(0f));
        assertThat(ByteUnit.MiB.toPiB(1f), equalTo(9.3132257461e-10f));
        assertThat(ByteUnit.MiB.toPiB(1_073_741_823f), equalTo(0.99999999906f));
        assertThat(ByteUnit.MiB.toPiB(1_073_741_824f), equalTo(1f));

        assertThat(ByteUnit.MiB.toPiB(-1_073_741_824d), equalTo(-1d));
        assertThat(ByteUnit.MiB.toPiB(-1_073_741_823d), equalTo(-0.9999999990686774d));
        assertThat(ByteUnit.MiB.toPiB(-1d), equalTo(-9.313225746154785e-10d));
        assertThat(ByteUnit.MiB.toPiB(0d), equalTo(0d));
        assertThat(ByteUnit.MiB.toPiB(1d), equalTo(9.313225746154785e-10d));
        assertThat(ByteUnit.MiB.toPiB(1_073_741_823d), equalTo(0.9999999990686774d));
        assertThat(ByteUnit.MiB.toPiB(1_073_741_824d), equalTo(1d));
    }


    @Test
    public void shouldYieldPrefixOfGiga()
    {
        assertThat(ByteUnit.Type.DECIMAL.unitsOf(-999_999_999_999L), is(ByteUnit.GB));
        assertThat(ByteUnit.Type.DECIMAL.unitsOf(-1_000_000_000L), is(ByteUnit.GB));
        assertThat(ByteUnit.Type.DECIMAL.unitsOf(1_000_000_000L), is(ByteUnit.GB));
        assertThat(ByteUnit.Type.DECIMAL.unitsOf(999_999_999_999L), is(ByteUnit.GB));

        assertThat(ByteUnit.Type.DECIMAL.unitsOf(Integer.MIN_VALUE), is(ByteUnit.GB));
        assertThat(ByteUnit.Type.DECIMAL.unitsOf(-1_000_000_000), is(ByteUnit.GB));
        assertThat(ByteUnit.Type.DECIMAL.unitsOf(1_000_000_000), is(ByteUnit.GB));
        assertThat(ByteUnit.Type.DECIMAL.unitsOf(Integer.MAX_VALUE), is(ByteUnit.GB));
    }

    @Test
    public void shouldYieldExactUnitsOfGiga()
    {
        assertThat(ByteUnit.Type.DECIMAL.exactUnitsOf(-1_000_001_000_000_000L), is(ByteUnit.GB));
        assertThat(ByteUnit.Type.DECIMAL.exactUnitsOf(-1_001_000_000_000L), is(ByteUnit.GB));
        assertThat(ByteUnit.Type.DECIMAL.exactUnitsOf(-2_000_000_000L), is(ByteUnit.GB));
        assertThat(ByteUnit.Type.DECIMAL.exactUnitsOf(-1_000_000_000L), is(ByteUnit.GB));
        assertThat(ByteUnit.Type.DECIMAL.exactUnitsOf(1_000_000_000L), is(ByteUnit.GB));
        assertThat(ByteUnit.Type.DECIMAL.exactUnitsOf(2_000_000_000L), is(ByteUnit.GB));
        assertThat(ByteUnit.Type.DECIMAL.exactUnitsOf(1_001_000_000_000L), is(ByteUnit.GB));
        assertThat(ByteUnit.Type.DECIMAL.exactUnitsOf(1_000_001_000_000_000L), is(ByteUnit.GB));
    }

    @Test
    public void shouldClaimGigaType()
    {
        assertThat(ByteUnit.GB.hasType(ByteUnit.Type.BINARY), equalTo(false));
        assertThat(ByteUnit.GB.hasType(ByteUnit.Type.DECIMAL), equalTo(true));
    }

    @Test
    public void shouldConvertToGiga()
    {
        assertThat(ByteUnit.GB.convert(5_000_000_000L, ByteUnit.BYTES), equalTo(5L));
        assertThat(ByteUnit.GB.convert(5_000_000L, ByteUnit.KB), equalTo(5L));
        assertThat(ByteUnit.GB.convert(5_000L, ByteUnit.MB), equalTo(5L));
        assertThat(ByteUnit.GB.convert(5L, ByteUnit.GB), equalTo(5L));
        assertThat(ByteUnit.GB.convert(1L, ByteUnit.TB), equalTo(1_000L));
        assertThat(ByteUnit.GB.convert(1L, ByteUnit.PB), equalTo(1_000_000L));

        assertThat(ByteUnit.GB.convert(2_000_000_000, ByteUnit.BYTES), equalTo(2));
        assertThat(ByteUnit.GB.convert(5_000_000, ByteUnit.KB), equalTo(5));
        assertThat(ByteUnit.GB.convert(5_000, ByteUnit.MB), equalTo(5));
        assertThat(ByteUnit.GB.convert(5, ByteUnit.GB), equalTo(5));
        assertThat(ByteUnit.GB.convert(1, ByteUnit.TB), equalTo(1_000));
        assertThat(ByteUnit.GB.convert(1, ByteUnit.PB), equalTo(1_000_000));

        assertThat(ByteUnit.GB.convert(1_000_000_000d, ByteUnit.BYTES), equalTo(1d));
        assertThat(ByteUnit.GB.convert(1_000_000d, ByteUnit.KB), equalTo(1d));
        assertThat(ByteUnit.GB.convert(1_000d, ByteUnit.MB), equalTo(1d));
        assertThat(ByteUnit.GB.convert(1d, ByteUnit.GB), equalTo(1d));
        assertThat(ByteUnit.GB.convert(1d, ByteUnit.TB), equalTo(1_000d));
        assertThat(ByteUnit.GB.convert(1d, ByteUnit.PB), equalTo(1_000_000d));
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertGigaToBytes1()
    {
        ByteUnit.GB.toBytes(Integer.MIN_VALUE+1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertGigaToBytes2()
    {
        ByteUnit.GB.toBytes(-3);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertGigaToBytes3()
    {
        ByteUnit.GB.toBytes(3);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertGigaToBytes4()
    {
        ByteUnit.GB.toBytes(Integer.MAX_VALUE-1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertGigaToBytes5()
    {
        ByteUnit.GB.toBytes(Long.MIN_VALUE+1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertGigaToBytes6()
    {
        ByteUnit.GB.toBytes(-9_223_372_037L);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertGigaToBytes7()
    {
        ByteUnit.GB.toBytes(9_223_372_037L);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertGigaToBytes8()
    {
        ByteUnit.GB.toBytes(Long.MAX_VALUE-1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertGigaToBytes9()
    {
        ByteUnit.GB.toBytes(Integer.MIN_VALUE);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertGigaToBytes10()
    {
        ByteUnit.GB.toBytes(Long.MIN_VALUE);
    }

    @Test
    public void shouldConvertGigaToBytes()
    {
        assertThat(ByteUnit.GB.toBytes(-2), equalTo(-2_000_000_000));
        assertThat(ByteUnit.GB.toBytes(-1), equalTo(-1_000_000_000));
        assertThat(ByteUnit.GB.toBytes(0), equalTo(0));
        assertThat(ByteUnit.GB.toBytes(1), equalTo(1_000_000_000));
        assertThat(ByteUnit.GB.toBytes(2), equalTo(2_000_000_000));
        assertThat(ByteUnit.GB.toBytes(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.GB.toBytes(-9_223_372_036L), equalTo(-9_223_372_036_000_000_000L));
        assertThat(ByteUnit.GB.toBytes(-1L), equalTo(-1_000_000_000L));
        assertThat(ByteUnit.GB.toBytes(0L), equalTo(0L));
        assertThat(ByteUnit.GB.toBytes(1L), equalTo(1_000_000_000L));
        assertThat(ByteUnit.GB.toBytes(9_223_372_036L), equalTo(9_223_372_036_000_000_000L));
        assertThat(ByteUnit.GB.toBytes(Long.MAX_VALUE), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.GB.toBytes(-1f), equalTo(-1e9f));
        assertThat(ByteUnit.GB.toBytes(0f), equalTo(0f));
        assertThat(ByteUnit.GB.toBytes(1f), equalTo(1e9f));

        assertThat(ByteUnit.GB.toBytes(-1d), equalTo(-1e9d));
        assertThat(ByteUnit.GB.toBytes(0d), equalTo(0d));
        assertThat(ByteUnit.GB.toBytes(1d), equalTo(1e9d));
    }


    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertGigaToKilo1()
    {
        ByteUnit.GB.toKB(Integer.MIN_VALUE+1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertGigaToKilo2()
    {
        ByteUnit.GB.toKB(-2_148);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertGigaToKilo3()
    {
        ByteUnit.GB.toKB(Integer.MAX_VALUE-1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertGigaToKilo4()
    {
        ByteUnit.GB.toKB(Long.MIN_VALUE+1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertGigaToKilo5()
    {
        ByteUnit.GB.toKB(Long.MAX_VALUE-1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertGigaToKilo6()
    {
        ByteUnit.GB.toKB(2_148);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertGigaToKilo7()
    {
        ByteUnit.GB.toKB(-9_223_372_036_855L);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertGigaToKilo8()
    {
        ByteUnit.GB.toKB(9_223_372_036_855L);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertGigaToKilo9()
    {
        ByteUnit.GB.toKB(Integer.MIN_VALUE);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertGigaToKilo10()
    {
        ByteUnit.GB.toKB(Long.MIN_VALUE);
    }

    @Test
    public void shouldConvertGigaToKilo()
    {
        assertThat(ByteUnit.GB.toKB(-2_147), equalTo(-2_147_000_000));
        assertThat(ByteUnit.GB.toKB(-1), equalTo(-1_000_000));
        assertThat(ByteUnit.GB.toKB(0), equalTo(0));
        assertThat(ByteUnit.GB.toKB(1), equalTo(1_000_000));
        assertThat(ByteUnit.GB.toKB(2_147), equalTo(2_147_000_000));
        assertThat(ByteUnit.GB.toKB(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.GB.toKB(-9_223_372_036_854L), equalTo(-9_223_372_036_854_000_000L));
        assertThat(ByteUnit.GB.toKB(-1L), equalTo(-1_000_000L));
        assertThat(ByteUnit.GB.toKB(0L), equalTo(0L));
        assertThat(ByteUnit.GB.toKB(1L), equalTo(1_000_000L));
        assertThat(ByteUnit.GB.toKB(9_223_372_036_854L), equalTo(9_223_372_036_854_000_000L));
        assertThat(ByteUnit.GB.toKB(Long.MAX_VALUE), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.GB.toKB(-1f), equalTo(-1e6f));
        assertThat(ByteUnit.GB.toKB(0f), equalTo(0f));
        assertThat(ByteUnit.GB.toKB(1f), equalTo(1e6f));

        assertThat(ByteUnit.GB.toKB(-1d), equalTo(-1e6d));
        assertThat(ByteUnit.GB.toKB(0d), equalTo(0d));
        assertThat(ByteUnit.GB.toKB(1d), equalTo(1e6d));
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertGigaToMega1()
    {
        ByteUnit.GB.toMB(Integer.MIN_VALUE+1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertGigaToMega2()
    {
        ByteUnit.GB.toMB(-2_147_484);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertGigaToMega3()
    {
        ByteUnit.GB.toMB(2_147_484);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertGigaToMega4()
    {
        ByteUnit.GB.toMB(Integer.MAX_VALUE-1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertGigaToMega5()
    {
        ByteUnit.GB.toMB(Long.MIN_VALUE+1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertGigaToMega6()
    {
        ByteUnit.GB.toMB(-9_223_372_036_854_776L);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertGigaToMega7()
    {
        ByteUnit.GB.toMB(9_223_372_036_854_776L);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertGigaToMega8()
    {
        ByteUnit.GB.toMB(Long.MAX_VALUE-1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertGigaToMega9()
    {
        ByteUnit.GB.toMB(Integer.MIN_VALUE);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertGigaToMega10()
    {
        ByteUnit.GB.toMB(Long.MIN_VALUE);
    }

    @Test
    public void shouldConvertGigaToMega()
    {
        assertThat(ByteUnit.GB.toMB(-2_147_483), equalTo(-2_147_483_000));
        assertThat(ByteUnit.GB.toMB(-1), equalTo(-1_000));
        assertThat(ByteUnit.GB.toMB(0), equalTo(0));
        assertThat(ByteUnit.GB.toMB(1), equalTo(1_000));
        assertThat(ByteUnit.GB.toMB(2_147_483), equalTo(2_147_483_000));
        assertThat(ByteUnit.GB.toMB(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.GB.toMB(-9_223_372_036_854_775L), equalTo(-9_223_372_036_854_775_000L));
        assertThat(ByteUnit.GB.toMB(-1L), equalTo(-1_000L));
        assertThat(ByteUnit.GB.toMB(0L), equalTo(0L));
        assertThat(ByteUnit.GB.toMB(1L), equalTo(1_000L));
        assertThat(ByteUnit.GB.toMB(9_223_372_036_854_775L), equalTo(9_223_372_036_854_775_000L));
        assertThat(ByteUnit.GB.toMB(Long.MAX_VALUE), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.GB.toMB(-1f), equalTo(-1_000f));
        assertThat(ByteUnit.GB.toMB(0f), equalTo(0f));
        assertThat(ByteUnit.GB.toMB(1f), equalTo(1_000f));

        assertThat(ByteUnit.GB.toMB(-1d), equalTo(-1_000d));
        assertThat(ByteUnit.GB.toMB(0d), equalTo(0d));
        assertThat(ByteUnit.GB.toMB(1d), equalTo(1_000d));
    }

    @Test
    public void shouldConvertFromGigaToGiga()
    {
        assertThat(ByteUnit.GB.toGB(Integer.MIN_VALUE), equalTo(Integer.MIN_VALUE));
        assertThat(ByteUnit.GB.toGB(-1), equalTo(-1));
        assertThat(ByteUnit.GB.toGB(0), equalTo(0));
        assertThat(ByteUnit.GB.toGB(1), equalTo(1));
        assertThat(ByteUnit.GB.toGB(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.GB.toGB(Long.MIN_VALUE), equalTo(Long.MIN_VALUE));
        assertThat(ByteUnit.GB.toGB(-1L), equalTo(-1L));
        assertThat(ByteUnit.GB.toGB(0L), equalTo(0L));
        assertThat(ByteUnit.GB.toGB(1L), equalTo(1L));
        assertThat(ByteUnit.GB.toGB(Long.MAX_VALUE), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.GB.toGB(-1f), equalTo(-1f));
        assertThat(ByteUnit.GB.toGB(0f), equalTo(0f));
        assertThat(ByteUnit.GB.toGB(1f), equalTo(1f));

        assertThat(ByteUnit.GB.toGB(-1d), equalTo(-1d));
        assertThat(ByteUnit.GB.toGB(0d), equalTo(0d));
        assertThat(ByteUnit.GB.toGB(1d), equalTo(1d));
    }

    @Test
    public void shouldConvertGigaToTera()
    {
        assertThat(ByteUnit.GB.toTB(Integer.MIN_VALUE), equalTo(-2147483));
        assertThat(ByteUnit.GB.toTB(-1_000), equalTo(-1));
        assertThat(ByteUnit.GB.toTB(-999), equalTo(0));
        assertThat(ByteUnit.GB.toTB(0), equalTo(0));
        assertThat(ByteUnit.GB.toTB(999), equalTo(0));
        assertThat(ByteUnit.GB.toTB(1_000), equalTo(1));
        assertThat(ByteUnit.GB.toTB(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.GB.toTB(Long.MIN_VALUE), equalTo(-9223372036854775L));
        assertThat(ByteUnit.GB.toTB(-1_000L), equalTo(-1L));
        assertThat(ByteUnit.GB.toTB(-999L), equalTo(0L));
        assertThat(ByteUnit.GB.toTB(0L), equalTo(0L));
        assertThat(ByteUnit.GB.toTB(999L), equalTo(0L));
        assertThat(ByteUnit.GB.toTB(1_000L), equalTo(1L));
        assertThat(ByteUnit.GB.toTB(Long.MAX_VALUE), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.GB.toTB(-1f), equalTo(-0.001f));
        assertThat(ByteUnit.GB.toTB(0f), equalTo(0f));
        assertThat(ByteUnit.GB.toTB(1f), equalTo(0.001f));

        assertThat(ByteUnit.GB.toTB(-1d), equalTo(-0.001d));
        assertThat(ByteUnit.GB.toTB(0d), equalTo(0d));
        assertThat(ByteUnit.GB.toTB(1d), equalTo(0.001d));
    }

    @Test
    public void shouldConvertGigaToPeta()
    {
        assertThat(ByteUnit.GB.toPB(Integer.MIN_VALUE), equalTo(-2147));
        assertThat(ByteUnit.GB.toPB(-1_000_000), equalTo(-1));
        assertThat(ByteUnit.GB.toPB(-999_999), equalTo(0));
        assertThat(ByteUnit.GB.toPB(0), equalTo(0));
        assertThat(ByteUnit.GB.toPB(999_999), equalTo(0));
        assertThat(ByteUnit.GB.toPB(1_000_000), equalTo(1));
        assertThat(ByteUnit.GB.toPB(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.GB.toPB(Long.MIN_VALUE), equalTo(-9223372036854L));
        assertThat(ByteUnit.GB.toPB(-1_000_000L), equalTo(-1L));
        assertThat(ByteUnit.GB.toPB(-999_999L), equalTo(0L));
        assertThat(ByteUnit.GB.toPB(0L), equalTo(0L));
        assertThat(ByteUnit.GB.toPB(999_999L), equalTo(0L));
        assertThat(ByteUnit.GB.toPB(1_000_000L), equalTo(1L));
        assertThat(ByteUnit.GB.toPB(Long.MAX_VALUE), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.GB.toPB(-1f), equalTo(-0.000_001f));
        assertThat(ByteUnit.GB.toPB(0f), equalTo(0f));
        assertThat(ByteUnit.GB.toPB(1f), equalTo(0.000_001f));

        assertThat(ByteUnit.GB.toPB(-1d), equalTo(-0.000_001d));
        assertThat(ByteUnit.GB.toPB(0d), equalTo(0d));
        assertThat(ByteUnit.GB.toPB(1d), equalTo(0.000_001d));
    }


    @Test
    public void shouldYieldPrefixOfGibi()
    {
        assertThat(ByteUnit.Type.BINARY.unitsOf(-1099511627775L), is(ByteUnit.GiB));
        assertThat(ByteUnit.Type.BINARY.unitsOf(-1073741824L), is(ByteUnit.GiB));
        assertThat(ByteUnit.Type.BINARY.unitsOf(1073741824L), is(ByteUnit.GiB));
        assertThat(ByteUnit.Type.BINARY.unitsOf(1099511627775L), is(ByteUnit.GiB));

        assertThat(ByteUnit.Type.BINARY.unitsOf(Integer.MIN_VALUE), is(ByteUnit.GiB));
        assertThat(ByteUnit.Type.BINARY.unitsOf(-1073741824), is(ByteUnit.GiB));
        assertThat(ByteUnit.Type.BINARY.unitsOf(1073741824), is(ByteUnit.GiB));
        assertThat(ByteUnit.Type.BINARY.unitsOf(Integer.MAX_VALUE), is(ByteUnit.GiB));
    }

    @Test
    public void shouldYieldExactUnitsOfGibi()
    {
        assertThat(ByteUnit.Type.BINARY.exactUnitsOf(-(1073741824L+1125899906842624L)), is(ByteUnit.GiB));
        assertThat(ByteUnit.Type.BINARY.exactUnitsOf(-(1073741824L+1099511627776L)), is(ByteUnit.GiB));
        assertThat(ByteUnit.Type.BINARY.exactUnitsOf(-2*1073741824L), is(ByteUnit.GiB));
        assertThat(ByteUnit.Type.BINARY.exactUnitsOf(-1073741824L), is(ByteUnit.GiB));
        assertThat(ByteUnit.Type.BINARY.exactUnitsOf(1073741824L), is(ByteUnit.GiB));
        assertThat(ByteUnit.Type.BINARY.exactUnitsOf(2*1073741824L), is(ByteUnit.GiB));
        assertThat(ByteUnit.Type.BINARY.exactUnitsOf(1073741824L+1099511627776L), is(ByteUnit.GiB));
        assertThat(ByteUnit.Type.BINARY.exactUnitsOf(1073741824L+1125899906842624L), is(ByteUnit.GiB));
    }

    @Test
    public void shouldClaimGibiType()
    {
        assertThat(ByteUnit.GiB.hasType(ByteUnit.Type.BINARY), equalTo(true));
        assertThat(ByteUnit.GiB.hasType(ByteUnit.Type.DECIMAL), equalTo(false));
    }

    @Test
    public void shouldConvertToGibi()
    {
        assertThat(ByteUnit.GiB.convert(5L * 1073741824L, ByteUnit.BYTES), equalTo(5L));
        assertThat(ByteUnit.GiB.convert(5L * 1048576L, ByteUnit.KiB), equalTo(5L));
        assertThat(ByteUnit.GiB.convert(5L * 1024L, ByteUnit.MiB), equalTo(5L));
        assertThat(ByteUnit.GiB.convert(5L, ByteUnit.GiB), equalTo(5L));
        assertThat(ByteUnit.GiB.convert(1L, ByteUnit.TiB), equalTo(1024L));
        assertThat(ByteUnit.GiB.convert(1L, ByteUnit.PiB), equalTo(1048576L));

        assertThat(ByteUnit.GiB.convert(Integer.MAX_VALUE, ByteUnit.BYTES), equalTo(Integer.MAX_VALUE));
        assertThat(ByteUnit.GiB.convert(5 * 1048576, ByteUnit.KiB), equalTo(5));
        assertThat(ByteUnit.GiB.convert(5 * 1024, ByteUnit.MiB), equalTo(5));
        assertThat(ByteUnit.GiB.convert(5, ByteUnit.GiB), equalTo(5));
        assertThat(ByteUnit.GiB.convert(1, ByteUnit.TiB), equalTo(1024));
        assertThat(ByteUnit.GiB.convert(1, ByteUnit.PiB), equalTo(1048576));

        assertThat(ByteUnit.GiB.convert(1073741824d, ByteUnit.BYTES), equalTo(1d));
        assertThat(ByteUnit.GiB.convert(1048576d, ByteUnit.KiB), equalTo(1d));
        assertThat(ByteUnit.GiB.convert(1024d, ByteUnit.MiB), equalTo(1d));
        assertThat(ByteUnit.GiB.convert(1d, ByteUnit.GiB), equalTo(1d));
        assertThat(ByteUnit.GiB.convert(1d, ByteUnit.TiB), equalTo(1024d));
        assertThat(ByteUnit.GiB.convert(1d, ByteUnit.PiB), equalTo(1048576d));
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertGibiToBytes1()
    {
        ByteUnit.GiB.toBytes(Integer.MIN_VALUE+1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertGibiToBytes2()
    {
        ByteUnit.GiB.toBytes(Integer.MAX_VALUE-1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertGibiToBytes3()
    {
        ByteUnit.GiB.toBytes(Long.MIN_VALUE+1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertGibiToBytes4()
    {
        ByteUnit.GiB.toBytes(Long.MAX_VALUE-1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertGibiToBytes5()
    {
        ByteUnit.GiB.toBytes(2);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertGibiToBytes6()
    {
        ByteUnit.GiB.toBytes(-8_589_934_593L);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertGibiToBytes7()
    {
        ByteUnit.GiB.toBytes(8_589_934_592L);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertGibiToBytes8()
    {
        ByteUnit.GiB.toBytes(Integer.MIN_VALUE);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertGibiToBytes9()
    {
        ByteUnit.GiB.toBytes(Long.MIN_VALUE);
    }

    @Test
    public void shouldConvertGibiToBytes()
    {
        assertThat(ByteUnit.GiB.toBytes(-2), equalTo(Integer.MIN_VALUE));
        assertThat(ByteUnit.GiB.toBytes(-1), equalTo(-1_073_741_824));
        assertThat(ByteUnit.GiB.toBytes(0), equalTo(0));
        assertThat(ByteUnit.GiB.toBytes(1), equalTo(1_073_741_824));
        assertThat(ByteUnit.GiB.toBytes(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.GiB.toBytes(-8_589_934_592L), equalTo(-9_223_372_036_854_775_808L));
        assertThat(ByteUnit.GiB.toBytes(-1L), equalTo(-1_073_741_824L));
        assertThat(ByteUnit.GiB.toBytes(0L), equalTo(0L));
        assertThat(ByteUnit.GiB.toBytes(1L), equalTo(1_073_741_824L));
        assertThat(ByteUnit.GiB.toBytes(8_589_934_591L), equalTo(9_223_372_035_781_033_984L));
        assertThat(ByteUnit.GiB.toBytes(Long.MAX_VALUE), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.GiB.toBytes(-1f), equalTo(-1073741824f));
        assertThat(ByteUnit.GiB.toBytes(0f), equalTo(0f));
        assertThat(ByteUnit.GiB.toBytes(1f), equalTo(1073741824f));

        assertThat(ByteUnit.GiB.toBytes(-1d), equalTo(-1073741824d));
        assertThat(ByteUnit.GiB.toBytes(0d), equalTo(0d));
        assertThat(ByteUnit.GiB.toBytes(1d), equalTo(1073741824d));
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertGibiToKibi()
    {
        assertThat(ByteUnit.GiB.toKiB(Integer.MIN_VALUE+1), equalTo(Integer.MIN_VALUE));
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertGibiToKibi2()
    {
        assertThat(ByteUnit.GiB.toKiB(-2049), equalTo(Integer.MIN_VALUE));
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertGibiToKibi3()
    {
        assertThat(ByteUnit.GiB.toKiB(2049), equalTo(Integer.MAX_VALUE));
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertGibiToKibi4()
    {
        assertThat(ByteUnit.GiB.toKiB(Integer.MAX_VALUE-1), equalTo(Integer.MAX_VALUE));
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertGibiToKibi5()
    {
        ByteUnit.GiB.toKiB(Long.MIN_VALUE+1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertGibiToKibi6()
    {
        ByteUnit.GiB.toKiB(Long.MAX_VALUE-1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertGibiToKibi7()
    {
        ByteUnit.GiB.toKiB(2048);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertGibiToKibi8()
    {
        ByteUnit.GiB.toKiB(8_796_093_022_208L);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertGibiToKibi9()
    {
        ByteUnit.GiB.toKiB(Integer.MIN_VALUE);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertGibiToKibi10()
    {
        ByteUnit.GiB.toKiB(Long.MIN_VALUE);
    }

    @Test
    public void shouldConvertGibiToKibi()
    {
        assertThat(ByteUnit.GiB.toKiB(-2048), equalTo(Integer.MIN_VALUE));
        assertThat(ByteUnit.GiB.toKiB(-2047), equalTo(-2_146_435_072));
        assertThat(ByteUnit.GiB.toKiB(-1), equalTo(-1_048_576));
        assertThat(ByteUnit.GiB.toKiB(0), equalTo(0));
        assertThat(ByteUnit.GiB.toKiB(1), equalTo(1_048_576));
        assertThat(ByteUnit.GiB.toKiB(2047), equalTo(2_146_435_072));
        assertThat(ByteUnit.GiB.toKiB(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.GiB.toKiB(-8_796_093_022_208L), equalTo(Long.MIN_VALUE));
        assertThat(ByteUnit.GiB.toKiB(-8_796_093_022_207L), equalTo(-9_223_372_036_853_727_232L));
        assertThat(ByteUnit.GiB.toKiB(-1L), equalTo(-1048576L));
        assertThat(ByteUnit.GiB.toKiB(0L), equalTo(0L));
        assertThat(ByteUnit.GiB.toKiB(1L), equalTo(1048576L));
        assertThat(ByteUnit.GiB.toKiB(8_796_093_022_207L), equalTo(9_223_372_036_853_727_232L));
        assertThat(ByteUnit.GiB.toKiB(Long.MAX_VALUE), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.GiB.toKiB(-1f), equalTo(-1048576f));
        assertThat(ByteUnit.GiB.toKiB(0f), equalTo(0f));
        assertThat(ByteUnit.GiB.toKiB(1f), equalTo(1048576f));

        assertThat(ByteUnit.GiB.toKiB(-1d), equalTo(-1048576d));
        assertThat(ByteUnit.GiB.toKiB(0d), equalTo(0d));
        assertThat(ByteUnit.GiB.toKiB(1d), equalTo(1048576d));
    }


    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertGibiToMebi1()
    {
        ByteUnit.GiB.toMiB(Integer.MIN_VALUE+1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertGibiToMebi2()
    {
        ByteUnit.GiB.toMiB(Integer.MAX_VALUE-1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertGibiToMebi3()
    {
        ByteUnit.GiB.toMiB(Long.MIN_VALUE+1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertGibiToMebi4()
    {
        ByteUnit.GiB.toMiB(Long.MAX_VALUE-1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertGibiToMebi5()
    {
        ByteUnit.GiB.toMiB(2_097_152);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertGibiToMebi6()
    {
        ByteUnit.GiB.toMiB(9_007_199_254_740_992L);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertGibiToMebi7()
    {
        ByteUnit.GiB.toMiB(Integer.MIN_VALUE);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertGibiToMebi8()
    {
        ByteUnit.GiB.toMiB(Long.MIN_VALUE);
    }
    @Test
    public void shouldConvertGibiToMebi()
    {
        assertThat(ByteUnit.GiB.toMiB(-2_097_152), equalTo(Integer.MIN_VALUE));
        assertThat(ByteUnit.GiB.toMiB(-2_097_151), equalTo(-2_147_482_624));
        assertThat(ByteUnit.GiB.toMiB(-1), equalTo(-1024));
        assertThat(ByteUnit.GiB.toMiB(0), equalTo(0));
        assertThat(ByteUnit.GiB.toMiB(1), equalTo(1024));
        assertThat(ByteUnit.GiB.toMiB(2_097_151), equalTo(2_147_482_624));
        assertThat(ByteUnit.GiB.toMiB(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.GiB.toMiB(-9_007_199_254_740_992L), equalTo(Long.MIN_VALUE));
        assertThat(ByteUnit.GiB.toMiB(-9_007_199_254_740_991L), equalTo(-9_223_372_036_854_774_784L));
        assertThat(ByteUnit.GiB.toMiB(-1L), equalTo(-1024L));
        assertThat(ByteUnit.GiB.toMiB(0L), equalTo(0L));
        assertThat(ByteUnit.GiB.toMiB(1L), equalTo(1024L));
        assertThat(ByteUnit.GiB.toMiB(9_007_199_254_740_991L), equalTo(9_223_372_036_854_774_784L));
        assertThat(ByteUnit.GiB.toMiB(Long.MAX_VALUE), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.GiB.toMiB(-1f), equalTo(-1024f));
        assertThat(ByteUnit.GiB.toMiB(0f), equalTo(0f));
        assertThat(ByteUnit.GiB.toMiB(1f), equalTo(1024f));

        assertThat(ByteUnit.GiB.toMiB(-1d), equalTo(-1024d));
        assertThat(ByteUnit.GiB.toMiB(0d), equalTo(0d));
        assertThat(ByteUnit.GiB.toMiB(1d), equalTo(1024d));
    }

    @Test
    public void shouldConvertFromGibiToGibi()
    {
        assertThat(ByteUnit.GiB.toGiB(Integer.MIN_VALUE), equalTo(Integer.MIN_VALUE));
        assertThat(ByteUnit.GiB.toGiB(-1), equalTo(-1));
        assertThat(ByteUnit.GiB.toGiB(0), equalTo(0));
        assertThat(ByteUnit.GiB.toGiB(1), equalTo(1));
        assertThat(ByteUnit.GiB.toGiB(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.GiB.toGiB(Long.MIN_VALUE), equalTo(Long.MIN_VALUE));
        assertThat(ByteUnit.GiB.toGiB(-1L), equalTo(-1L));
        assertThat(ByteUnit.GiB.toGiB(0L), equalTo(0L));
        assertThat(ByteUnit.GiB.toGiB(1L), equalTo(1L));
        assertThat(ByteUnit.GiB.toGiB(Long.MAX_VALUE), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.GiB.toGiB(-1f), equalTo(-1f));
        assertThat(ByteUnit.GiB.toGiB(0f), equalTo(0f));
        assertThat(ByteUnit.GiB.toGiB(1f), equalTo(1f));

        assertThat(ByteUnit.GiB.toGiB(-1d), equalTo(-1d));
        assertThat(ByteUnit.GiB.toGiB(0d), equalTo(0d));
        assertThat(ByteUnit.GiB.toGiB(1d), equalTo(1d));
    }

    @Test
    public void shouldConvertFromGibiToTebi()
    {
        assertThat(ByteUnit.GiB.toTiB(Integer.MIN_VALUE), equalTo(-2097152));
        assertThat(ByteUnit.GiB.toTiB(-1024), equalTo(-1));
        assertThat(ByteUnit.GiB.toTiB(-1023), equalTo(0));
        assertThat(ByteUnit.GiB.toTiB(0), equalTo(0));
        assertThat(ByteUnit.GiB.toTiB(1023), equalTo(0));
        assertThat(ByteUnit.GiB.toTiB(1024), equalTo(1));
        assertThat(ByteUnit.GiB.toTiB(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.GiB.toTiB(Long.MIN_VALUE), equalTo(-9007199254740992L));
        assertThat(ByteUnit.GiB.toTiB(-1024L), equalTo(-1L));
        assertThat(ByteUnit.GiB.toTiB(-1023L), equalTo(0L));
        assertThat(ByteUnit.GiB.toTiB(0L), equalTo(0L));
        assertThat(ByteUnit.GiB.toTiB(1023L), equalTo(0L));
        assertThat(ByteUnit.GiB.toTiB(1024L), equalTo(1L));
        assertThat(ByteUnit.GiB.toTiB(Long.MAX_VALUE), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.GiB.toTiB(-1f), equalTo(-0.0009765625f));
        assertThat(ByteUnit.GiB.toTiB(0f), equalTo(0f));
        assertThat(ByteUnit.GiB.toTiB(1f), equalTo(0.0009765625f));

        assertThat(ByteUnit.GiB.toTiB(-1d), equalTo(-0.0009765625d));
        assertThat(ByteUnit.GiB.toTiB(0d), equalTo(0d));
        assertThat(ByteUnit.GiB.toTiB(1d), equalTo(0.0009765625d));
    }

    @Test
    public void shouldConvertFromGebiToPebi()
    {
        assertThat(ByteUnit.GiB.toPiB(Integer.MIN_VALUE), equalTo(-2048));
        assertThat(ByteUnit.GiB.toPiB(-1_048_576), equalTo(-1));
        assertThat(ByteUnit.GiB.toPiB(-1_048_575), equalTo(0));
        assertThat(ByteUnit.GiB.toPiB(0), equalTo(0));
        assertThat(ByteUnit.GiB.toPiB(1_048_575), equalTo(0));
        assertThat(ByteUnit.GiB.toPiB(1_048_576), equalTo(1));
        assertThat(ByteUnit.GiB.toPiB(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.GiB.toPiB(Long.MIN_VALUE), equalTo(-8796093022208L));
        assertThat(ByteUnit.GiB.toPiB(-1_048_576L), equalTo(-1L));
        assertThat(ByteUnit.GiB.toPiB(-1_048_575L), equalTo(0L));
        assertThat(ByteUnit.GiB.toPiB(0L), equalTo(0L));
        assertThat(ByteUnit.GiB.toPiB(1_048_575L), equalTo(0L));
        assertThat(ByteUnit.GiB.toPiB(1_048_576L), equalTo(1L));
        assertThat(ByteUnit.GiB.toPiB(Long.MAX_VALUE), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.GiB.toPiB(-1f), equalTo(-9.536743e-7f));
        assertThat(ByteUnit.GiB.toPiB(0f), equalTo(0f));
        assertThat(ByteUnit.GiB.toPiB(1f), equalTo(9.536743e-7f));

        assertThat(ByteUnit.GiB.toPiB(-1d), equalTo(-9.5367431640625e-7d));
        assertThat(ByteUnit.GiB.toPiB(0d), equalTo(0d));
        assertThat(ByteUnit.GiB.toPiB(1d), equalTo(9.5367431640625e-7d));
    }


    @Test
    public void shouldYieldPrefixOfTera()
    {
        assertThat(ByteUnit.Type.DECIMAL.unitsOf(-999_999_999_999_999L), is(ByteUnit.TB));
        assertThat(ByteUnit.Type.DECIMAL.unitsOf(-1_000_000_000_000L), is(ByteUnit.TB));
        assertThat(ByteUnit.Type.DECIMAL.unitsOf(1_000_000_000_000L), is(ByteUnit.TB));
        assertThat(ByteUnit.Type.DECIMAL.unitsOf(999_999_999_999_999L), is(ByteUnit.TB));
    }

    @Test
    public void shouldYieldExactUnitsOfTera()
    {
        assertThat(ByteUnit.Type.DECIMAL.exactUnitsOf(-1_001_000_000_000_000L), is(ByteUnit.TB));
        assertThat(ByteUnit.Type.DECIMAL.exactUnitsOf(-2_000_000_000_000L), is(ByteUnit.TB));
        assertThat(ByteUnit.Type.DECIMAL.exactUnitsOf(-1_000_000_000_000L), is(ByteUnit.TB));
        assertThat(ByteUnit.Type.DECIMAL.exactUnitsOf(1_000_000_000_000L), is(ByteUnit.TB));
        assertThat(ByteUnit.Type.DECIMAL.exactUnitsOf(2_000_000_000_000L), is(ByteUnit.TB));
        assertThat(ByteUnit.Type.DECIMAL.exactUnitsOf(1_001_000_000_000_000L), is(ByteUnit.TB));
    }

    @Test
    public void shouldClaimTeraType()
    {
        assertThat(ByteUnit.TB.hasType(ByteUnit.Type.BINARY), equalTo(false));
        assertThat(ByteUnit.TB.hasType(ByteUnit.Type.DECIMAL), equalTo(true));
    }

    @Test
    public void shouldConvertToTera()
    {
        assertThat(ByteUnit.TB.convert(5_000_000_000_000L, ByteUnit.BYTES), equalTo(5L));
        assertThat(ByteUnit.TB.convert(5_000_000_000L, ByteUnit.KB), equalTo(5L));
        assertThat(ByteUnit.TB.convert(5_000_000L, ByteUnit.MB), equalTo(5L));
        assertThat(ByteUnit.TB.convert(5_000L, ByteUnit.GB), equalTo(5L));
        assertThat(ByteUnit.TB.convert(5L, ByteUnit.TB), equalTo(5L));
        assertThat(ByteUnit.TB.convert(1L, ByteUnit.PB), equalTo(1_000L));

        assertThat(ByteUnit.TB.convert(Integer.MAX_VALUE, ByteUnit.BYTES), equalTo(Integer.MAX_VALUE));
        assertThat(ByteUnit.TB.convert(2_000_000_000, ByteUnit.KB), equalTo(2));
        assertThat(ByteUnit.TB.convert(5_000_000, ByteUnit.MB), equalTo(5));
        assertThat(ByteUnit.TB.convert(5_000, ByteUnit.GB), equalTo(5));
        assertThat(ByteUnit.TB.convert(5, ByteUnit.TB), equalTo(5));
        assertThat(ByteUnit.TB.convert(1, ByteUnit.PB), equalTo(1_000));

        assertThat(ByteUnit.TB.convert(1_000_000_000_000d, ByteUnit.BYTES), equalTo(1d));
        assertThat(ByteUnit.TB.convert(1_000_000_000d, ByteUnit.KB), equalTo(1d));
        assertThat(ByteUnit.TB.convert(1_000_000d, ByteUnit.MB), equalTo(1d));
        assertThat(ByteUnit.TB.convert(1_000d, ByteUnit.GB), equalTo(1d));
        assertThat(ByteUnit.TB.convert(1d, ByteUnit.TB), equalTo(1d));
        assertThat(ByteUnit.TB.convert(1d, ByteUnit.PB), equalTo(1_000d));
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertTeraToBytes1()
    {
        ByteUnit.TB.toBytes(Integer.MIN_VALUE+1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertTeraToBytes2()
    {
        ByteUnit.TB.toBytes(-1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertTeraToBytes3()
    {
        ByteUnit.TB.toBytes(1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertTeraToBytes4()
    {
        ByteUnit.TB.toBytes(Integer.MAX_VALUE-1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertTeraToBytes5()
    {
        ByteUnit.TB.toBytes(Long.MIN_VALUE+1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertTeraToBytes6()
    {
        ByteUnit.TB.toBytes(-9_223_373L);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertTeraToBytes7()
    {
        ByteUnit.TB.toBytes(9_223_373L);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertTeraToBytes8()
    {
        ByteUnit.TB.toBytes(Long.MAX_VALUE-1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertTeraToBytes9()
    {
        ByteUnit.TB.toBytes(Integer.MIN_VALUE);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertTeraToBytes10()
    {
        ByteUnit.TB.toBytes(Long.MIN_VALUE);
    }

    @Test
    public void shouldConvertTeraToBytes()
    {
        assertThat(ByteUnit.TB.toBytes(0), equalTo(0));
        assertThat(ByteUnit.TB.toBytes(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.TB.toBytes(-9_223_372L), equalTo(-9_223_372_000_000_000_000L));
        assertThat(ByteUnit.TB.toBytes(-1L), equalTo(-1_000_000_000_000L));
        assertThat(ByteUnit.TB.toBytes(0L), equalTo(0L));
        assertThat(ByteUnit.TB.toBytes(1L), equalTo(1_000_000_000_000L));
        assertThat(ByteUnit.TB.toBytes(9_223_372L), equalTo(9_223_372_000_000_000_000L));
        assertThat(ByteUnit.TB.toBytes(Long.MAX_VALUE), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.TB.toBytes(-1f), equalTo(-1e12f));
        assertThat(ByteUnit.TB.toBytes(0f), equalTo(0f));
        assertThat(ByteUnit.TB.toBytes(1f), equalTo(1e12f));

        assertThat(ByteUnit.TB.toBytes(-1d), equalTo(-1e12d));
        assertThat(ByteUnit.TB.toBytes(0d), equalTo(0d));
        assertThat(ByteUnit.TB.toBytes(1d), equalTo(1e12d));
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertTeraToKilo1()
    {
        ByteUnit.TB.toKB(Integer.MIN_VALUE+1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertTeraToKilo2()
    {
        ByteUnit.TB.toKB(-3);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertTeraToKilo3()
    {
        ByteUnit.TB.toKB(3);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertTeraToKilo4()
    {
        ByteUnit.TB.toKB(Integer.MAX_VALUE-1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertTeraToKilo5()
    {
        ByteUnit.TB.toKB(Long.MIN_VALUE+1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertTeraToKilo6()
    {
        ByteUnit.TB.toKB(-9_223_372_037L);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertTeraToKilo7()
    {
        ByteUnit.TB.toKB(9_223_372_037L);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertTeraToKilo8()
    {
        ByteUnit.TB.toKB(Long.MAX_VALUE-1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertTeraToKilo9()
    {
        ByteUnit.TB.toKB(Integer.MIN_VALUE);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertTeraToKilo10()
    {
        ByteUnit.TB.toKB(Long.MIN_VALUE);
    }

    @Test
    public void shouldConvertTeraToKilo()
    {
        assertThat(ByteUnit.TB.toKB(-2), equalTo(-2_000_000_000));
        assertThat(ByteUnit.TB.toKB(-1), equalTo(-1_000_000_000));
        assertThat(ByteUnit.TB.toKB(0), equalTo(0));
        assertThat(ByteUnit.TB.toKB(1), equalTo(1_000_000_000));
        assertThat(ByteUnit.TB.toKB(2), equalTo(2_000_000_000));
        assertThat(ByteUnit.TB.toKB(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.TB.toKB(-9_223_372_036L), equalTo(-9_223_372_036_000_000_000L));
        assertThat(ByteUnit.TB.toKB(-1L), equalTo(-1_000_000_000L));
        assertThat(ByteUnit.TB.toKB(0L), equalTo(0L));
        assertThat(ByteUnit.TB.toKB(1L), equalTo(1_000_000_000L));
        assertThat(ByteUnit.TB.toKB(9_223_372_036L), equalTo(9_223_372_036_000_000_000L));
        assertThat(ByteUnit.TB.toKB(Long.MAX_VALUE), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.TB.toKB(-1f), equalTo(-1e9f));
        assertThat(ByteUnit.TB.toKB(0f), equalTo(0f));
        assertThat(ByteUnit.TB.toKB(1f), equalTo(1e9f));

        assertThat(ByteUnit.TB.toKB(-1d), equalTo(-1e9d));
        assertThat(ByteUnit.TB.toKB(0d), equalTo(0d));
        assertThat(ByteUnit.TB.toKB(1d), equalTo(1e9d));
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertTeraToMega1()
    {
        ByteUnit.TB.toMB(Integer.MIN_VALUE+1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertTeraToMega2()
    {
        ByteUnit.TB.toMB(-2_148);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertTeraToMega3()
    {
        ByteUnit.TB.toMB(2_148);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertTeraToMega4()
    {
        ByteUnit.TB.toMB(Integer.MAX_VALUE-1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertTeraToMega5()
    {
        ByteUnit.TB.toMB(Long.MIN_VALUE+1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertTeraToMega6()
    {
        ByteUnit.TB.toMB(-9_223_372_036_855L);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertTeraToMega7()
    {
        ByteUnit.TB.toMB(9_223_372_036_855L);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertTeraToMega8()
    {
        ByteUnit.TB.toMB(Long.MAX_VALUE-1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertTeraToMega9()
    {
        ByteUnit.TB.toMB(Integer.MIN_VALUE);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertTeraToMega10()
    {
        ByteUnit.TB.toMB(Long.MIN_VALUE);
    }

    @Test
    public void shouldConvertTeraToMega()
    {
        assertThat(ByteUnit.TB.toMB(-2_147), equalTo(-2_147_000_000));
        assertThat(ByteUnit.TB.toMB(-1), equalTo(-1_000_000));
        assertThat(ByteUnit.TB.toMB(0), equalTo(0));
        assertThat(ByteUnit.TB.toMB(1), equalTo(1_000_000));
        assertThat(ByteUnit.TB.toMB(2_147), equalTo(2_147_000_000));
        assertThat(ByteUnit.TB.toMB(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.TB.toMB(-9_223_372_036_854L), equalTo(-9_223_372_036_854_000_000L));
        assertThat(ByteUnit.TB.toMB(-1L), equalTo(-1_000_000L));
        assertThat(ByteUnit.TB.toMB(0L), equalTo(0L));
        assertThat(ByteUnit.TB.toMB(1L), equalTo(1_000_000L));
        assertThat(ByteUnit.TB.toMB(9_223_372_036_854L), equalTo(9_223_372_036_854_000_000L));
        assertThat(ByteUnit.TB.toMB(Long.MAX_VALUE), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.TB.toMB(-1f), equalTo(-1e6f));
        assertThat(ByteUnit.TB.toMB(0f), equalTo(0f));
        assertThat(ByteUnit.TB.toMB(1f), equalTo(1e6f));

        assertThat(ByteUnit.TB.toMB(-1d), equalTo(-1e6d));
        assertThat(ByteUnit.TB.toMB(0d), equalTo(0d));
        assertThat(ByteUnit.TB.toMB(1d), equalTo(1e6d));
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertTeraToGiga1()
    {
        ByteUnit.TB.toGB(Integer.MIN_VALUE+1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertTeraToGiga2()
    {
        ByteUnit.TB.toGB(-2_147_484);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertTeraToGiga3()
    {
        ByteUnit.TB.toGB(2_147_484);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertTeraToGiga4()
    {
        ByteUnit.TB.toGB(Integer.MAX_VALUE-1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertTeraToGiga5()
    {
        ByteUnit.TB.toGB(Long.MIN_VALUE+1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertTeraToGiga6()
    {
        ByteUnit.TB.toGB(-9_223_372_036_854_776L);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertTeraToGiga7()
    {
        ByteUnit.TB.toGB(9_223_372_036_854_776L);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertTeraToGiga8()
    {
        ByteUnit.TB.toGB(Long.MAX_VALUE-1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertTeraToGiga9()
    {
        ByteUnit.TB.toGB(Integer.MIN_VALUE);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertTeraToGiga10()
    {
        ByteUnit.TB.toGB(Long.MIN_VALUE);
    }

    @Test
    public void shouldConvertTeraToGiga()
    {
        assertThat(ByteUnit.TB.toGB(-2_147_483), equalTo(-2_147_483_000));
        assertThat(ByteUnit.TB.toGB(-1), equalTo(-1_000));
        assertThat(ByteUnit.TB.toGB(0), equalTo(0));
        assertThat(ByteUnit.TB.toGB(1), equalTo(1_000));
        assertThat(ByteUnit.TB.toGB(2_147_483), equalTo(2_147_483_000));
        assertThat(ByteUnit.TB.toGB(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.TB.toGB(-9_223_372_036_854_775L), equalTo(-9_223_372_036_854_775_000L));
        assertThat(ByteUnit.TB.toGB(-1L), equalTo(-1_000L));
        assertThat(ByteUnit.TB.toGB(0L), equalTo(0L));
        assertThat(ByteUnit.TB.toGB(1L), equalTo(1_000L));
        assertThat(ByteUnit.TB.toGB(9_223_372_036_854_775L), equalTo(9_223_372_036_854_775_000L));
        assertThat(ByteUnit.TB.toGB(Long.MAX_VALUE), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.TB.toGB(-1f), equalTo(-1_000f));
        assertThat(ByteUnit.TB.toGB(0f), equalTo(0f));
        assertThat(ByteUnit.TB.toGB(1f), equalTo(1_000f));

        assertThat(ByteUnit.TB.toGB(-1d), equalTo(-1_000d));
        assertThat(ByteUnit.TB.toGB(0d), equalTo(0d));
        assertThat(ByteUnit.TB.toGB(1d), equalTo(1_000d));
    }

    @Test
    public void shouldConvertFromTeraToTera()
    {
        assertThat(ByteUnit.TB.toTB(Integer.MIN_VALUE), equalTo(Integer.MIN_VALUE));
        assertThat(ByteUnit.TB.toTB(-1), equalTo(-1));
        assertThat(ByteUnit.TB.toTB(0), equalTo(0));
        assertThat(ByteUnit.TB.toTB(1), equalTo(1));
        assertThat(ByteUnit.TB.toTB(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.TB.toTB(Long.MIN_VALUE), equalTo(Long.MIN_VALUE));
        assertThat(ByteUnit.TB.toTB(-1L), equalTo(-1L));
        assertThat(ByteUnit.TB.toTB(0L), equalTo(0L));
        assertThat(ByteUnit.TB.toTB(1L), equalTo(1L));
        assertThat(ByteUnit.TB.toTB(Long.MAX_VALUE), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.TB.toTB(-1f), equalTo(-1f));
        assertThat(ByteUnit.TB.toTB(0f), equalTo(0f));
        assertThat(ByteUnit.TB.toTB(1f), equalTo(1f));

        assertThat(ByteUnit.TB.toTB(-1d), equalTo(-1d));
        assertThat(ByteUnit.TB.toTB(0d), equalTo(0d));
        assertThat(ByteUnit.TB.toTB(1d), equalTo(1d));
    }

    @Test
    public void shouldConvertTeraToPeta()
    {
        assertThat(ByteUnit.TB.toPB(Integer.MIN_VALUE), equalTo(-2147483));
        assertThat(ByteUnit.TB.toPB(-1_000), equalTo(-1));
        assertThat(ByteUnit.TB.toPB(-999), equalTo(0));
        assertThat(ByteUnit.TB.toPB(0), equalTo(0));
        assertThat(ByteUnit.TB.toPB(999), equalTo(0));
        assertThat(ByteUnit.TB.toPB(1_000), equalTo(1));
        assertThat(ByteUnit.TB.toPB(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.TB.toPB(Long.MIN_VALUE), equalTo(-9223372036854775L));
        assertThat(ByteUnit.TB.toPB(-1_000L), equalTo(-1L));
        assertThat(ByteUnit.TB.toPB(-999L), equalTo(0L));
        assertThat(ByteUnit.TB.toPB(0L), equalTo(0L));
        assertThat(ByteUnit.TB.toPB(999L), equalTo(0L));
        assertThat(ByteUnit.TB.toPB(1_000L), equalTo(1L));
        assertThat(ByteUnit.TB.toPB(Long.MAX_VALUE), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.TB.toPB(-1f), equalTo(-0.001f));
        assertThat(ByteUnit.TB.toPB(0f), equalTo(0f));
        assertThat(ByteUnit.TB.toPB(1f), equalTo(0.001f));

        assertThat(ByteUnit.TB.toPB(-1d), equalTo(-0.001d));
        assertThat(ByteUnit.TB.toPB(0d), equalTo(0d));
        assertThat(ByteUnit.TB.toPB(1d), equalTo(0.001d));
    }


    @Test
    public void shouldYieldPrefixOfTebi()
    {
        assertThat(ByteUnit.Type.BINARY.unitsOf(-1125899906842623L), is(ByteUnit.TiB));
        assertThat(ByteUnit.Type.BINARY.unitsOf(-1099511627776L), is(ByteUnit.TiB));
        assertThat(ByteUnit.Type.BINARY.unitsOf(1099511627776L), is(ByteUnit.TiB));
        assertThat(ByteUnit.Type.BINARY.unitsOf(1125899906842623L), is(ByteUnit.TiB));
    }

    @Test
    public void shouldYieldExactUnitsOfTebi()
    {
        assertThat(ByteUnit.Type.BINARY.exactUnitsOf(-(1099511627776L+1125899906842624L)), is(ByteUnit.TiB));
        assertThat(ByteUnit.Type.BINARY.exactUnitsOf(-2*1099511627776L), is(ByteUnit.TiB));
        assertThat(ByteUnit.Type.BINARY.exactUnitsOf(-1099511627776L), is(ByteUnit.TiB));
        assertThat(ByteUnit.Type.BINARY.exactUnitsOf(1099511627776L), is(ByteUnit.TiB));
        assertThat(ByteUnit.Type.BINARY.exactUnitsOf(2*1099511627776L), is(ByteUnit.TiB));
        assertThat(ByteUnit.Type.BINARY.exactUnitsOf(1099511627776L+1125899906842624L), is(ByteUnit.TiB));
    }

    @Test
    public void shouldClaimTebiType()
    {
        assertThat(ByteUnit.TiB.hasType(ByteUnit.Type.BINARY), equalTo(true));
        assertThat(ByteUnit.TiB.hasType(ByteUnit.Type.DECIMAL), equalTo(false));
    }

    @Test
    public void shouldConvertToTebi()
    {
        assertThat(ByteUnit.TiB.convert(Long.MAX_VALUE, ByteUnit.BYTES), equalTo(Long.MAX_VALUE));
        assertThat(ByteUnit.TiB.convert(Long.MAX_VALUE, ByteUnit.KB), equalTo(Long.MAX_VALUE));
        assertThat(ByteUnit.TiB.convert(Long.MAX_VALUE, ByteUnit.KiB), equalTo(Long.MAX_VALUE));
        assertThat(ByteUnit.TiB.convert(Long.MAX_VALUE, ByteUnit.MB), equalTo(Long.MAX_VALUE));
        assertThat(ByteUnit.TiB.convert(Long.MAX_VALUE, ByteUnit.MiB), equalTo(Long.MAX_VALUE));
        assertThat(ByteUnit.TiB.convert(Long.MAX_VALUE, ByteUnit.GB), equalTo(Long.MAX_VALUE));
        assertThat(ByteUnit.TiB.convert(Long.MAX_VALUE, ByteUnit.GiB), equalTo(Long.MAX_VALUE));
        assertThat(ByteUnit.TiB.convert(Long.MAX_VALUE, ByteUnit.TB), equalTo(Long.MAX_VALUE));
        assertThat(ByteUnit.TiB.convert(Long.MAX_VALUE, ByteUnit.TiB), equalTo(Long.MAX_VALUE));
        assertThat(ByteUnit.TiB.convert(Long.MAX_VALUE, ByteUnit.PB), equalTo(Long.MAX_VALUE));
        assertThat(ByteUnit.TiB.convert(Long.MAX_VALUE, ByteUnit.PiB), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.TiB.convert(5L * 1099511627776L, ByteUnit.BYTES), equalTo(5L));
        assertThat(ByteUnit.TiB.convert(5L * 1073741824L, ByteUnit.KiB), equalTo(5L));
        assertThat(ByteUnit.TiB.convert(5L * 1048576L, ByteUnit.MiB), equalTo(5L));
        assertThat(ByteUnit.TiB.convert(5L * 1024L, ByteUnit.GiB), equalTo(5L));
        assertThat(ByteUnit.TiB.convert(5L, ByteUnit.TiB), equalTo(5L));
        assertThat(ByteUnit.TiB.convert(1L, ByteUnit.PiB), equalTo(1024L));

        assertThat(ByteUnit.TiB.convert(Integer.MAX_VALUE, ByteUnit.BYTES), equalTo(Integer.MAX_VALUE));
        assertThat(ByteUnit.TiB.convert(Integer.MAX_VALUE, ByteUnit.KB), equalTo(Integer.MAX_VALUE));
        assertThat(ByteUnit.TiB.convert(Integer.MAX_VALUE, ByteUnit.KiB), equalTo(Integer.MAX_VALUE));
        assertThat(ByteUnit.TiB.convert(Integer.MAX_VALUE, ByteUnit.MB), equalTo(Integer.MAX_VALUE));
        assertThat(ByteUnit.TiB.convert(Integer.MAX_VALUE, ByteUnit.MiB), equalTo(Integer.MAX_VALUE));
        assertThat(ByteUnit.TiB.convert(Integer.MAX_VALUE, ByteUnit.GB), equalTo(Integer.MAX_VALUE));
        assertThat(ByteUnit.TiB.convert(Integer.MAX_VALUE, ByteUnit.GiB), equalTo(Integer.MAX_VALUE));
        assertThat(ByteUnit.TiB.convert(Integer.MAX_VALUE, ByteUnit.TB), equalTo(Integer.MAX_VALUE));
        assertThat(ByteUnit.TiB.convert(Integer.MAX_VALUE, ByteUnit.TiB), equalTo(Integer.MAX_VALUE));
        assertThat(ByteUnit.TiB.convert(Integer.MAX_VALUE, ByteUnit.PB), equalTo(Integer.MAX_VALUE));
        assertThat(ByteUnit.TiB.convert(Integer.MAX_VALUE, ByteUnit.PiB), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.TiB.convert(5 * 1048576, ByteUnit.MiB), equalTo(5));
        assertThat(ByteUnit.TiB.convert(5 * 1024, ByteUnit.GiB), equalTo(5));
        assertThat(ByteUnit.TiB.convert(5, ByteUnit.TiB), equalTo(5));
        assertThat(ByteUnit.TiB.convert(1, ByteUnit.PiB), equalTo(1024));

        assertThat(ByteUnit.TiB.convert(1099511627776d, ByteUnit.BYTES), equalTo(1d));
        assertThat(ByteUnit.TiB.convert(1073741824d, ByteUnit.KiB), equalTo(1d));
        assertThat(ByteUnit.TiB.convert(1048576d, ByteUnit.MiB), equalTo(1d));
        assertThat(ByteUnit.TiB.convert(1024d, ByteUnit.GiB), equalTo(1d));
        assertThat(ByteUnit.TiB.convert(1d, ByteUnit.TiB), equalTo(1d));
        assertThat(ByteUnit.TiB.convert(1d, ByteUnit.PiB), equalTo(1024d));
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertTebiToBytes1()
    {
        ByteUnit.TiB.toBytes(Integer.MIN_VALUE+1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertTebiToBytes2()
    {
        ByteUnit.TiB.toBytes(-1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertTebiToBytes3()
    {
        ByteUnit.TiB.toBytes(1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertTebiToBytes4()
    {
        ByteUnit.TiB.toBytes(Integer.MAX_VALUE-1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertTebiToBytes5()
    {
        ByteUnit.TiB.toBytes(Long.MIN_VALUE+1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertTebiToBytes6()
    {
        ByteUnit.TiB.toBytes(8_388_608L);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertTebiToBytes7()
    {
        ByteUnit.TiB.toBytes(Long.MAX_VALUE-1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertTebiToBytes8()
    {
        ByteUnit.TiB.toBytes(Integer.MIN_VALUE);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertTebiToBytes9()
    {
        ByteUnit.TiB.toBytes(Long.MIN_VALUE);
    }

    @Test
    public void shouldConvertTebiToBytes()
    {
        assertThat(ByteUnit.TiB.toBytes(0), equalTo(0));
        assertThat(ByteUnit.TiB.toBytes(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.TiB.toBytes(-8_388_608L), equalTo(Long.MIN_VALUE));
        assertThat(ByteUnit.TiB.toBytes(-8_388_607L), equalTo(-9_223_370_937_343_148_032L));
        assertThat(ByteUnit.TiB.toBytes(-1L), equalTo(-1_099_511_627_776L));
        assertThat(ByteUnit.TiB.toBytes(0L), equalTo(0L));
        assertThat(ByteUnit.TiB.toBytes(1L), equalTo(1_099_511_627_776L));
        assertThat(ByteUnit.TiB.toBytes(8_388_607L), equalTo(9_223_370_937_343_148_032L));
        assertThat(ByteUnit.TiB.toBytes(Long.MAX_VALUE), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.TiB.toBytes(-1f), equalTo(-1099511627776f));
        assertThat(ByteUnit.TiB.toBytes(0f), equalTo(0f));
        assertThat(ByteUnit.TiB.toBytes(1f), equalTo(1099511627776f));

        assertThat(ByteUnit.TiB.toBytes(-1d), equalTo(-1099511627776d));
        assertThat(ByteUnit.TiB.toBytes(0d), equalTo(0d));
        assertThat(ByteUnit.TiB.toBytes(1d), equalTo(1099511627776d));
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertTebiToKibi1()
    {
        ByteUnit.TiB.toKiB(Integer.MIN_VALUE+1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertTebiToKibi2()
    {
        ByteUnit.TiB.toKiB(2);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertTebiToKibi3()
    {
        ByteUnit.TiB.toKiB(Integer.MAX_VALUE-1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertTebiToKibi4()
    {
        ByteUnit.TiB.toKiB(Long.MIN_VALUE+1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertTebiToKibi5()
    {
        ByteUnit.TiB.toKiB(-8_589_934_593L);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertTebiToKibi6()
    {
        ByteUnit.TiB.toKiB(8_589_934_592L);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertTebiToKibi7()
    {
        ByteUnit.TiB.toKiB(Long.MAX_VALUE-1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertTebiToKibi8()
    {
        ByteUnit.TiB.toKiB(Integer.MIN_VALUE);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertTebiToKibi9()
    {
        ByteUnit.TiB.toKiB(Long.MIN_VALUE);
    }

    @Test
    public void shouldConvertTebiToKibi()
    {
        assertThat(ByteUnit.TiB.toKiB(-2), equalTo(Integer.MIN_VALUE));
        assertThat(ByteUnit.TiB.toKiB(-1), equalTo(-1_073_741_824));
        assertThat(ByteUnit.TiB.toKiB(0), equalTo(0));
        assertThat(ByteUnit.TiB.toKiB(1), equalTo(1_073_741_824));
        assertThat(ByteUnit.TiB.toKiB(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.TiB.toKiB(-8_589_934_592L), equalTo(-9_223_372_036_854_775_808L));
        assertThat(ByteUnit.TiB.toKiB(-1L), equalTo(-1_073_741_824L));
        assertThat(ByteUnit.TiB.toKiB(0L), equalTo(0L));
        assertThat(ByteUnit.TiB.toKiB(1L), equalTo(1_073_741_824L));
        assertThat(ByteUnit.TiB.toKiB(8_589_934_591L), equalTo(9_223_372_035_781_033_984L));
        assertThat(ByteUnit.TiB.toKiB(Long.MAX_VALUE), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.TiB.toKiB(-1f), equalTo(-1073741824f));
        assertThat(ByteUnit.TiB.toKiB(0f), equalTo(0f));
        assertThat(ByteUnit.TiB.toKiB(1f), equalTo(1073741824f));

        assertThat(ByteUnit.TiB.toKiB(-1d), equalTo(-1073741824d));
        assertThat(ByteUnit.TiB.toKiB(0d), equalTo(0d));
        assertThat(ByteUnit.TiB.toKiB(1d), equalTo(1073741824d));
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertTebiToMebi1()
    {
        ByteUnit.TiB.toMiB(Integer.MIN_VALUE+1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertTebiToMebi2()
    {
        ByteUnit.TiB.toMiB(-2049);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertTebiToMebi3()
    {
        ByteUnit.TiB.toMiB(2048);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertTebiToMebi4()
    {
        ByteUnit.TiB.toMiB(Integer.MAX_VALUE-1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertTebiToMebi5()
    {
        ByteUnit.TiB.toMiB(Long.MIN_VALUE+1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertTebiToMebi6()
    {
        ByteUnit.TiB.toMiB(8_796_093_022_208L);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertTebiToMebi7()
    {
        ByteUnit.TiB.toMiB(Long.MAX_VALUE-1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertTebiToMebi8()
    {
        ByteUnit.TiB.toMiB(Integer.MIN_VALUE);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertTebiToMebi9()
    {
        ByteUnit.TiB.toMiB(Long.MIN_VALUE);
    }

    @Test
    public void shouldConvertTebiToMebi()
    {
        assertThat(ByteUnit.TiB.toMiB(-2048), equalTo(Integer.MIN_VALUE));
        assertThat(ByteUnit.TiB.toMiB(-2047), equalTo(-2_146_435_072));
        assertThat(ByteUnit.TiB.toMiB(-1), equalTo(-1_048_576));
        assertThat(ByteUnit.TiB.toMiB(0), equalTo(0));
        assertThat(ByteUnit.TiB.toMiB(1), equalTo(1_048_576));
        assertThat(ByteUnit.TiB.toMiB(2047), equalTo(2_146_435_072));
        assertThat(ByteUnit.TiB.toMiB(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.TiB.toMiB(-8_796_093_022_208L), equalTo(Long.MIN_VALUE));
        assertThat(ByteUnit.TiB.toMiB(-8_796_093_022_207L), equalTo(-9_223_372_036_853_727_232L));
        assertThat(ByteUnit.TiB.toMiB(-1L), equalTo(-1048576L));
        assertThat(ByteUnit.TiB.toMiB(0L), equalTo(0L));
        assertThat(ByteUnit.TiB.toMiB(1L), equalTo(1048576L));
        assertThat(ByteUnit.TiB.toMiB(8_796_093_022_207L), equalTo(9_223_372_036_853_727_232L));
        assertThat(ByteUnit.TiB.toMiB(Long.MAX_VALUE), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.TiB.toMiB(-1f), equalTo(-1048576f));
        assertThat(ByteUnit.TiB.toMiB(0f), equalTo(0f));
        assertThat(ByteUnit.TiB.toMiB(1f), equalTo(1048576f));

        assertThat(ByteUnit.TiB.toMiB(-1d), equalTo(-1048576d));
        assertThat(ByteUnit.TiB.toMiB(0d), equalTo(0d));
        assertThat(ByteUnit.TiB.toMiB(1d), equalTo(1048576d));
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertTebiToGibi1()
    {
        ByteUnit.TiB.toGiB(Integer.MIN_VALUE+1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertTebiToGibi2()
    {
        ByteUnit.TiB.toGiB(2_097_152);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertTebiToGibi3()
    {
        ByteUnit.TiB.toGiB(Integer.MAX_VALUE-1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertTebiToGibi4()
    {
        ByteUnit.TiB.toGiB(Long.MIN_VALUE+1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertTebiToGibi5()
    {
        ByteUnit.TiB.toGiB(9_007_199_254_740_992L);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertTebiToGibi6()
    {
        ByteUnit.TiB.toGiB(Long.MAX_VALUE-1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertTebiToGibi7()
    {
        ByteUnit.TiB.toGiB(Integer.MIN_VALUE);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertTebiToGibi8()
    {
        ByteUnit.TiB.toGiB(Long.MIN_VALUE);
    }

    @Test
    public void shouldConvertTebiToGibi()
    {
        assertThat(ByteUnit.TiB.toGiB(-2_097_152), equalTo(Integer.MIN_VALUE));
        assertThat(ByteUnit.TiB.toGiB(-2_097_151), equalTo(-2_147_482_624));
        assertThat(ByteUnit.TiB.toGiB(-1), equalTo(-1024));
        assertThat(ByteUnit.TiB.toGiB(0), equalTo(0));
        assertThat(ByteUnit.TiB.toGiB(1), equalTo(1024));
        assertThat(ByteUnit.TiB.toGiB(2_097_151), equalTo(2_147_482_624));
        assertThat(ByteUnit.TiB.toGiB(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.TiB.toGiB(-9_007_199_254_740_992L), equalTo(Long.MIN_VALUE));
        assertThat(ByteUnit.TiB.toGiB(-9_007_199_254_740_991L), equalTo(-9_223_372_036_854_774_784L));
        assertThat(ByteUnit.TiB.toGiB(-1L), equalTo(-1024L));
        assertThat(ByteUnit.TiB.toGiB(0L), equalTo(0L));
        assertThat(ByteUnit.TiB.toGiB(1L), equalTo(1024L));
        assertThat(ByteUnit.TiB.toGiB(9_007_199_254_740_991L), equalTo(9_223_372_036_854_774_784L));
        assertThat(ByteUnit.TiB.toGiB(Long.MAX_VALUE), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.TiB.toGiB(-1f), equalTo(-1024f));
        assertThat(ByteUnit.TiB.toGiB(0f), equalTo(0f));
        assertThat(ByteUnit.TiB.toGiB(1f), equalTo(1024f));

        assertThat(ByteUnit.TiB.toGiB(-1d), equalTo(-1024d));
        assertThat(ByteUnit.TiB.toGiB(0d), equalTo(0d));
        assertThat(ByteUnit.TiB.toGiB(1d), equalTo(1024d));
    }

    @Test
    public void shouldConvertFromTebiToTebi()
    {
        assertThat(ByteUnit.TiB.toTiB(Integer.MIN_VALUE), equalTo(Integer.MIN_VALUE));
        assertThat(ByteUnit.TiB.toTiB(-1), equalTo(-1));
        assertThat(ByteUnit.TiB.toTiB(0), equalTo(0));
        assertThat(ByteUnit.TiB.toTiB(1), equalTo(1));
        assertThat(ByteUnit.TiB.toTiB(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.TiB.toTiB(Long.MIN_VALUE), equalTo(Long.MIN_VALUE));
        assertThat(ByteUnit.TiB.toTiB(-1L), equalTo(-1L));
        assertThat(ByteUnit.TiB.toTiB(0L), equalTo(0L));
        assertThat(ByteUnit.TiB.toTiB(1L), equalTo(1L));
        assertThat(ByteUnit.TiB.toTiB(Long.MAX_VALUE), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.TiB.toTiB(-1f), equalTo(-1f));
        assertThat(ByteUnit.TiB.toTiB(0f), equalTo(0f));
        assertThat(ByteUnit.TiB.toTiB(1f), equalTo(1f));

        assertThat(ByteUnit.TiB.toTiB(-1d), equalTo(-1d));
        assertThat(ByteUnit.TiB.toTiB(0d), equalTo(0d));
        assertThat(ByteUnit.TiB.toTiB(1d), equalTo(1d));
    }


    @Test
    public void shouldConvertFromTebiToPebi()
    {
        assertThat(ByteUnit.TiB.toPiB(Integer.MIN_VALUE), equalTo(-2097152));
        assertThat(ByteUnit.TiB.toPiB(-1024), equalTo(-1));
        assertThat(ByteUnit.TiB.toPiB(-1023), equalTo(0));
        assertThat(ByteUnit.TiB.toPiB(0), equalTo(0));
        assertThat(ByteUnit.TiB.toPiB(1023), equalTo(0));
        assertThat(ByteUnit.TiB.toPiB(1024), equalTo(1));
        assertThat(ByteUnit.TiB.toPiB(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.TiB.toPiB(Long.MIN_VALUE), equalTo(-9007199254740992L));
        assertThat(ByteUnit.TiB.toPiB(-1024L), equalTo(-1L));
        assertThat(ByteUnit.TiB.toPiB(-1023L), equalTo(0L));
        assertThat(ByteUnit.TiB.toPiB(0L), equalTo(0L));
        assertThat(ByteUnit.TiB.toPiB(1023L), equalTo(0L));
        assertThat(ByteUnit.TiB.toPiB(1024L), equalTo(1L));
        assertThat(ByteUnit.TiB.toPiB(Long.MAX_VALUE), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.TiB.toPiB(-1f), equalTo(-0.0009765625f));
        assertThat(ByteUnit.TiB.toPiB(0f), equalTo(0f));
        assertThat(ByteUnit.TiB.toPiB(1f), equalTo(0.0009765625f));

        assertThat(ByteUnit.TiB.toPiB(-1d), equalTo(-0.0009765625d));
        assertThat(ByteUnit.TiB.toPiB(0d), equalTo(0d));
        assertThat(ByteUnit.TiB.toPiB(1d), equalTo(0.0009765625d));
    }


    @Test
    public void shouldYieldPrefixOfPeta()
    {
        assertThat(ByteUnit.Type.DECIMAL.unitsOf(Long.MIN_VALUE), is(ByteUnit.PB));
        assertThat(ByteUnit.Type.DECIMAL.unitsOf(-1_000_000_000_000_000L), is(ByteUnit.PB));
        assertThat(ByteUnit.Type.DECIMAL.unitsOf(1_000_000_000_000_000L), is(ByteUnit.PB));
        assertThat(ByteUnit.Type.DECIMAL.unitsOf(Long.MAX_VALUE), is(ByteUnit.PB));
    }

    @Test
    public void shouldYieldExactUnitsOfPeta()
    {
        assertThat(ByteUnit.Type.DECIMAL.exactUnitsOf(-2_000_000_000_000_000L), is(ByteUnit.PB));
        assertThat(ByteUnit.Type.DECIMAL.exactUnitsOf(-1_000_000_000_000_000L), is(ByteUnit.PB));
        assertThat(ByteUnit.Type.DECIMAL.exactUnitsOf(1_000_000_000_000_000L), is(ByteUnit.PB));
        assertThat(ByteUnit.Type.DECIMAL.exactUnitsOf(2_000_000_000_000_000L), is(ByteUnit.PB));
    }

    @Test
    public void shouldClaimPetaType()
    {
        assertThat(ByteUnit.PB.hasType(ByteUnit.Type.BINARY), equalTo(false));
        assertThat(ByteUnit.PB.hasType(ByteUnit.Type.DECIMAL), equalTo(true));
    }

    @Test
    public void shouldConvertToPeta()
    {
        assertThat(ByteUnit.PB.convert(Long.MAX_VALUE, ByteUnit.BYTES), equalTo(Long.MAX_VALUE));
        assertThat(ByteUnit.PB.convert(Long.MAX_VALUE, ByteUnit.KB), equalTo(Long.MAX_VALUE));
        assertThat(ByteUnit.PB.convert(Long.MAX_VALUE, ByteUnit.KiB), equalTo(Long.MAX_VALUE));
        assertThat(ByteUnit.PB.convert(Long.MAX_VALUE, ByteUnit.MB), equalTo(Long.MAX_VALUE));
        assertThat(ByteUnit.PB.convert(Long.MAX_VALUE, ByteUnit.MiB), equalTo(Long.MAX_VALUE));
        assertThat(ByteUnit.PB.convert(Long.MAX_VALUE, ByteUnit.GB), equalTo(Long.MAX_VALUE));
        assertThat(ByteUnit.PB.convert(Long.MAX_VALUE, ByteUnit.GiB), equalTo(Long.MAX_VALUE));
        assertThat(ByteUnit.PB.convert(Long.MAX_VALUE, ByteUnit.TB), equalTo(Long.MAX_VALUE));
        assertThat(ByteUnit.PB.convert(Long.MAX_VALUE, ByteUnit.TiB), equalTo(Long.MAX_VALUE));
        assertThat(ByteUnit.PB.convert(Long.MAX_VALUE, ByteUnit.PB), equalTo(Long.MAX_VALUE));
        assertThat(ByteUnit.PB.convert(Long.MAX_VALUE, ByteUnit.PiB), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.PB.convert(5_000_000_000_000_000L, ByteUnit.BYTES), equalTo(5L));
        assertThat(ByteUnit.PB.convert(5_000_000_000_000L, ByteUnit.KB), equalTo(5L));
        assertThat(ByteUnit.PB.convert(5_000_000_000L, ByteUnit.MB), equalTo(5L));
        assertThat(ByteUnit.PB.convert(5_000_000L, ByteUnit.GB), equalTo(5L));
        assertThat(ByteUnit.PB.convert(5_000L, ByteUnit.TB), equalTo(5L));
        assertThat(ByteUnit.PB.convert(5L, ByteUnit.PB), equalTo(5L));

        assertThat(ByteUnit.PB.convert(Integer.MAX_VALUE, ByteUnit.BYTES), equalTo(Integer.MAX_VALUE));
        assertThat(ByteUnit.PB.convert(Integer.MAX_VALUE, ByteUnit.KB), equalTo(Integer.MAX_VALUE));
        assertThat(ByteUnit.PB.convert(Integer.MAX_VALUE, ByteUnit.KiB), equalTo(Integer.MAX_VALUE));
        assertThat(ByteUnit.PB.convert(Integer.MAX_VALUE, ByteUnit.MB), equalTo(Integer.MAX_VALUE));
        assertThat(ByteUnit.PB.convert(Integer.MAX_VALUE, ByteUnit.MiB), equalTo(Integer.MAX_VALUE));
        assertThat(ByteUnit.PB.convert(Integer.MAX_VALUE, ByteUnit.GB), equalTo(Integer.MAX_VALUE));
        assertThat(ByteUnit.PB.convert(Integer.MAX_VALUE, ByteUnit.GiB), equalTo(Integer.MAX_VALUE));
        assertThat(ByteUnit.PB.convert(Integer.MAX_VALUE, ByteUnit.TB), equalTo(Integer.MAX_VALUE));
        assertThat(ByteUnit.PB.convert(Integer.MAX_VALUE, ByteUnit.TiB), equalTo(Integer.MAX_VALUE));
        assertThat(ByteUnit.PB.convert(Integer.MAX_VALUE, ByteUnit.PB), equalTo(Integer.MAX_VALUE));
        assertThat(ByteUnit.PB.convert(Integer.MAX_VALUE, ByteUnit.PiB), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.PB.convert(2_000_000_000, ByteUnit.MB), equalTo(2));
        assertThat(ByteUnit.PB.convert(5_000_000, ByteUnit.GB), equalTo(5));
        assertThat(ByteUnit.PB.convert(5_000, ByteUnit.TB), equalTo(5));
        assertThat(ByteUnit.PB.convert(5, ByteUnit.PB), equalTo(5));

        assertThat(ByteUnit.PB.convert(1_000_000_000_000_000d, ByteUnit.BYTES), equalTo(1d));
        assertThat(ByteUnit.PB.convert(1_000_000_000_000d, ByteUnit.KB), equalTo(1d));
        assertThat(ByteUnit.PB.convert(1_000_000_000d, ByteUnit.MB), equalTo(1d));
        assertThat(ByteUnit.PB.convert(1_000_000d, ByteUnit.GB), equalTo(1d));
        assertThat(ByteUnit.PB.convert(1_000d, ByteUnit.TB), equalTo(1d));
        assertThat(ByteUnit.PB.convert(1d, ByteUnit.PB), equalTo(1d));
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPetaToBytes1()
    {
        ByteUnit.PB.toBytes(Integer.MIN_VALUE+1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPetaToBytes2()
    {
        ByteUnit.PB.toBytes(-1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPetaToBytes3()
    {
        ByteUnit.PB.toBytes(1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPetaToBytes4()
    {
        ByteUnit.PB.toBytes(Integer.MAX_VALUE-1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPetaToBytes5()
    {
        ByteUnit.PB.toBytes(Long.MIN_VALUE+1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPetaToBytes6()
    {
        ByteUnit.PB.toBytes(-9_224L);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPetaToBytes7()
    {
        ByteUnit.PB.toBytes(9_224L);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPetaToBytes8()
    {
        ByteUnit.PB.toBytes(Long.MAX_VALUE-1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPetaToBytes9()
    {
        ByteUnit.PB.toBytes(Integer.MIN_VALUE);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPetaToBytes10()
    {
        ByteUnit.PB.toBytes(Long.MIN_VALUE);
    }

    @Test
    public void shouldConvertPetaToBytes()
    {
        assertThat(ByteUnit.PB.toBytes(0), equalTo(0));
        assertThat(ByteUnit.PB.toBytes(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.PB.toBytes(-9_223L), equalTo(-9_223_000_000_000_000_000L));
        assertThat(ByteUnit.PB.toBytes(-1L), equalTo(-1_000_000_000_000_000L));
        assertThat(ByteUnit.PB.toBytes(0L), equalTo(0L));
        assertThat(ByteUnit.PB.toBytes(1L), equalTo(1_000_000_000_000_000L));
        assertThat(ByteUnit.PB.toBytes(9_223L), equalTo(9_223_000_000_000_000_000L));
        assertThat(ByteUnit.PB.toBytes(Long.MAX_VALUE), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.PB.toBytes(-1f), equalTo(-1e15f));
        assertThat(ByteUnit.PB.toBytes(0f), equalTo(0f));
        assertThat(ByteUnit.PB.toBytes(1f), equalTo(1e15f));

        assertThat(ByteUnit.PB.toBytes(-1d), equalTo(-1e15d));
        assertThat(ByteUnit.PB.toBytes(0d), equalTo(0d));
        assertThat(ByteUnit.PB.toBytes(1d), equalTo(1e15d));
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPetaToKilo1()
    {
        ByteUnit.PB.toKB(Integer.MIN_VALUE+1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPetaToKilo2()
    {
        ByteUnit.PB.toKB(-1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPetaToKilo3()
    {
        ByteUnit.PB.toKB(1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPetaToKilo4()
    {
        ByteUnit.PB.toKB(Integer.MAX_VALUE-1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPetaToKilo5()
    {
        ByteUnit.PB.toKB(Long.MIN_VALUE+1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPetaToKilo6()
    {
        ByteUnit.PB.toKB(-9_223_373L);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPetaToKilo7()
    {
        ByteUnit.PB.toKB(9_223_373L);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPetaToKilo8()
    {
        ByteUnit.PB.toKB(Long.MAX_VALUE-1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPetaToKilo9()
    {
        ByteUnit.PB.toKB(Integer.MIN_VALUE);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPetaToKilo10()
    {
        ByteUnit.PB.toKB(Long.MIN_VALUE);
    }

    @Test
    public void shouldConvertPetaToKilo()
    {
        assertThat(ByteUnit.PB.toKB(0), equalTo(0));
        assertThat(ByteUnit.PB.toKB(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.PB.toKB(-9_223_372L), equalTo(-9_223_372_000_000_000_000L));
        assertThat(ByteUnit.PB.toKB(-1L), equalTo(-1_000_000_000_000L));
        assertThat(ByteUnit.PB.toKB(0L), equalTo(0L));
        assertThat(ByteUnit.PB.toKB(1L), equalTo(1_000_000_000_000L));
        assertThat(ByteUnit.PB.toKB(9_223_372L), equalTo(9_223_372_000_000_000_000L));
        assertThat(ByteUnit.PB.toKB(Long.MAX_VALUE), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.PB.toKB(-1f), equalTo(-1e12f));
        assertThat(ByteUnit.PB.toKB(0f), equalTo(0f));
        assertThat(ByteUnit.PB.toKB(1f), equalTo(1e12f));

        assertThat(ByteUnit.PB.toKB(-1d), equalTo(-1e12d));
        assertThat(ByteUnit.PB.toKB(0d), equalTo(0d));
        assertThat(ByteUnit.PB.toKB(1d), equalTo(1e12d));
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPetaToMega1()
    {
        ByteUnit.PB.toMB(Integer.MIN_VALUE+1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPetaToMega2()
    {
        ByteUnit.PB.toMB(-3);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPetaToMega3()
    {
        ByteUnit.PB.toMB(3);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPetaToMega4()
    {
        ByteUnit.PB.toMB(Integer.MAX_VALUE-1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPetaToMega5()
    {
        ByteUnit.PB.toMB(Long.MIN_VALUE+1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPetaToMega6()
    {
        ByteUnit.PB.toMB(-9_223_372_037L);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPetaToMega7()
    {
        ByteUnit.PB.toMB(9_223_372_037L);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPetaToMega8()
    {
        ByteUnit.PB.toMB(Long.MAX_VALUE-1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPetaToMega9()
    {
        ByteUnit.PB.toMB(Integer.MIN_VALUE);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPetaToMega10()
    {
        ByteUnit.PB.toMB(Long.MIN_VALUE);
    }

    @Test
    public void shouldConvertPetaToMega()
    {
        assertThat(ByteUnit.PB.toMB(-2), equalTo(-2_000_000_000));
        assertThat(ByteUnit.PB.toMB(-1), equalTo(-1_000_000_000));
        assertThat(ByteUnit.PB.toMB(0), equalTo(0));
        assertThat(ByteUnit.PB.toMB(1), equalTo(1_000_000_000));
        assertThat(ByteUnit.PB.toMB(2), equalTo(2_000_000_000));
        assertThat(ByteUnit.PB.toMB(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.PB.toMB(-9_223_372_036L), equalTo(-9_223_372_036_000_000_000L));
        assertThat(ByteUnit.PB.toMB(-1L), equalTo(-1_000_000_000L));
        assertThat(ByteUnit.PB.toMB(0L), equalTo(0L));
        assertThat(ByteUnit.PB.toMB(1L), equalTo(1_000_000_000L));
        assertThat(ByteUnit.PB.toMB(9_223_372_036L), equalTo(9_223_372_036_000_000_000L));
        assertThat(ByteUnit.PB.toMB(Long.MAX_VALUE), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.PB.toMB(-1f), equalTo(-1e9f));
        assertThat(ByteUnit.PB.toMB(0f), equalTo(0f));
        assertThat(ByteUnit.PB.toMB(1f), equalTo(1e9f));

        assertThat(ByteUnit.PB.toMB(-1d), equalTo(-1e9d));
        assertThat(ByteUnit.PB.toMB(0d), equalTo(0d));
        assertThat(ByteUnit.PB.toMB(1d), equalTo(1e9d));
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPetaToGiga1()
    {
        ByteUnit.PB.toGB(Integer.MIN_VALUE+1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPetaToGiga2()
    {
        ByteUnit.PB.toGB(-2_148);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPetaToGiga3()
    {
        ByteUnit.PB.toGB(2_148);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPetaToGiga4()
    {
        ByteUnit.PB.toGB(Integer.MAX_VALUE-1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPetaToGiga5()
    {
        ByteUnit.PB.toGB(Long.MIN_VALUE+1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPetaToGiga6()
    {
        ByteUnit.PB.toGB(-9_223_372_036_855L);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPetaToGiga7()
    {
        ByteUnit.PB.toGB(9_223_372_036_855L);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPetaToGiga8()
    {
        ByteUnit.PB.toGB(Long.MAX_VALUE-1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPetaToGiga9()
    {
        ByteUnit.PB.toGB(Integer.MIN_VALUE);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPetaToGiga10()
    {
        ByteUnit.PB.toGB(Long.MIN_VALUE);
    }

    @Test
    public void shouldConvertPetaToGiga()
    {
        assertThat(ByteUnit.PB.toGB(-2_147), equalTo(-2_147_000_000));
        assertThat(ByteUnit.PB.toGB(-1), equalTo(-1_000_000));
        assertThat(ByteUnit.PB.toGB(0), equalTo(0));
        assertThat(ByteUnit.PB.toGB(1), equalTo(1_000_000));
        assertThat(ByteUnit.PB.toGB(2_147), equalTo(2_147_000_000));
        assertThat(ByteUnit.PB.toGB(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.PB.toGB(-9_223_372_036_854L), equalTo(-9_223_372_036_854_000_000L));
        assertThat(ByteUnit.PB.toGB(-1L), equalTo(-1_000_000L));
        assertThat(ByteUnit.PB.toGB(0L), equalTo(0L));
        assertThat(ByteUnit.PB.toGB(1L), equalTo(1_000_000L));
        assertThat(ByteUnit.PB.toGB(9_223_372_036_854L), equalTo(9_223_372_036_854_000_000L));
        assertThat(ByteUnit.PB.toGB(Long.MAX_VALUE), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.PB.toGB(-1f), equalTo(-1e6f));
        assertThat(ByteUnit.PB.toGB(0f), equalTo(0f));
        assertThat(ByteUnit.PB.toGB(1f), equalTo(1e6f));

        assertThat(ByteUnit.PB.toGB(-1d), equalTo(-1e6d));
        assertThat(ByteUnit.PB.toGB(0d), equalTo(0d));
        assertThat(ByteUnit.PB.toGB(1d), equalTo(1e6d));
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPetaToTera1()
    {
        ByteUnit.PB.toTB(Integer.MIN_VALUE+1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPetaToTera2()
    {
        ByteUnit.PB.toTB(-2_147_484);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPetaToTera3()
    {
        ByteUnit.PB.toTB(2_147_484);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPetaToTera4()
    {
        ByteUnit.PB.toTB(Integer.MAX_VALUE-1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPetaToTera5()
    {
        ByteUnit.PB.toTB(Long.MIN_VALUE+1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPetaToTera6()
    {
        ByteUnit.PB.toTB(-9_223_372_036_854_776L);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPetaToTera7()
    {
        ByteUnit.PB.toTB(9_223_372_036_854_776L);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPetaToTera8()
    {
        ByteUnit.PB.toTB(Long.MAX_VALUE-1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPetaToTera9()
    {
        ByteUnit.PB.toTB(Integer.MIN_VALUE);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPetaToTera10()
    {
        ByteUnit.PB.toTB(Long.MIN_VALUE);
    }

    @Test
    public void shouldConvertPetaToTera()
    {
        assertThat(ByteUnit.PB.toTB(-2_147_483), equalTo(-2_147_483_000));
        assertThat(ByteUnit.PB.toTB(-1), equalTo(-1_000));
        assertThat(ByteUnit.PB.toTB(0), equalTo(0));
        assertThat(ByteUnit.PB.toTB(1), equalTo(1_000));
        assertThat(ByteUnit.PB.toTB(2_147_483), equalTo(2_147_483_000));
        assertThat(ByteUnit.PB.toTB(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.PB.toTB(-9_223_372_036_854_775L), equalTo(-9_223_372_036_854_775_000L));
        assertThat(ByteUnit.PB.toTB(-1L), equalTo(-1_000L));
        assertThat(ByteUnit.PB.toTB(0L), equalTo(0L));
        assertThat(ByteUnit.PB.toTB(1L), equalTo(1_000L));
        assertThat(ByteUnit.PB.toTB(9_223_372_036_854_775L), equalTo(9_223_372_036_854_775_000L));
        assertThat(ByteUnit.PB.toTB(Long.MAX_VALUE), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.PB.toTB(-1f), equalTo(-1_000f));
        assertThat(ByteUnit.PB.toTB(0f), equalTo(0f));
        assertThat(ByteUnit.PB.toTB(1f), equalTo(1_000f));

        assertThat(ByteUnit.PB.toTB(-1d), equalTo(-1_000d));
        assertThat(ByteUnit.PB.toTB(0d), equalTo(0d));
        assertThat(ByteUnit.PB.toTB(1d), equalTo(1_000d));
    }

    @Test
    public void shouldConvertFromPetaToPeta()
    {
        assertThat(ByteUnit.PB.toPB(Integer.MIN_VALUE), equalTo(Integer.MIN_VALUE));
        assertThat(ByteUnit.PB.toPB(-1), equalTo(-1));
        assertThat(ByteUnit.PB.toPB(0), equalTo(0));
        assertThat(ByteUnit.PB.toPB(1), equalTo(1));
        assertThat(ByteUnit.PB.toPB(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.PB.toPB(Long.MIN_VALUE), equalTo(Long.MIN_VALUE));
        assertThat(ByteUnit.PB.toPB(-1L), equalTo(-1L));
        assertThat(ByteUnit.PB.toPB(0L), equalTo(0L));
        assertThat(ByteUnit.PB.toPB(1L), equalTo(1L));
        assertThat(ByteUnit.PB.toPB(Long.MAX_VALUE), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.PB.toPB(-1f), equalTo(-1f));
        assertThat(ByteUnit.PB.toPB(0f), equalTo(0f));
        assertThat(ByteUnit.PB.toPB(1f), equalTo(1f));

        assertThat(ByteUnit.PB.toPB(-1d), equalTo(-1d));
        assertThat(ByteUnit.PB.toPB(0d), equalTo(0d));
        assertThat(ByteUnit.PB.toPB(1d), equalTo(1d));
    }


    @Test
    public void shouldYieldPrefixOfPebi()
    {
        assertThat(ByteUnit.Type.BINARY.unitsOf(Long.MIN_VALUE), is(ByteUnit.PiB));
        assertThat(ByteUnit.Type.BINARY.unitsOf(-1125899906842624L), is(ByteUnit.PiB));
        assertThat(ByteUnit.Type.BINARY.unitsOf(1125899906842624L), is(ByteUnit.PiB));
        assertThat(ByteUnit.Type.BINARY.unitsOf(Long.MAX_VALUE), is(ByteUnit.PiB));
    }

    @Test
    public void shouldYieldExactUnitsOfPebi()
    {
        assertThat(ByteUnit.Type.BINARY.exactUnitsOf(-2*1125899906842624L), is(ByteUnit.PiB));
        assertThat(ByteUnit.Type.BINARY.exactUnitsOf(-1125899906842624L), is(ByteUnit.PiB));
        assertThat(ByteUnit.Type.BINARY.exactUnitsOf(1125899906842624L), is(ByteUnit.PiB));
        assertThat(ByteUnit.Type.BINARY.exactUnitsOf(2*1125899906842624L), is(ByteUnit.PiB));
    }

    @Test
    public void shouldClaimPebiType()
    {
        assertThat(ByteUnit.PiB.hasType(ByteUnit.Type.BINARY), equalTo(true));
        assertThat(ByteUnit.PiB.hasType(ByteUnit.Type.DECIMAL), equalTo(false));
    }

    @Test
    public void shouldConvertToPebi()
    {
        assertThat(ByteUnit.PiB.convert(Long.MAX_VALUE, ByteUnit.BYTES), equalTo(Long.MAX_VALUE));
        assertThat(ByteUnit.PiB.convert(Long.MAX_VALUE, ByteUnit.KB), equalTo(Long.MAX_VALUE));
        assertThat(ByteUnit.PiB.convert(Long.MAX_VALUE, ByteUnit.KiB), equalTo(Long.MAX_VALUE));
        assertThat(ByteUnit.PiB.convert(Long.MAX_VALUE, ByteUnit.MB), equalTo(Long.MAX_VALUE));
        assertThat(ByteUnit.PiB.convert(Long.MAX_VALUE, ByteUnit.MiB), equalTo(Long.MAX_VALUE));
        assertThat(ByteUnit.PiB.convert(Long.MAX_VALUE, ByteUnit.GB), equalTo(Long.MAX_VALUE));
        assertThat(ByteUnit.PiB.convert(Long.MAX_VALUE, ByteUnit.GiB), equalTo(Long.MAX_VALUE));
        assertThat(ByteUnit.PiB.convert(Long.MAX_VALUE, ByteUnit.TB), equalTo(Long.MAX_VALUE));
        assertThat(ByteUnit.PiB.convert(Long.MAX_VALUE, ByteUnit.TiB), equalTo(Long.MAX_VALUE));
        assertThat(ByteUnit.PiB.convert(Long.MAX_VALUE, ByteUnit.PB), equalTo(Long.MAX_VALUE));
        assertThat(ByteUnit.PiB.convert(Long.MAX_VALUE, ByteUnit.PiB), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.PiB.convert(5L * 1125899906842624L, ByteUnit.BYTES), equalTo(5L));
        assertThat(ByteUnit.PiB.convert(5L * 1099511627776L, ByteUnit.KiB), equalTo(5L));
        assertThat(ByteUnit.PiB.convert(5L * 1073741824L, ByteUnit.MiB), equalTo(5L));
        assertThat(ByteUnit.PiB.convert(5L * 1048576L, ByteUnit.GiB), equalTo(5L));
        assertThat(ByteUnit.PiB.convert(5L * 1024L, ByteUnit.TiB), equalTo(5L));
        assertThat(ByteUnit.PiB.convert(5L, ByteUnit.PiB), equalTo(5L));

        assertThat(ByteUnit.PiB.convert(Integer.MAX_VALUE, ByteUnit.BYTES), equalTo(Integer.MAX_VALUE));
        assertThat(ByteUnit.PiB.convert(Integer.MAX_VALUE, ByteUnit.KB), equalTo(Integer.MAX_VALUE));
        assertThat(ByteUnit.PiB.convert(Integer.MAX_VALUE, ByteUnit.KiB), equalTo(Integer.MAX_VALUE));
        assertThat(ByteUnit.PiB.convert(Integer.MAX_VALUE, ByteUnit.MB), equalTo(Integer.MAX_VALUE));
        assertThat(ByteUnit.PiB.convert(Integer.MAX_VALUE, ByteUnit.MiB), equalTo(Integer.MAX_VALUE));
        assertThat(ByteUnit.PiB.convert(Integer.MAX_VALUE, ByteUnit.GB), equalTo(Integer.MAX_VALUE));
        assertThat(ByteUnit.PiB.convert(Integer.MAX_VALUE, ByteUnit.GiB), equalTo(Integer.MAX_VALUE));
        assertThat(ByteUnit.PiB.convert(Integer.MAX_VALUE, ByteUnit.TB), equalTo(Integer.MAX_VALUE));
        assertThat(ByteUnit.PiB.convert(Integer.MAX_VALUE, ByteUnit.TiB), equalTo(Integer.MAX_VALUE));
        assertThat(ByteUnit.PiB.convert(Integer.MAX_VALUE, ByteUnit.PB), equalTo(Integer.MAX_VALUE));
        assertThat(ByteUnit.PiB.convert(Integer.MAX_VALUE, ByteUnit.PiB), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.PiB.convert(5 * 1048576, ByteUnit.GiB), equalTo(5));
        assertThat(ByteUnit.PiB.convert(5 * 1024, ByteUnit.TiB), equalTo(5));
        assertThat(ByteUnit.PiB.convert(5, ByteUnit.PiB), equalTo(5));

        assertThat(ByteUnit.PiB.convert(1125899906842624d, ByteUnit.BYTES), equalTo(1d));
        assertThat(ByteUnit.PiB.convert(1099511627776d, ByteUnit.KiB), equalTo(1d));
        assertThat(ByteUnit.PiB.convert(1073741824d, ByteUnit.MiB), equalTo(1d));
        assertThat(ByteUnit.PiB.convert(1048576d, ByteUnit.GiB), equalTo(1d));
        assertThat(ByteUnit.PiB.convert(1024d, ByteUnit.TiB), equalTo(1d));
        assertThat(ByteUnit.PiB.convert(1d, ByteUnit.PiB), equalTo(1d));
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPebiToBytes1()
    {
        ByteUnit.PiB.toBytes(Integer.MIN_VALUE+1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPebiToBytes2()
    {
        ByteUnit.PiB.toBytes(-1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPebiToBytes3()
    {
        ByteUnit.PiB.toBytes(1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPebiToBytes4()
    {
        ByteUnit.PiB.toBytes(Integer.MAX_VALUE-1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPebiToBytes5()
    {
        ByteUnit.PiB.toBytes(Long.MIN_VALUE+1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPebiToBytes6()
    {
        ByteUnit.PiB.toBytes(8_192L);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPebiToBytes7()
    {
        ByteUnit.PiB.toBytes(Long.MAX_VALUE-1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPebiToBytes8()
    {
        ByteUnit.PiB.toBytes(Integer.MIN_VALUE);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPebiToBytes9()
    {
        ByteUnit.PiB.toBytes(Long.MIN_VALUE);
    }

    @Test
    public void shouldConvertPebiToBytes()
    {
        assertThat(ByteUnit.PiB.toBytes(0), equalTo(0));
        assertThat(ByteUnit.PiB.toBytes(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.PiB.toBytes(-8_192L), equalTo(Long.MIN_VALUE));
        assertThat(ByteUnit.PiB.toBytes(-8_191L), equalTo(-9_222_246_136_947_933_184L));
        assertThat(ByteUnit.PiB.toBytes(-1L), equalTo(-1_125_899_906_842_624L));
        assertThat(ByteUnit.PiB.toBytes(0L), equalTo(0L));
        assertThat(ByteUnit.PiB.toBytes(1L), equalTo(1_125_899_906_842_624L));
        assertThat(ByteUnit.PiB.toBytes(8_191L), equalTo(9_222_246_136_947_933_184L));
        assertThat(ByteUnit.PiB.toBytes(Long.MAX_VALUE), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.PiB.toBytes(-1f), equalTo(-1125899906842624f));
        assertThat(ByteUnit.PiB.toBytes(0f), equalTo(0f));
        assertThat(ByteUnit.PiB.toBytes(1f), equalTo(1125899906842624f));

        assertThat(ByteUnit.PiB.toBytes(-1d), equalTo(-1125899906842624d));
        assertThat(ByteUnit.PiB.toBytes(0d), equalTo(0d));
        assertThat(ByteUnit.PiB.toBytes(1d), equalTo(1125899906842624d));
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPebiToKibi1()
    {
        ByteUnit.PiB.toKiB(Integer.MIN_VALUE+1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPebiToKibi2()
    {
        ByteUnit.PiB.toKiB(-1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPebiToKibi3()
    {
        ByteUnit.PiB.toKiB(1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPebiToKibi4()
    {
        ByteUnit.PiB.toKiB(Integer.MAX_VALUE-1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPebiToKibi5()
    {
        ByteUnit.PiB.toKiB(Long.MIN_VALUE+1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPebiToKibi6()
    {
        ByteUnit.PiB.toKiB(8_388_608L);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPebiToKibi7()
    {
        ByteUnit.PiB.toKiB(Long.MAX_VALUE-1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPebiToKibi8()
    {
        ByteUnit.PiB.toKiB(Integer.MIN_VALUE);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPebiToKibi9()
    {
        ByteUnit.PiB.toKiB(Long.MIN_VALUE);
    }

    @Test
    public void shouldConvertPebiToKibi()
    {
        assertThat(ByteUnit.PiB.toKiB(0), equalTo(0));
        assertThat(ByteUnit.PiB.toKiB(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.PiB.toKiB(-8_388_608L), equalTo(Long.MIN_VALUE));
        assertThat(ByteUnit.PiB.toKiB(-8_388_607L), equalTo(-9_223_370_937_343_148_032L));
        assertThat(ByteUnit.PiB.toKiB(-1L), equalTo(-1_099_511_627_776L));
        assertThat(ByteUnit.PiB.toKiB(0L), equalTo(0L));
        assertThat(ByteUnit.PiB.toKiB(1L), equalTo(1_099_511_627_776L));
        assertThat(ByteUnit.PiB.toKiB(8_388_607L), equalTo(9_223_370_937_343_148_032L));
        assertThat(ByteUnit.PiB.toKiB(Long.MAX_VALUE), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.PiB.toKiB(-1f), equalTo(-1099511627776f));
        assertThat(ByteUnit.PiB.toKiB(0f), equalTo(0f));
        assertThat(ByteUnit.PiB.toKiB(1f), equalTo(1099511627776f));

        assertThat(ByteUnit.PiB.toKiB(-1d), equalTo(-1099511627776d));
        assertThat(ByteUnit.PiB.toKiB(0d), equalTo(0d));
        assertThat(ByteUnit.PiB.toKiB(1d), equalTo(1099511627776d));
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPebiToMebi1()
    {
        ByteUnit.PiB.toMiB(Integer.MIN_VALUE+1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPebiToMebi2()
    {
        ByteUnit.PiB.toMiB(2);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPebiToMebi3()
    {
        ByteUnit.PiB.toMiB(Integer.MAX_VALUE-1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPebiToMebi4()
    {
        ByteUnit.PiB.toMiB(Long.MIN_VALUE+1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPebiToMebi5()
    {
        ByteUnit.PiB.toMiB(-8_589_934_593L);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPebiToMebi6()
    {
        ByteUnit.PiB.toMiB(8_589_934_592L);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPebiToMebi7()
    {
        ByteUnit.PiB.toMiB(Long.MAX_VALUE-1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPebiToMebi8()
    {
        ByteUnit.PiB.toMiB(Integer.MIN_VALUE);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPebiToMebi9()
    {
        ByteUnit.PiB.toMiB(Long.MIN_VALUE);
    }

    @Test
    public void shouldConvertPebiToMebi()
    {
        assertThat(ByteUnit.PiB.toMiB(-2), equalTo(Integer.MIN_VALUE));
        assertThat(ByteUnit.PiB.toMiB(-1), equalTo(-1_073_741_824));
        assertThat(ByteUnit.PiB.toMiB(0), equalTo(0));
        assertThat(ByteUnit.PiB.toMiB(1), equalTo(1_073_741_824));
        assertThat(ByteUnit.PiB.toMiB(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.PiB.toMiB(-8_589_934_592L), equalTo(-9_223_372_036_854_775_808L));
        assertThat(ByteUnit.PiB.toMiB(-1L), equalTo(-1_073_741_824L));
        assertThat(ByteUnit.PiB.toMiB(0L), equalTo(0L));
        assertThat(ByteUnit.PiB.toMiB(1L), equalTo(1_073_741_824L));
        assertThat(ByteUnit.PiB.toMiB(8_589_934_591L), equalTo(9_223_372_035_781_033_984L));
        assertThat(ByteUnit.PiB.toMiB(Long.MAX_VALUE), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.PiB.toMiB(-1f), equalTo(-1073741824f));
        assertThat(ByteUnit.PiB.toMiB(0f), equalTo(0f));
        assertThat(ByteUnit.PiB.toMiB(1f), equalTo(1073741824f));

        assertThat(ByteUnit.PiB.toMiB(-1d), equalTo(-1073741824d));
        assertThat(ByteUnit.PiB.toMiB(0d), equalTo(0d));
        assertThat(ByteUnit.PiB.toMiB(1d), equalTo(1073741824d));
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPebiToGibi1()
    {
        ByteUnit.PiB.toGiB(Integer.MIN_VALUE+1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPebiToGibi2()
    {
        ByteUnit.PiB.toGiB(-2049);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPebiToGibi3()
    {
        ByteUnit.PiB.toGiB(2048);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPebiToGibi4()
    {
        ByteUnit.PiB.toGiB(2049);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPebiToGibi5()
    {
        ByteUnit.PiB.toGiB(Integer.MAX_VALUE-1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPebiToGibi6()
    {
        ByteUnit.PiB.toGiB(Long.MIN_VALUE+1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPebiToGibi7()
    {
        ByteUnit.PiB.toGiB(8_796_093_022_208L);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPebiToGibi8()
    {
        ByteUnit.PiB.toGiB(Long.MAX_VALUE-1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPebiToGibi9()
    {
        ByteUnit.PiB.toGiB(Integer.MIN_VALUE);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPebiToGibi10()
    {
        ByteUnit.PiB.toGiB(Long.MIN_VALUE);
    }

    @Test
    public void shouldConvertPebiToGibi()
    {
        assertThat(ByteUnit.PiB.toGiB(-2048), equalTo(Integer.MIN_VALUE));
        assertThat(ByteUnit.PiB.toGiB(-2047), equalTo(-2_146_435_072));
        assertThat(ByteUnit.PiB.toGiB(-1), equalTo(-1_048_576));
        assertThat(ByteUnit.PiB.toGiB(0), equalTo(0));
        assertThat(ByteUnit.PiB.toGiB(1), equalTo(1_048_576));
        assertThat(ByteUnit.PiB.toGiB(2047), equalTo(2_146_435_072));
        assertThat(ByteUnit.PiB.toGiB(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.PiB.toGiB(-8_796_093_022_208L), equalTo(Long.MIN_VALUE));
        assertThat(ByteUnit.PiB.toGiB(-8_796_093_022_207L), equalTo(-9_223_372_036_853_727_232L));
        assertThat(ByteUnit.PiB.toGiB(-1L), equalTo(-1048576L));
        assertThat(ByteUnit.PiB.toGiB(0L), equalTo(0L));
        assertThat(ByteUnit.PiB.toGiB(1L), equalTo(1048576L));
        assertThat(ByteUnit.PiB.toGiB(8_796_093_022_207L), equalTo(9_223_372_036_853_727_232L));
        assertThat(ByteUnit.PiB.toGiB(Long.MAX_VALUE), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.PiB.toGiB(-1f), equalTo(-1048576f));
        assertThat(ByteUnit.PiB.toGiB(0f), equalTo(0f));
        assertThat(ByteUnit.PiB.toGiB(1f), equalTo(1048576f));

        assertThat(ByteUnit.PiB.toGiB(-1d), equalTo(-1048576d));
        assertThat(ByteUnit.PiB.toGiB(0d), equalTo(0d));
        assertThat(ByteUnit.PiB.toGiB(1d), equalTo(1048576d));
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPebiToTebi1()
    {
        ByteUnit.PiB.toTiB(Integer.MIN_VALUE+1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPebiToTebi2()
    {
        ByteUnit.PiB.toTiB(2_097_152);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPebiToTebi3()
    {
        ByteUnit.PiB.toTiB(Integer.MAX_VALUE-1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPebiToTebi4()
    {
        ByteUnit.PiB.toTiB(Long.MIN_VALUE+1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPebiToTebi5()
    {
        ByteUnit.PiB.toTiB(9_007_199_254_740_992L);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPebiToTebi6()
    {
        ByteUnit.PiB.toTiB(Long.MAX_VALUE-1);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPebiToTebi7()
    {
        ByteUnit.PiB.toTiB(Integer.MIN_VALUE);
    }

    @Test(expected=ArithmeticException.class)
    public void shouldThrowArithmeticExceptionWhenCannotConvertPebiToTebi8()
    {
        ByteUnit.PiB.toTiB(Long.MIN_VALUE);
    }

    @Test
    public void shouldConvertPebiToTebi()
    {
        assertThat(ByteUnit.PiB.toTiB(-2_097_152), equalTo(Integer.MIN_VALUE));
        assertThat(ByteUnit.PiB.toTiB(-2_097_151), equalTo(-2_147_482_624));
        assertThat(ByteUnit.PiB.toTiB(-1), equalTo(-1024));
        assertThat(ByteUnit.PiB.toTiB(0), equalTo(0));
        assertThat(ByteUnit.PiB.toTiB(1), equalTo(1024));
        assertThat(ByteUnit.PiB.toTiB(2_097_151), equalTo(2_147_482_624));
        assertThat(ByteUnit.PiB.toTiB(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.PiB.toTiB(-9_007_199_254_740_992L), equalTo(Long.MIN_VALUE));
        assertThat(ByteUnit.PiB.toTiB(-9_007_199_254_740_991L), equalTo(-9_223_372_036_854_774_784L));
        assertThat(ByteUnit.PiB.toTiB(-1L), equalTo(-1024L));
        assertThat(ByteUnit.PiB.toTiB(0L), equalTo(0L));
        assertThat(ByteUnit.PiB.toTiB(1L), equalTo(1024L));
        assertThat(ByteUnit.PiB.toTiB(9_007_199_254_740_991L), equalTo(9_223_372_036_854_774_784L));
        assertThat(ByteUnit.PiB.toTiB(Long.MAX_VALUE), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.PiB.toTiB(-1f), equalTo(-1024f));
        assertThat(ByteUnit.PiB.toTiB(0f), equalTo(0f));
        assertThat(ByteUnit.PiB.toTiB(1f), equalTo(1024f));

        assertThat(ByteUnit.PiB.toTiB(-1d), equalTo(-1024d));
        assertThat(ByteUnit.PiB.toTiB(0d), equalTo(0d));
        assertThat(ByteUnit.PiB.toTiB(1d), equalTo(1024d));
    }

    @Test
    public void shouldConvertFromPebiToPebi()
    {
        assertThat(ByteUnit.PiB.toPiB(Integer.MIN_VALUE), equalTo(Integer.MIN_VALUE));
        assertThat(ByteUnit.PiB.toPiB(-1), equalTo(-1));
        assertThat(ByteUnit.PiB.toPiB(0), equalTo(0));
        assertThat(ByteUnit.PiB.toPiB(1), equalTo(1));
        assertThat(ByteUnit.PiB.toPiB(Integer.MAX_VALUE), equalTo(Integer.MAX_VALUE));

        assertThat(ByteUnit.PiB.toPiB(Long.MIN_VALUE), equalTo(Long.MIN_VALUE));
        assertThat(ByteUnit.PiB.toPiB(-1L), equalTo(-1L));
        assertThat(ByteUnit.PiB.toPiB(0L), equalTo(0L));
        assertThat(ByteUnit.PiB.toPiB(1L), equalTo(1L));
        assertThat(ByteUnit.PiB.toPiB(Long.MAX_VALUE), equalTo(Long.MAX_VALUE));

        assertThat(ByteUnit.PiB.toPiB(-1f), equalTo(-1f));
        assertThat(ByteUnit.PiB.toPiB(0f), equalTo(0f));
        assertThat(ByteUnit.PiB.toPiB(1f), equalTo(1f));

        assertThat(ByteUnit.PiB.toPiB(-1d), equalTo(-1d));
        assertThat(ByteUnit.PiB.toPiB(0d), equalTo(0d));
        assertThat(ByteUnit.PiB.toPiB(1d), equalTo(1d));
    }
}
