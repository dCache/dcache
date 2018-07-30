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

import org.junit.Test;

import org.dcache.util.ByteSizeParser.Whitespace;

import static org.dcache.util.ByteSizeParser.Coercion.*;
import static org.dcache.util.ByteSizeParser.NumericalInput.FLOATING_POINT;
import static org.dcache.util.ByteSizeParser.NumericalInput.INTEGER;
import static org.dcache.util.ByteSizeParser.UnitPresence.OPTIONAL;
import static org.dcache.util.ByteSizeParser.UnitPresence.REQUIRED;
import static org.dcache.util.ByteUnit.MiB;
import static org.dcache.util.ByteUnits.*;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

public class ByteSizeParserTest
{
    @Test
    public void shouldParseIsoSymbolSimpleNumber()
    {
        long value = ByteSizeParser.using(isoSymbol()).parse("1");

        assertThat(value, is(equalTo(1L)));
    }

    @Test
    public void shouldParseIsoSymbolSpaceByteNumber()
    {
        long value = ByteSizeParser.using(isoSymbol()).parse("1 B");

        assertThat(value, is(equalTo(1L)));
    }

    @Test
    public void shouldParseIsoSymbolNoSpaceByteNumber()
    {
        long value = ByteSizeParser.using(isoSymbol()).parse("1B");

        assertThat(value, is(equalTo(1L)));
    }

    @Test
    public void shouldParseIsoSymbolNoSpaceKiloNumber()
    {
        long value = ByteSizeParser.using(isoSymbol()).parse("1kB");

        assertThat(value, is(equalTo(1_000L)));
    }

    @Test
    public void shouldParseIsoSymbolNoSpaceKibiNumber()
    {
        long value = ByteSizeParser.using(isoSymbol()).parse("1KiB");

        assertThat(value, is(equalTo(1024L)));
    }

    @Test(expected=NumberFormatException.class)
    public void shouldNotParseNonIsoSymbolKibi()
    {
        ByteSizeParser.using(isoSymbol()).parse("1Ki");
    }

    @Test(expected=NumberFormatException.class)
    public void shouldNotParseNonIsoSymbolWrongKibi()
    {
        ByteSizeParser.using(isoSymbol()).parse("1kiB");
    }

    @Test(expected=NumberFormatException.class)
    public void shouldNotParseNonIsoSymbolKilo()
    {
        ByteSizeParser.using(isoSymbol()).parse("1K");
    }

    @Test
    public void shouldParseIsoPrefixSimpleNumber()
    {
        long value = ByteSizeParser.using(isoPrefix()).parse("1");

        assertThat(value, is(equalTo(1L)));
    }

    @Test
    public void shouldParseIsoPrefixNoSpaceKiloNumber()
    {
        long value = ByteSizeParser.using(isoPrefix()).parse("1k");

        assertThat(value, is(equalTo(1_000L)));
    }

    @Test
    public void shouldParseIsoPrefixSpaceKiloNumber()
    {
        long value = ByteSizeParser.using(isoPrefix()).parse("1 k");

        assertThat(value, is(equalTo(1_000L)));
    }

    @Test
    public void shouldParseIsoPrefixNoSpaceKibiNumber()
    {
        long value = ByteSizeParser.using(isoPrefix()).parse("1Ki");

        assertThat(value, is(equalTo(1024L)));
    }

    @Test
    public void shouldParseIsoPrefixSpaceKibiNumber()
    {
        long value = ByteSizeParser.using(isoPrefix()).parse("1 Ki");

        assertThat(value, is(equalTo(1024L)));
    }

    @Test
    public void shouldParseJedecSymbolSimpleNumber()
    {
        long value = ByteSizeParser.using(jedecSymbol()).parse("1");

        assertThat(value, is(equalTo(1L)));
    }

    @Test
    public void shouldParseJedecSymbolSpaceByte()
    {
        long value = ByteSizeParser.using(jedecSymbol()).parse("1 B");

        assertThat(value, is(equalTo(1L)));
    }

    @Test
    public void shouldParseJedecSymbolNoSpaceByte()
    {
        long value = ByteSizeParser.using(jedecSymbol()).parse("1B");

        assertThat(value, is(equalTo(1L)));
    }

    @Test
    public void shouldParseJedecSymbolNoSpaceKiloNumber()
    {
        long value = ByteSizeParser.using(jedecSymbol()).parse("1kB");

        assertThat(value, is(equalTo(1024L)));
    }

    @Test
    public void shouldParseJedecSymbolSpaceKiloNumber()
    {
        long value = ByteSizeParser.using(jedecSymbol()).parse("1 kB");

        assertThat(value, is(equalTo(1024L)));
    }

    @Test
    public void shouldParseJedecSymbolNoSpaceKibiNumber()
    {
        long value = ByteSizeParser.using(jedecSymbol()).parse("1KB");

        assertThat(value, is(equalTo(1024L)));
    }

    @Test
    public void shouldParseJedecSymbolSpaceKibiNumber()
    {
        long value = ByteSizeParser.using(jedecSymbol()).parse("1 KB");

        assertThat(value, is(equalTo(1024L)));
    }

    @Test
    public void shouldParseJedecPrefixSimpleNumber()
    {
        long value = ByteSizeParser.using(jedecPrefix()).parse("1");

        assertThat(value, is(equalTo(1L)));
    }

    @Test
    public void shouldParseJedecPrefixNoSpaceKiloNumber()
    {
        long value = ByteSizeParser.using(jedecPrefix()).parse("1k");

        assertThat(value, is(equalTo(1024L)));
    }

    @Test
    public void shouldParseJedecPrefixSpaceKiloNumber()
    {
        long value = ByteSizeParser.using(jedecPrefix()).parse("1 k");

        assertThat(value, is(equalTo(1024L)));
    }

    @Test
    public void shouldParseJedecPrefixNoSpaceKibiNumber()
    {
        long value = ByteSizeParser.using(jedecPrefix()).parse("1K");

        assertThat(value, is(equalTo(1024L)));
    }

    @Test
    public void shouldParseJedecPrefixSpaceKibiNumber()
    {
        long value = ByteSizeParser.using(jedecPrefix()).parse("1 K");

        assertThat(value, is(equalTo(1024L)));
    }

    @Test
    public void shouldParseSimpleNumberWithDefaultUnits()
    {
        long value = ByteSizeParser.using(isoSymbol()).withDefaultUnits(MiB).parse("1");

        assertThat(value, is(equalTo(1048576L)));
    }

    @Test
    public void shouldParseNumberWithUnitsWithDefaultUnits()
    {
        long value = ByteSizeParser.using(isoSymbol()).withDefaultUnits(MiB).parse("1KiB");

        assertThat(value, is(equalTo(1024L)));
    }

    @Test
    public void shouldParseIntegerWithIntegerInput()
    {
        long value = ByteSizeParser.using(isoSymbol()).withInput(INTEGER).parse("1");

        assertThat(value, is(equalTo(1L)));
    }

    @Test(expected=NumberFormatException.class)
    public void shouldNotParseFloatWithIntegerInput()
    {
        ByteSizeParser.using(isoSymbol()).withInput(INTEGER).parse("1.5KiB");
    }

    @Test
    public void shouldParseIntegerWithFloatInput()
    {
        long value = ByteSizeParser.using(isoSymbol()).withInput(FLOATING_POINT).parse("1");

        assertThat(value, is(equalTo(1L)));
    }

    @Test
    public void shouldParseFloatWithFloatInput()
    {
        long value = ByteSizeParser.using(isoSymbol()).withInput(FLOATING_POINT).parse("1.5KiB");

        assertThat(value, is(equalTo(1536L)));
    }

    @Test
    public void shouldParseNumberWithUnitsWithRequiredUnits()
    {
        long value = ByteSizeParser.using(isoSymbol()).withUnits(REQUIRED).parse("1KiB");

        assertThat(value, is(equalTo(1024L)));
    }

    @Test(expected=NumberFormatException.class)
    public void shouldNotParseNumberWithoutUnitsWithRequiredUnits()
    {
        ByteSizeParser.using(isoSymbol()).withUnits(REQUIRED).parse("1");
    }

    @Test
    public void shouldParseNumberWithUnitsWithOptionalUnits()
    {
        long value = ByteSizeParser.using(isoSymbol()).withUnits(OPTIONAL).parse("1KiB");

        assertThat(value, is(equalTo(1024L)));
    }

    @Test
    public void shouldParseNumberWithoutUnitsWithOptionalUnits()
    {
        long value = ByteSizeParser.using(isoSymbol()).withUnits(OPTIONAL).parse("1");

        assertThat(value, is(equalTo(1L)));
    }

    @Test
    public void shouldParseNumberWithWhitespaceWithOptionalWhitespace()
    {
        long value = ByteSizeParser.using(isoSymbol()).withWhitespace(Whitespace.OPTIONAL).parse("1 KiB");

        assertThat(value, is(equalTo(1024L)));
    }

    @Test
    public void shouldParseNumberWithoutWhitespaceWithOptionalWhitespace()
    {
        long value = ByteSizeParser.using(isoSymbol()).withWhitespace(Whitespace.OPTIONAL).parse("1KiB");

        assertThat(value, is(equalTo(1024L)));
    }

    @Test
    public void shouldParseNumberWithWhitespaceWithRequiredWhitespace()
    {
        long value = ByteSizeParser.using(isoSymbol()).withWhitespace(Whitespace.REQUIRED).parse("1 KiB");

        assertThat(value, is(equalTo(1024L)));
    }

    @Test(expected=NumberFormatException.class)
    public void shouldNotParseNumberWithoutWhitespaceWithRequiredWhitespace()
    {
        ByteSizeParser.using(isoSymbol()).withWhitespace(Whitespace.REQUIRED).parse("1KiB");
    }

    @Test(expected=NumberFormatException.class)
    public void shouldNotParseNumberWithWhitespaceWithForbiddenWhitespace()
    {
        ByteSizeParser.using(isoSymbol()).withWhitespace(Whitespace.NOT_ALLOWED).parse("1 KiB");
    }

    @Test
    public void shouldParseNumberWithoutWhitespaceWithForbiddenWhitespace()
    {
        long value = ByteSizeParser.using(isoSymbol()).withWhitespace(Whitespace.NOT_ALLOWED).parse("1KiB");

        assertThat(value, is(equalTo(1024L)));
    }

    @Test
    public void shouldParseNumberWithCeilingCoersion()
    {
        long value = ByteSizeParser.using(isoSymbol()).withCoersion(CEIL).parse("1.1KiB"); // 1126.4

        assertThat(value, is(equalTo(1127L)));
    }

    @Test
    public void shouldParseNumberWithFloorCoersion()
    {
        long value = ByteSizeParser.using(isoSymbol()).withCoersion(FLOOR).parse("1.2KiB"); // 1228.8

        assertThat(value, is(equalTo(1228L)));
    }

    @Test
    public void shouldParseNumberRoundUpWithRoundCoersion()
    {
        long value = ByteSizeParser.using(isoSymbol()).withCoersion(ROUND).parse("1.2KiB"); // 1228.8

        assertThat(value, is(equalTo(1229L)));
    }

    @Test
    public void shouldParseNumberRoundDownWithRoundCoersion()
    {
        long value = ByteSizeParser.using(isoSymbol()).withCoersion(ROUND).parse("1.1KiB"); // 1126.4

        assertThat(value, is(equalTo(1126L)));
    }
}
