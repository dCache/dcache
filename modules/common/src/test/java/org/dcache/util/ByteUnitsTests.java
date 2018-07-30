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

import java.util.Optional;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

public class ByteUnitsTests
{
    @Test
    public void shouldHaveCorrectIsoPrefix()
    {
        assertThat(ByteUnits.isoPrefix().of(ByteUnit.BYTES), equalTo(""));
    }

    @Test
    public void shouldHaveCorrectJedecPrefix()
    {
        assertThat(ByteUnits.jedecPrefix().of(ByteUnit.BYTES), equalTo(""));
    }

    @Test
    public void shouldHaveCorrectIsoSymbol()
    {
        assertThat(ByteUnits.isoSymbol().of(ByteUnit.BYTES), equalTo("B"));
    }

    @Test
    public void shouldHaveCorrectJedecSymbol()
    {
        assertThat(ByteUnits.jedecSymbol().of(ByteUnit.BYTES), equalTo("B"));
    }

    @Test
    public void shouldHaveCorrectKiloIsoPrefix()
    {
        assertThat(ByteUnits.isoPrefix().of(ByteUnit.KB), equalTo("k"));
    }

    @Test(expected=UnsupportedOperationException.class)
    public void shouldThrowExceptionOnKiloJedecPrefix()
    {
        ByteUnits.jedecPrefix().of(ByteUnit.KB);
    }

    @Test
    public void shouldHaveCorrectKiloIsoSymbol()
    {
        assertThat(ByteUnits.isoSymbol().of(ByteUnit.KB), equalTo("kB"));
    }

    @Test(expected=UnsupportedOperationException.class)
    public void shouldThrowExceptionOnKiloJedecSymbol()
    {
        ByteUnits.jedecPrefix().of(ByteUnit.KB);
    }

    @Test
    public void shouldHaveCorrectKibiIsoPrefix()
    {
        assertThat(ByteUnits.isoPrefix().of(ByteUnit.KiB), equalTo("Ki"));
    }

    @Test
    public void shouldHaveCorrectKibiJedecPrefix()
    {
        assertThat(ByteUnits.jedecPrefix().of(ByteUnit.KiB), equalTo("K"));
    }

    @Test
    public void shouldHaveCorrectKibiIsoSymbol()
    {
        assertThat(ByteUnits.isoSymbol().of(ByteUnit.KiB), equalTo("KiB"));
    }

    public void shouldHaveCorrectKibiJedecSymbol()
    {
        assertThat(ByteUnits.jedecSymbol().of(ByteUnit.KiB), equalTo("KB"));
    }

    @Test
    public void shouldHaveCorrectMegaIsoPrefix()
    {
        assertThat(ByteUnits.isoPrefix().of(ByteUnit.MB), equalTo("M"));
    }

    @Test(expected=UnsupportedOperationException.class)
    public void shouldThrowExceptionOnMegaJedecPrefix()
    {
        ByteUnits.jedecPrefix().of(ByteUnit.MB);
    }

    @Test
    public void shouldHaveCorrectMegaIsoSymbol()
    {
        assertThat(ByteUnits.isoSymbol().of(ByteUnit.MB), equalTo("MB"));
    }

    @Test(expected=UnsupportedOperationException.class)
    public void shouldThrowExceptionOnMegaJedecSymbol()
    {
        ByteUnits.jedecSymbol().of(ByteUnit.MB);
    }


    @Test
    public void shouldHaveCorrectMebiIsoPrefix()
    {
        assertThat(ByteUnits.isoPrefix().of(ByteUnit.MiB), equalTo("Mi"));
    }

    @Test
    public void shouldHaveCorrectMebiJedecPrefix()
    {
        assertThat(ByteUnits.jedecPrefix().of(ByteUnit.MiB), equalTo("M"));
    }

    @Test
    public void shouldHaveCorrectMebiIsoSymbol()
    {
        assertThat(ByteUnits.isoSymbol().of(ByteUnit.MiB), equalTo("MiB"));
    }

    @Test
    public void shouldHaveCorrectMebiJedecSymbol()
    {
        assertThat(ByteUnits.jedecSymbol().of(ByteUnit.MiB), equalTo("MB"));
    }


    @Test
    public void shouldHaveCorrectGigaIsoPrefix()
    {
        assertThat(ByteUnits.isoPrefix().of(ByteUnit.GB), equalTo("G"));
    }

    @Test(expected=UnsupportedOperationException.class)
    public void shouldThrowExceptionOnGigaJedecPrefix()
    {
        ByteUnits.jedecPrefix().of(ByteUnit.GB);
    }

    @Test
    public void shouldHaveCorrectGigaIsoSymbol()
    {
        assertThat(ByteUnits.isoSymbol().of(ByteUnit.GB), equalTo("GB"));
    }

    @Test(expected=UnsupportedOperationException.class)
    public void shouldThrowExceptionOnGigaJedecSymbol()
    {
        ByteUnits.jedecSymbol().of(ByteUnit.GB);
    }


    @Test
    public void shouldHaveCorrectGibiIsoPrefix()
    {
        assertThat(ByteUnits.isoPrefix().of(ByteUnit.GiB), equalTo("Gi"));
    }

    @Test
    public void shouldHaveCorrectGibiJedecPrefix()
    {
        assertThat(ByteUnits.jedecPrefix().of(ByteUnit.GiB), equalTo("G"));
    }

    @Test
    public void shouldHaveCorrectGibiIsoSymbol()
    {
        assertThat(ByteUnits.isoSymbol().of(ByteUnit.GiB), equalTo("GiB"));
    }

    @Test
    public void shouldHaveCorrectGibiJedecSymbol()
    {
        assertThat(ByteUnits.jedecSymbol().of(ByteUnit.GiB), equalTo("GB"));
    }


    @Test
    public void shouldHaveCorrectTeraIsoPrefix()
    {
        assertThat(ByteUnits.isoPrefix().of(ByteUnit.TB), equalTo("T"));
    }

    @Test(expected=UnsupportedOperationException.class)
    public void shouldThrowExceptionOnTeraJedecPrefix()
    {
        ByteUnits.jedecPrefix().of(ByteUnit.TB);
    }

    @Test
    public void shouldHaveCorrectTeraIsoSymbol()
    {
        assertThat(ByteUnits.isoSymbol().of(ByteUnit.TB), equalTo("TB"));
    }

    @Test(expected=UnsupportedOperationException.class)
    public void shouldThrowExceptionOnTeraJedecSymbol()
    {
        ByteUnits.jedecSymbol().of(ByteUnit.TB);
    }


    @Test
    public void shouldHaveCorrectTebiIsoPrefix()
    {
        assertThat(ByteUnits.isoPrefix().of(ByteUnit.TiB), equalTo("Ti"));
    }

    @Test
    public void shouldHaveCorrectTebiJedecPrefix()
    {
        assertThat(ByteUnits.jedecPrefix().of(ByteUnit.TiB), equalTo("T"));
    }

    @Test
    public void shouldHaveCorrectTebiIsoSymbol()
    {
        assertThat(ByteUnits.isoSymbol().of(ByteUnit.TiB), equalTo("TiB"));
    }

    @Test
    public void shouldHaveCorrectTebiJedecSymbol()
    {
        assertThat(ByteUnits.jedecSymbol().of(ByteUnit.TiB), equalTo("TB"));
    }


    @Test
    public void shouldHaveCorrectPetaIsoPrefix()
    {
        assertThat(ByteUnits.isoPrefix().of(ByteUnit.PB), equalTo("P"));
    }

    @Test(expected=UnsupportedOperationException.class)
    public void shouldThrowExceptionOnPetaJedecPrefix()
    {
        ByteUnits.jedecPrefix().of(ByteUnit.PB);
    }

    @Test
    public void shouldHaveCorrectPetaIsoSymbol()
    {
        assertThat(ByteUnits.isoSymbol().of(ByteUnit.PB), equalTo("PB"));
    }

    @Test(expected=UnsupportedOperationException.class)
    public void shouldThrowExceptionOnPetaJedecSymbol()
    {
        ByteUnits.jedecSymbol().of(ByteUnit.PB);
    }


    @Test
    public void shouldHaveCorrectPebiIsoPrefix()
    {
        assertThat(ByteUnits.isoPrefix().of(ByteUnit.PiB), equalTo("Pi"));
    }

    @Test
    public void shouldHaveCorrectPebiJedecPrefix()
    {
        assertThat(ByteUnits.jedecPrefix().of(ByteUnit.PiB), equalTo("P"));
    }

    @Test
    public void shouldHaveCorrectPebiIsoSymbol()
    {
        assertThat(ByteUnits.isoSymbol().of(ByteUnit.PiB), equalTo("PiB"));
    }

    @Test
    public void shouldHaveCorrectPebiJedecSymbol()
    {
        assertThat(ByteUnits.jedecSymbol().of(ByteUnit.PiB), equalTo("PB"));
    }


    @Test
    public void shouldHaveCorrectExiIsoPrefix()
    {
        assertThat(ByteUnits.isoPrefix().of(ByteUnit.EB), equalTo("E"));
    }

    @Test(expected=UnsupportedOperationException.class)
    public void shouldThrowExceptionOnExiJedecPrefix()
    {
        ByteUnits.jedecPrefix().of(ByteUnit.EB);
    }

    @Test
    public void shouldHaveCorrectExiIsoSymbol()
    {
        assertThat(ByteUnits.isoSymbol().of(ByteUnit.EB), equalTo("EB"));
    }

    @Test(expected=UnsupportedOperationException.class)
    public void shouldThrowExceptionOnExiJedecSymbol()
    {
        ByteUnits.jedecSymbol().of(ByteUnit.EB);
    }


    @Test
    public void shouldHaveCorrectExbiIsoPrefix()
    {
        assertThat(ByteUnits.isoPrefix().of(ByteUnit.EiB), equalTo("Ei"));
    }

    @Test
    public void shouldHaveCorrectExbiJedecPrefix()
    {
        assertThat(ByteUnits.jedecPrefix().of(ByteUnit.EiB), equalTo("E"));
    }

    @Test
    public void shouldHaveCorrectExbiIsoSymbol()
    {
        assertThat(ByteUnits.isoSymbol().of(ByteUnit.EiB), equalTo("EiB"));
    }

    @Test
    public void shouldHaveCorrectExbiJedecSymbol()
    {
        assertThat(ByteUnits.jedecSymbol().of(ByteUnit.EiB), equalTo("EB"));
    }

    @Test
    public void shouldParseIsoPrefixBytes()
    {
        assertThat(ByteUnits.isoPrefix().parse(""), equalTo(Optional.of(ByteUnit.BYTES)));
    }

    @Test
    public void shouldParseIsoPrefixKilo()
    {
        assertThat(ByteUnits.isoPrefix().parse("k"), equalTo(Optional.of(ByteUnit.KB)));
    }

    @Test
    public void shouldNotParseIsoSymbolKilo()
    {
        assertThat(ByteUnits.isoPrefix().parse("kB"), equalTo(Optional.empty()));
    }

    @Test
    public void shouldParseIsoPrefixKibi()
    {
        assertThat(ByteUnits.isoPrefix().parse("Ki"), equalTo(Optional.of(ByteUnit.KiB)));
    }

    @Test
    public void shouldNotParseWrongIsoPrefixKibi()
    {
        assertThat(ByteUnits.isoPrefix().parse("ki"), equalTo(Optional.empty()));
    }

    @Test
    public void shouldParseIsoPrefixMega()
    {
        assertThat(ByteUnits.isoPrefix().parse("M"), equalTo(Optional.of(ByteUnit.MB)));
    }

    @Test
    public void shouldParseIsoPrefixMibi()
    {
        assertThat(ByteUnits.isoPrefix().parse("Mi"), equalTo(Optional.of(ByteUnit.MiB)));
    }

    @Test
    public void shouldParseIsoPrefixGiga()
    {
        assertThat(ByteUnits.isoPrefix().parse("G"), equalTo(Optional.of(ByteUnit.GB)));
    }

    @Test
    public void shouldParseIsoPrefixGibi()
    {
        assertThat(ByteUnits.isoPrefix().parse("Gi"), equalTo(Optional.of(ByteUnit.GiB)));
    }

    @Test
    public void shouldParseIsoPrefixTera()
    {
        assertThat(ByteUnits.isoPrefix().parse("T"), equalTo(Optional.of(ByteUnit.TB)));
    }

    @Test
    public void shouldParseIsoPrefixTibi()
    {
        assertThat(ByteUnits.isoPrefix().parse("Ti"), equalTo(Optional.of(ByteUnit.TiB)));
    }

    @Test
    public void shouldParseIsoPrefixPeta()
    {
        assertThat(ByteUnits.isoPrefix().parse("P"), equalTo(Optional.of(ByteUnit.PB)));
    }

    @Test
    public void shouldParseIsoPrefixPibi()
    {
        assertThat(ByteUnits.isoPrefix().parse("Pi"), equalTo(Optional.of(ByteUnit.PiB)));
    }

    @Test
    public void shouldParseIsoPrefixExa()
    {
        assertThat(ByteUnits.isoPrefix().parse("E"), equalTo(Optional.of(ByteUnit.EB)));
    }

    @Test
    public void shouldParseIsoPrefixEibi()
    {
        assertThat(ByteUnits.isoPrefix().parse("Ei"), equalTo(Optional.of(ByteUnit.EiB)));
    }

    @Test
    public void shouldParseIsoSymbolBytes()
    {
        assertThat(ByteUnits.isoSymbol().parse("B"), equalTo(Optional.of(ByteUnit.BYTES)));
    }

    @Test
    public void shouldNotParseIsoPrefixKilo()
    {
        assertThat(ByteUnits.isoSymbol().parse("k"), equalTo(Optional.empty()));
    }

    @Test
    public void shouldParseIsoSymbolKilo()
    {
        assertThat(ByteUnits.isoSymbol().parse("kB"), equalTo(Optional.of(ByteUnit.KB)));
    }

    @Test
    public void shouldParseIsoSymbolKibi()
    {
        assertThat(ByteUnits.isoSymbol().parse("KiB"), equalTo(Optional.of(ByteUnit.KiB)));
    }

    @Test
    public void shouldNotParseWrongIsoSymbolKibi()
    {
        assertThat(ByteUnits.isoSymbol().parse("kiB"), equalTo(Optional.empty()));
    }

    @Test
    public void shouldParseIsoSymbolMega()
    {
        assertThat(ByteUnits.isoSymbol().parse("MB"), equalTo(Optional.of(ByteUnit.MB)));
    }

    @Test
    public void shouldParseIsoSymbolMibi()
    {
        assertThat(ByteUnits.isoSymbol().parse("MiB"), equalTo(Optional.of(ByteUnit.MiB)));
    }

    @Test
    public void shouldParseIsoSymbolGiga()
    {
        assertThat(ByteUnits.isoSymbol().parse("GB"), equalTo(Optional.of(ByteUnit.GB)));
    }

    @Test
    public void shouldParseIsoSymbolGibi()
    {
        assertThat(ByteUnits.isoSymbol().parse("GiB"), equalTo(Optional.of(ByteUnit.GiB)));
    }

    @Test
    public void shouldParseIsoSymbolTera()
    {
        assertThat(ByteUnits.isoSymbol().parse("TB"), equalTo(Optional.of(ByteUnit.TB)));
    }

    @Test
    public void shouldParseIsoSymbolTibi()
    {
        assertThat(ByteUnits.isoSymbol().parse("TiB"), equalTo(Optional.of(ByteUnit.TiB)));
    }

    @Test
    public void shouldParseIsoSymbolPeta()
    {
        assertThat(ByteUnits.isoSymbol().parse("PB"), equalTo(Optional.of(ByteUnit.PB)));
    }

    @Test
    public void shouldParseIsoSymbolPibi()
    {
        assertThat(ByteUnits.isoSymbol().parse("PiB"), equalTo(Optional.of(ByteUnit.PiB)));
    }

    @Test
    public void shouldParseIsoSymbolExa()
    {
        assertThat(ByteUnits.isoSymbol().parse("EB"), equalTo(Optional.of(ByteUnit.EB)));
    }

    @Test
    public void shouldParseIsoSymbolEibi()
    {
        assertThat(ByteUnits.isoSymbol().parse("EiB"), equalTo(Optional.of(ByteUnit.EiB)));
    }

    @Test
    public void shouldParseJedecPrefixBytes()
    {
        assertThat(ByteUnits.jedecPrefix().parse(""), equalTo(Optional.of(ByteUnit.BYTES)));
    }

    @Test
    public void shouldParseJedecPrefixKilo()
    {
        assertThat(ByteUnits.jedecPrefix().parse("K"), equalTo(Optional.of(ByteUnit.KiB)));
    }

    @Test
    public void shouldParseJedecAlternativePrefixKilo()
    {
        assertThat(ByteUnits.jedecPrefix().parse("k"), equalTo(Optional.of(ByteUnit.KiB)));
    }

    @Test
    public void shouldParseJedecPrefixMega()
    {
        assertThat(ByteUnits.jedecPrefix().parse("M"), equalTo(Optional.of(ByteUnit.MiB)));
    }

    @Test
    public void shouldParseJedecPrefixGiga()
    {
        assertThat(ByteUnits.jedecPrefix().parse("G"), equalTo(Optional.of(ByteUnit.GiB)));
    }

    @Test
    public void shouldParseJedecPrefixTera()
    {
        assertThat(ByteUnits.jedecPrefix().parse("T"), equalTo(Optional.of(ByteUnit.TiB)));
    }

    @Test
    public void shouldParseJedecPrefixPeta()
    {
        assertThat(ByteUnits.jedecPrefix().parse("P"), equalTo(Optional.of(ByteUnit.PiB)));
    }

    @Test
    public void shouldParseJedecPrefixExa()
    {
        assertThat(ByteUnits.jedecPrefix().parse("E"), equalTo(Optional.of(ByteUnit.EiB)));
    }

    @Test
    public void shouldParseJedecSymbolBytes()
    {
        assertThat(ByteUnits.jedecSymbol().parse("B"), equalTo(Optional.of(ByteUnit.BYTES)));
    }

    @Test
    public void shouldParseJedecSymbolKilo()
    {
        assertThat(ByteUnits.jedecSymbol().parse("KB"), equalTo(Optional.of(ByteUnit.KiB)));
    }

    @Test
    public void shouldParseJedecAlternativeSymbolKilo()
    {
        assertThat(ByteUnits.jedecSymbol().parse("kB"), equalTo(Optional.of(ByteUnit.KiB)));
    }

    @Test
    public void shouldParseJedecSymbolMega()
    {
        assertThat(ByteUnits.jedecSymbol().parse("MB"), equalTo(Optional.of(ByteUnit.MiB)));
    }

    @Test
    public void shouldParseJedecSymbolGiga()
    {
        assertThat(ByteUnits.jedecSymbol().parse("GB"), equalTo(Optional.of(ByteUnit.GiB)));
    }

    @Test
    public void shouldParseJedecSymbolTera()
    {
        assertThat(ByteUnits.jedecSymbol().parse("TB"), equalTo(Optional.of(ByteUnit.TiB)));
    }

    @Test
    public void shouldParseJedecSymbolPeta()
    {
        assertThat(ByteUnits.jedecSymbol().parse("PB"), equalTo(Optional.of(ByteUnit.PiB)));
    }

    @Test
    public void shouldParseJedecSymbolExa()
    {
        assertThat(ByteUnits.jedecSymbol().parse("EB"), equalTo(Optional.of(ByteUnit.EiB)));
    }
}
