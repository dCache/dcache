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

}
