/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2019 Deutsches Elektronen-Synchrotron
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
package org.dcache.auth;

import org.junit.Test;

import java.util.EnumSet;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

public class LoAsTest
{
    @Test
    public void shouldImplyIgtfAspenFromIgtfSlcs()
    {
        EnumSet<LoA> implied = LoAs.withImpliedLoA(EnumSet.of(LoA.IGTF_AP_SLCS));

        assertThat(implied, containsInAnyOrder(LoA.IGTF_AP_SLCS, LoA.IGTF_LOA_ASPEN, LoA.REFEDS_IAP_LOW));
        assertThat(implied, not(containsInAnyOrder(LoA.IGTF_AP_CLASSIC,
                LoA.IGTF_AP_EXPERIMENTAL, LoA.IGTF_AP_IOTA, LoA.IGTF_AP_MICS, LoA.IGTF_AP_SGCS,
                LoA.IGTF_LOA_BIRCH, LoA.IGTF_LOA_CEDAR, LoA.IGTF_LOA_DOGWOOD,
                LoA.REFEDS_IAP_MEDIUM, LoA.REFEDS_IAP_HIGH)));
    }

    @Test
    public void shouldImplyIgtfBirchFromIgtfMics()
    {
        EnumSet<LoA> implied = LoAs.withImpliedLoA(EnumSet.of(LoA.IGTF_AP_MICS));

        assertThat(implied, containsInAnyOrder(LoA.IGTF_AP_MICS, LoA.IGTF_LOA_BIRCH, LoA.REFEDS_IAP_MEDIUM, LoA.REFEDS_IAP_LOW));
        assertThat(implied, not(containsInAnyOrder(
                LoA.IGTF_LOA_ASPEN, LoA.IGTF_LOA_CEDAR, LoA.IGTF_LOA_DOGWOOD,
                LoA.REFEDS_IAP_HIGH,
                LoA.IGTF_AP_CLASSIC, LoA.IGTF_AP_EXPERIMENTAL, LoA.IGTF_AP_IOTA, LoA.IGTF_AP_SLCS, LoA.IGTF_AP_SGCS)));
    }

    @Test
    public void shouldImplyIgtfDogwoodFromIgtfIota()
    {
        EnumSet<LoA> implied = LoAs.withImpliedLoA(EnumSet.of(LoA.IGTF_AP_IOTA));

        assertThat(implied, containsInAnyOrder(LoA.IGTF_AP_IOTA, LoA.IGTF_LOA_DOGWOOD, LoA.REFEDS_IAP_LOW));
        assertThat(implied, not(containsInAnyOrder(
                LoA.IGTF_LOA_ASPEN, LoA.IGTF_LOA_BIRCH, LoA.IGTF_LOA_CEDAR,
                LoA.REFEDS_IAP_MEDIUM, LoA.REFEDS_IAP_HIGH,
                LoA.IGTF_AP_MICS, LoA.IGTF_AP_EXPERIMENTAL, LoA.IGTF_AP_CLASSIC, LoA.IGTF_AP_SLCS, LoA.IGTF_AP_SGCS)));
    }

    @Test
    public void shouldImplyIgtfCedarFromIgtfClassic()
    {
        EnumSet<LoA> implied = LoAs.withImpliedLoA(EnumSet.of(LoA.IGTF_AP_CLASSIC));

        assertThat(implied, containsInAnyOrder(LoA.IGTF_AP_CLASSIC, LoA.IGTF_LOA_CEDAR, LoA.REFEDS_IAP_MEDIUM, LoA.REFEDS_IAP_LOW));
        assertThat(implied, not(containsInAnyOrder(
                LoA.IGTF_LOA_ASPEN, LoA.IGTF_LOA_BIRCH, LoA.IGTF_LOA_DOGWOOD,
                LoA.REFEDS_IAP_HIGH,
                LoA.IGTF_AP_MICS, LoA.IGTF_AP_EXPERIMENTAL, LoA.IGTF_AP_IOTA, LoA.IGTF_AP_SLCS, LoA.IGTF_AP_SGCS)));
    }

    @Test
    public void shouldImplyRefedsIapLowFromIgtfAspen()
    {
        EnumSet<LoA> implied = LoAs.withImpliedLoA(EnumSet.of(LoA.IGTF_LOA_ASPEN));

        assertThat(implied, containsInAnyOrder(LoA.IGTF_LOA_ASPEN, LoA.REFEDS_IAP_LOW));
        assertThat(implied, not(containsInAnyOrder(
                LoA.IGTF_LOA_BIRCH, LoA.IGTF_LOA_CEDAR, LoA.IGTF_LOA_DOGWOOD,
                LoA.REFEDS_IAP_MEDIUM, LoA.REFEDS_IAP_HIGH)));
    }

    @Test
    public void shouldImplyRefedsIapLowFromIgtfDogwood()
    {
        EnumSet<LoA> implied = LoAs.withImpliedLoA(EnumSet.of(LoA.IGTF_LOA_DOGWOOD));

        assertThat(implied, containsInAnyOrder(LoA.IGTF_LOA_DOGWOOD, LoA.REFEDS_IAP_LOW));
        assertThat(implied, not(containsInAnyOrder(
                LoA.IGTF_LOA_ASPEN, LoA.IGTF_LOA_BIRCH, LoA.IGTF_LOA_CEDAR,
                LoA.REFEDS_IAP_MEDIUM, LoA.REFEDS_IAP_HIGH)));
    }

    @Test
    public void shouldImplyRefedsIapMediumFromIgtfBirch()
    {
        EnumSet<LoA> implied = LoAs.withImpliedLoA(EnumSet.of(LoA.IGTF_LOA_BIRCH));

        assertThat(implied, containsInAnyOrder(LoA.IGTF_LOA_BIRCH, LoA.REFEDS_IAP_LOW, LoA.REFEDS_IAP_MEDIUM));
        assertThat(implied, not(contains(
                LoA.IGTF_LOA_ASPEN, LoA.IGTF_LOA_CEDAR, LoA.IGTF_LOA_DOGWOOD,
                LoA.REFEDS_IAP_HIGH)));
    }

    @Test
    public void shouldImplyRefedsIapMediumFromIgtfCedar()
    {
        EnumSet<LoA> implied = LoAs.withImpliedLoA(EnumSet.of(LoA.IGTF_LOA_CEDAR));

        assertThat(implied, containsInAnyOrder(LoA.IGTF_LOA_CEDAR, LoA.REFEDS_IAP_LOW, LoA.REFEDS_IAP_MEDIUM));
        assertThat(implied, not(contains(
                LoA.IGTF_LOA_ASPEN, LoA.IGTF_LOA_BIRCH, LoA.IGTF_LOA_DOGWOOD,
                LoA.REFEDS_IAP_HIGH)));
    }

    @Test
    public void shouldImplyRefedsIapMediumAndLowFromRefedsIapHigh()
    {
        EnumSet<LoA> implied = LoAs.withImpliedLoA(EnumSet.of(LoA.REFEDS_IAP_HIGH));

        assertThat(implied, containsInAnyOrder(LoA.REFEDS_IAP_LOW, LoA.REFEDS_IAP_MEDIUM, LoA.REFEDS_IAP_HIGH));
    }

    @Test
    public void shouldImplyRefedsIapLowFromRefedsIapMedium()
    {
        EnumSet<LoA> implied = LoAs.withImpliedLoA(EnumSet.of(LoA.REFEDS_IAP_MEDIUM));

        assertThat(implied, containsInAnyOrder(LoA.REFEDS_IAP_LOW, LoA.REFEDS_IAP_MEDIUM));
        assertThat(implied, not(contains(LoA.REFEDS_IAP_HIGH)));
    }

    @Test
    public void shouldNotImplyAnythingFromRefedsIapLow()
    {
        EnumSet<LoA> implied = LoAs.withImpliedLoA(EnumSet.of(LoA.REFEDS_IAP_LOW));

        assertThat(implied, contains(LoA.REFEDS_IAP_LOW));
        assertThat(implied, not(containsInAnyOrder(LoA.REFEDS_IAP_MEDIUM, LoA.REFEDS_IAP_HIGH)));
    }
}
