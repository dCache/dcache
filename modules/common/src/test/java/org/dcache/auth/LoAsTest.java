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
import java.util.Optional;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

public class LoAsTest
{
    @Test
    public void shouldImplyIgtfAspenFromIgtfSlcsForUnknownEntity()
    {
        EnumSet<LoA> implied = LoAs.withImpliedLoA(Optional.empty(), EnumSet.of(LoA.IGTF_AP_SLCS));

        assertThat(implied, containsInAnyOrder(LoA.IGTF_AP_SLCS, LoA.IGTF_LOA_ASPEN));
        assertThat(implied, not(containsInAnyOrder(LoA.IGTF_AP_CLASSIC,
                LoA.IGTF_AP_EXPERIMENTAL, LoA.IGTF_AP_IOTA, LoA.IGTF_AP_MICS, LoA.IGTF_AP_SGCS,
                LoA.IGTF_LOA_BIRCH, LoA.IGTF_LOA_CEDAR, LoA.IGTF_LOA_DOGWOOD,
                LoA.REFEDS_IAP_LOW, LoA.REFEDS_IAP_MEDIUM, LoA.REFEDS_IAP_HIGH)));
    }

    @Test
    public void shouldImplyIgtfAspenFromIgtfSlcsForHostEntity()
    {
        EnumSet<LoA> implied = LoAs.withImpliedLoA(Optional.of(EntityDefinition.HOST), EnumSet.of(LoA.IGTF_AP_SLCS));

        assertThat(implied, containsInAnyOrder(LoA.IGTF_AP_SLCS, LoA.IGTF_LOA_ASPEN));
        assertThat(implied, not(containsInAnyOrder(LoA.IGTF_AP_CLASSIC,
                LoA.IGTF_AP_EXPERIMENTAL, LoA.IGTF_AP_IOTA, LoA.IGTF_AP_MICS, LoA.IGTF_AP_SGCS,
                LoA.IGTF_LOA_BIRCH, LoA.IGTF_LOA_CEDAR, LoA.IGTF_LOA_DOGWOOD,
                LoA.REFEDS_IAP_LOW, LoA.REFEDS_IAP_MEDIUM, LoA.REFEDS_IAP_HIGH)));
    }

    @Test
    public void shouldImplyIgtfAspenFromIgtfSlcsForRobotEntity()
    {
        EnumSet<LoA> implied = LoAs.withImpliedLoA(Optional.of(EntityDefinition.ROBOT), EnumSet.of(LoA.IGTF_AP_SLCS));

        assertThat(implied, containsInAnyOrder(LoA.IGTF_AP_SLCS, LoA.IGTF_LOA_ASPEN));
        assertThat(implied, not(containsInAnyOrder(LoA.IGTF_AP_CLASSIC,
                LoA.IGTF_AP_EXPERIMENTAL, LoA.IGTF_AP_IOTA, LoA.IGTF_AP_MICS, LoA.IGTF_AP_SGCS,
                LoA.IGTF_LOA_BIRCH, LoA.IGTF_LOA_CEDAR, LoA.IGTF_LOA_DOGWOOD,
                LoA.REFEDS_IAP_LOW, LoA.REFEDS_IAP_MEDIUM, LoA.REFEDS_IAP_HIGH)));
    }

    @Test
    public void shouldImplyIgtfAspenFromIgtfSlcsForPersonEntity()
    {
        EnumSet<LoA> implied = LoAs.withImpliedLoA(Optional.of(EntityDefinition.PERSON), EnumSet.of(LoA.IGTF_AP_SLCS));

        assertThat(implied, containsInAnyOrder(LoA.IGTF_AP_SLCS, LoA.IGTF_LOA_ASPEN, LoA.REFEDS_IAP_LOW));
        assertThat(implied, not(containsInAnyOrder(LoA.IGTF_AP_CLASSIC,
                LoA.IGTF_AP_EXPERIMENTAL, LoA.IGTF_AP_IOTA, LoA.IGTF_AP_MICS, LoA.IGTF_AP_SGCS,
                LoA.IGTF_LOA_BIRCH, LoA.IGTF_LOA_CEDAR, LoA.IGTF_LOA_DOGWOOD,
                LoA.REFEDS_IAP_MEDIUM, LoA.REFEDS_IAP_HIGH)));
    }

    @Test
    public void shouldImplyIgtfBirchFromIgtfMicsForUnknownEntity()
    {
        EnumSet<LoA> implied = LoAs.withImpliedLoA(Optional.empty(), EnumSet.of(LoA.IGTF_AP_MICS));

        assertThat(implied, containsInAnyOrder(LoA.IGTF_AP_MICS, LoA.IGTF_LOA_BIRCH));
        assertThat(implied, not(containsInAnyOrder(
                LoA.IGTF_LOA_ASPEN, LoA.IGTF_LOA_CEDAR, LoA.IGTF_LOA_DOGWOOD,
                LoA.REFEDS_IAP_LOW, LoA.REFEDS_IAP_MEDIUM, LoA.REFEDS_IAP_HIGH,
                LoA.IGTF_AP_CLASSIC, LoA.IGTF_AP_EXPERIMENTAL, LoA.IGTF_AP_IOTA, LoA.IGTF_AP_SLCS, LoA.IGTF_AP_SGCS)));
    }

    @Test
    public void shouldImplyIgtfBirchFromIgtfMicsForHostEntity()
    {
        EnumSet<LoA> implied = LoAs.withImpliedLoA(Optional.of(EntityDefinition.HOST), EnumSet.of(LoA.IGTF_AP_MICS));

        assertThat(implied, containsInAnyOrder(LoA.IGTF_AP_MICS, LoA.IGTF_LOA_BIRCH));
        assertThat(implied, not(containsInAnyOrder(
                LoA.IGTF_LOA_ASPEN, LoA.IGTF_LOA_CEDAR, LoA.IGTF_LOA_DOGWOOD,
                LoA.REFEDS_IAP_LOW, LoA.REFEDS_IAP_MEDIUM, LoA.REFEDS_IAP_HIGH,
                LoA.IGTF_AP_CLASSIC, LoA.IGTF_AP_EXPERIMENTAL, LoA.IGTF_AP_IOTA, LoA.IGTF_AP_SLCS, LoA.IGTF_AP_SGCS)));
    }

    @Test
    public void shouldImplyIgtfBirchFromIgtfMicsForRobotEntity()
    {
        EnumSet<LoA> implied = LoAs.withImpliedLoA(Optional.of(EntityDefinition.ROBOT), EnumSet.of(LoA.IGTF_AP_MICS));

        assertThat(implied, containsInAnyOrder(LoA.IGTF_AP_MICS, LoA.IGTF_LOA_BIRCH));
        assertThat(implied, not(containsInAnyOrder(
                LoA.IGTF_LOA_ASPEN, LoA.IGTF_LOA_CEDAR, LoA.IGTF_LOA_DOGWOOD,
                LoA.REFEDS_IAP_LOW, LoA.REFEDS_IAP_MEDIUM, LoA.REFEDS_IAP_HIGH,
                LoA.IGTF_AP_CLASSIC, LoA.IGTF_AP_EXPERIMENTAL, LoA.IGTF_AP_IOTA, LoA.IGTF_AP_SLCS, LoA.IGTF_AP_SGCS)));
    }

    @Test
    public void shouldImplyIgtfBirchFromIgtfMicsForPersonEntity()
    {
        EnumSet<LoA> implied = LoAs.withImpliedLoA(Optional.of(EntityDefinition.PERSON), EnumSet.of(LoA.IGTF_AP_MICS));

        assertThat(implied, containsInAnyOrder(LoA.IGTF_AP_MICS, LoA.IGTF_LOA_BIRCH, LoA.REFEDS_IAP_MEDIUM, LoA.REFEDS_IAP_LOW));
        assertThat(implied, not(containsInAnyOrder(
                LoA.IGTF_LOA_ASPEN, LoA.IGTF_LOA_CEDAR, LoA.IGTF_LOA_DOGWOOD,
                LoA.REFEDS_IAP_HIGH,
                LoA.IGTF_AP_CLASSIC, LoA.IGTF_AP_EXPERIMENTAL, LoA.IGTF_AP_IOTA, LoA.IGTF_AP_SLCS, LoA.IGTF_AP_SGCS)));
    }

    @Test
    public void shouldImplyIgtfDogwoodFromIgtfIotaForUnknownEntity()
    {
        EnumSet<LoA> implied = LoAs.withImpliedLoA(Optional.empty(), EnumSet.of(LoA.IGTF_AP_IOTA));

        assertThat(implied, containsInAnyOrder(LoA.IGTF_AP_IOTA, LoA.IGTF_LOA_DOGWOOD));
        assertThat(implied, not(containsInAnyOrder(
                LoA.IGTF_LOA_ASPEN, LoA.IGTF_LOA_BIRCH, LoA.IGTF_LOA_CEDAR,
                LoA.REFEDS_IAP_LOW, LoA.REFEDS_IAP_MEDIUM, LoA.REFEDS_IAP_HIGH,
                LoA.IGTF_AP_MICS, LoA.IGTF_AP_EXPERIMENTAL, LoA.IGTF_AP_CLASSIC, LoA.IGTF_AP_SLCS, LoA.IGTF_AP_SGCS)));
    }

    @Test
    public void shouldImplyIgtfDogwoodFromIgtfIotaForHostEntity()
    {
        EnumSet<LoA> implied = LoAs.withImpliedLoA(Optional.of(EntityDefinition.HOST), EnumSet.of(LoA.IGTF_AP_IOTA));

        assertThat(implied, containsInAnyOrder(LoA.IGTF_AP_IOTA, LoA.IGTF_LOA_DOGWOOD));
        assertThat(implied, not(containsInAnyOrder(
                LoA.IGTF_LOA_ASPEN, LoA.IGTF_LOA_BIRCH, LoA.IGTF_LOA_CEDAR,
                LoA.REFEDS_IAP_LOW, LoA.REFEDS_IAP_MEDIUM, LoA.REFEDS_IAP_HIGH,
                LoA.IGTF_AP_MICS, LoA.IGTF_AP_EXPERIMENTAL, LoA.IGTF_AP_CLASSIC, LoA.IGTF_AP_SLCS, LoA.IGTF_AP_SGCS)));
    }

    @Test
    public void shouldImplyIgtfDogwoodFromIgtfIotaForRobotEntity()
    {
        EnumSet<LoA> implied = LoAs.withImpliedLoA(Optional.of(EntityDefinition.ROBOT), EnumSet.of(LoA.IGTF_AP_IOTA));

        assertThat(implied, containsInAnyOrder(LoA.IGTF_AP_IOTA, LoA.IGTF_LOA_DOGWOOD));
        assertThat(implied, not(containsInAnyOrder(
                LoA.IGTF_LOA_ASPEN, LoA.IGTF_LOA_BIRCH, LoA.IGTF_LOA_CEDAR,
                LoA.REFEDS_IAP_LOW, LoA.REFEDS_IAP_MEDIUM, LoA.REFEDS_IAP_HIGH,
                LoA.IGTF_AP_MICS, LoA.IGTF_AP_EXPERIMENTAL, LoA.IGTF_AP_CLASSIC, LoA.IGTF_AP_SLCS, LoA.IGTF_AP_SGCS)));
    }

    @Test
    public void shouldImplyIgtfDogwoodFromIgtfIotaForPersonEntity()
    {
        EnumSet<LoA> implied = LoAs.withImpliedLoA(Optional.of(EntityDefinition.PERSON), EnumSet.of(LoA.IGTF_AP_IOTA));

        assertThat(implied, containsInAnyOrder(LoA.IGTF_AP_IOTA, LoA.IGTF_LOA_DOGWOOD, LoA.REFEDS_IAP_LOW));
        assertThat(implied, not(containsInAnyOrder(
                LoA.IGTF_LOA_ASPEN, LoA.IGTF_LOA_BIRCH, LoA.IGTF_LOA_CEDAR,
                LoA.REFEDS_IAP_MEDIUM, LoA.REFEDS_IAP_HIGH,
                LoA.IGTF_AP_MICS, LoA.IGTF_AP_EXPERIMENTAL, LoA.IGTF_AP_CLASSIC, LoA.IGTF_AP_SLCS, LoA.IGTF_AP_SGCS)));
    }

    @Test
    public void shouldImplyIgtfCedarFromIgtfClassicForUnknownEntity()
    {
        EnumSet<LoA> implied = LoAs.withImpliedLoA(Optional.empty(), EnumSet.of(LoA.IGTF_AP_CLASSIC));

        assertThat(implied, containsInAnyOrder(LoA.IGTF_AP_CLASSIC, LoA.IGTF_LOA_CEDAR));
        assertThat(implied, not(containsInAnyOrder(
                LoA.IGTF_LOA_ASPEN, LoA.IGTF_LOA_BIRCH, LoA.IGTF_LOA_DOGWOOD,
                LoA.REFEDS_IAP_LOW, LoA.REFEDS_IAP_MEDIUM, LoA.REFEDS_IAP_HIGH,
                LoA.IGTF_AP_MICS, LoA.IGTF_AP_EXPERIMENTAL, LoA.IGTF_AP_IOTA, LoA.IGTF_AP_SLCS, LoA.IGTF_AP_SGCS)));
    }

    @Test
    public void shouldImplyIgtfCedarFromIgtfClassicForHostEntity()
    {
        EnumSet<LoA> implied = LoAs.withImpliedLoA(Optional.of(EntityDefinition.HOST), EnumSet.of(LoA.IGTF_AP_CLASSIC));

        assertThat(implied, containsInAnyOrder(LoA.IGTF_AP_CLASSIC, LoA.IGTF_LOA_CEDAR));
        assertThat(implied, not(containsInAnyOrder(
                LoA.IGTF_LOA_ASPEN, LoA.IGTF_LOA_BIRCH, LoA.IGTF_LOA_DOGWOOD,
                LoA.REFEDS_IAP_LOW, LoA.REFEDS_IAP_MEDIUM, LoA.REFEDS_IAP_HIGH,
                LoA.IGTF_AP_MICS, LoA.IGTF_AP_EXPERIMENTAL, LoA.IGTF_AP_IOTA, LoA.IGTF_AP_SLCS, LoA.IGTF_AP_SGCS)));
    }

    @Test
    public void shouldImplyIgtfCedarFromIgtfClassicForRobotEntity()
    {
        EnumSet<LoA> implied = LoAs.withImpliedLoA(Optional.of(EntityDefinition.ROBOT), EnumSet.of(LoA.IGTF_AP_CLASSIC));

        assertThat(implied, containsInAnyOrder(LoA.IGTF_AP_CLASSIC, LoA.IGTF_LOA_CEDAR));
        assertThat(implied, not(containsInAnyOrder(
                LoA.IGTF_LOA_ASPEN, LoA.IGTF_LOA_BIRCH, LoA.IGTF_LOA_DOGWOOD,
                LoA.REFEDS_IAP_LOW, LoA.REFEDS_IAP_MEDIUM, LoA.REFEDS_IAP_HIGH,
                LoA.IGTF_AP_MICS, LoA.IGTF_AP_EXPERIMENTAL, LoA.IGTF_AP_IOTA, LoA.IGTF_AP_SLCS, LoA.IGTF_AP_SGCS)));
    }

    @Test
    public void shouldImplyIgtfCedarFromIgtfClassicForPersonEntity()
    {
        EnumSet<LoA> implied = LoAs.withImpliedLoA(Optional.of(EntityDefinition.PERSON), EnumSet.of(LoA.IGTF_AP_CLASSIC));

        assertThat(implied, containsInAnyOrder(LoA.IGTF_AP_CLASSIC, LoA.IGTF_LOA_CEDAR, LoA.REFEDS_IAP_MEDIUM, LoA.REFEDS_IAP_LOW));
        assertThat(implied, not(containsInAnyOrder(
                LoA.IGTF_LOA_ASPEN, LoA.IGTF_LOA_BIRCH, LoA.IGTF_LOA_DOGWOOD,
                LoA.REFEDS_IAP_HIGH,
                LoA.IGTF_AP_MICS, LoA.IGTF_AP_EXPERIMENTAL, LoA.IGTF_AP_IOTA, LoA.IGTF_AP_SLCS, LoA.IGTF_AP_SGCS)));
    }

    @Test
    public void shouldNotImplyRefedsIapLowFromIgtfAspenForUnknownEntity()
    {
        EnumSet<LoA> implied = LoAs.withImpliedLoA(Optional.empty(), EnumSet.of(LoA.IGTF_LOA_ASPEN));

        assertThat(implied, containsInAnyOrder(LoA.IGTF_LOA_ASPEN));
        assertThat(implied, not(containsInAnyOrder(
                LoA.IGTF_LOA_BIRCH, LoA.IGTF_LOA_CEDAR, LoA.IGTF_LOA_DOGWOOD,
                LoA.REFEDS_IAP_LOW, LoA.REFEDS_IAP_MEDIUM, LoA.REFEDS_IAP_HIGH)));
    }

    @Test
    public void shouldImplyRefedsIapLowFromIgtfAspenForPersonEntity()
    {
        EnumSet<LoA> implied = LoAs.withImpliedLoA(Optional.of(EntityDefinition.PERSON), EnumSet.of(LoA.IGTF_LOA_ASPEN));

        assertThat(implied, containsInAnyOrder(LoA.IGTF_LOA_ASPEN, LoA.REFEDS_IAP_LOW));
        assertThat(implied, not(containsInAnyOrder(
                LoA.IGTF_LOA_BIRCH, LoA.IGTF_LOA_CEDAR, LoA.IGTF_LOA_DOGWOOD,
                LoA.REFEDS_IAP_MEDIUM, LoA.REFEDS_IAP_HIGH)));
    }

    @Test
    public void shouldNotImplyRefedsIapLowFromIgtfDogwoodForUnknownEntity()
    {
        EnumSet<LoA> implied = LoAs.withImpliedLoA(Optional.empty(), EnumSet.of(LoA.IGTF_LOA_DOGWOOD));

        assertThat(implied, containsInAnyOrder(LoA.IGTF_LOA_DOGWOOD));
        assertThat(implied, not(containsInAnyOrder(
                LoA.IGTF_LOA_ASPEN, LoA.IGTF_LOA_BIRCH, LoA.IGTF_LOA_CEDAR,
                LoA.REFEDS_IAP_LOW, LoA.REFEDS_IAP_MEDIUM, LoA.REFEDS_IAP_HIGH)));
    }

    @Test
    public void shouldImplyRefedsIapLowFromIgtfDogwoodForPersonEntity()
    {
        EnumSet<LoA> implied = LoAs.withImpliedLoA(Optional.of(EntityDefinition.PERSON), EnumSet.of(LoA.IGTF_LOA_DOGWOOD));

        assertThat(implied, containsInAnyOrder(LoA.IGTF_LOA_DOGWOOD, LoA.REFEDS_IAP_LOW));
        assertThat(implied, not(containsInAnyOrder(
                LoA.IGTF_LOA_ASPEN, LoA.IGTF_LOA_BIRCH, LoA.IGTF_LOA_CEDAR,
                LoA.REFEDS_IAP_MEDIUM, LoA.REFEDS_IAP_HIGH)));
    }

    @Test
    public void shouldNotImplyRefedsIapMediumFromIgtfBirchForUnknownEntity()
    {
        EnumSet<LoA> implied = LoAs.withImpliedLoA(Optional.empty(), EnumSet.of(LoA.IGTF_LOA_BIRCH));

        assertThat(implied, containsInAnyOrder(LoA.IGTF_LOA_BIRCH));
        assertThat(implied, not(contains(
                LoA.IGTF_LOA_ASPEN, LoA.IGTF_LOA_CEDAR, LoA.IGTF_LOA_DOGWOOD,
                LoA.REFEDS_IAP_LOW, LoA.REFEDS_IAP_MEDIUM, LoA.REFEDS_IAP_HIGH)));
    }

    @Test
    public void shouldImplyRefedsIapMediumFromIgtfBirchForPersonEntity()
    {
        EnumSet<LoA> implied = LoAs.withImpliedLoA(Optional.of(EntityDefinition.PERSON), EnumSet.of(LoA.IGTF_LOA_BIRCH));

        assertThat(implied, containsInAnyOrder(LoA.IGTF_LOA_BIRCH, LoA.REFEDS_IAP_LOW, LoA.REFEDS_IAP_MEDIUM));
        assertThat(implied, not(contains(
                LoA.IGTF_LOA_ASPEN, LoA.IGTF_LOA_CEDAR, LoA.IGTF_LOA_DOGWOOD,
                LoA.REFEDS_IAP_HIGH)));
    }

    @Test
    public void shouldNotImplyRefedsIapMediumFromIgtfCedarForUnknownEntity()
    {
        EnumSet<LoA> implied = LoAs.withImpliedLoA(Optional.empty(), EnumSet.of(LoA.IGTF_LOA_CEDAR));

        assertThat(implied, containsInAnyOrder(LoA.IGTF_LOA_CEDAR));
        assertThat(implied, not(contains(
                LoA.IGTF_LOA_ASPEN, LoA.IGTF_LOA_BIRCH, LoA.IGTF_LOA_DOGWOOD,
                LoA.REFEDS_IAP_LOW, LoA.REFEDS_IAP_MEDIUM, LoA.REFEDS_IAP_HIGH)));
    }

    @Test
    public void shouldImplyRefedsIapMediumFromIgtfCedarForPersonEntity()
    {
        EnumSet<LoA> implied = LoAs.withImpliedLoA(Optional.of(EntityDefinition.PERSON), EnumSet.of(LoA.IGTF_LOA_CEDAR));

        assertThat(implied, containsInAnyOrder(LoA.IGTF_LOA_CEDAR, LoA.REFEDS_IAP_LOW, LoA.REFEDS_IAP_MEDIUM));
        assertThat(implied, not(contains(
                LoA.IGTF_LOA_ASPEN, LoA.IGTF_LOA_BIRCH, LoA.IGTF_LOA_DOGWOOD,
                LoA.REFEDS_IAP_HIGH)));
    }

    @Test
    public void shouldImplyRefedsIapMediumAndLowFromRefedsIapHighForUnknownEntity()
    {
        EnumSet<LoA> implied = LoAs.withImpliedLoA(Optional.empty(), EnumSet.of(LoA.REFEDS_IAP_HIGH));

        assertThat(implied, containsInAnyOrder(LoA.REFEDS_IAP_LOW, LoA.REFEDS_IAP_MEDIUM, LoA.REFEDS_IAP_HIGH));
    }

    @Test
    public void shouldImplyRefedsIapMediumAndLowFromRefedsIapHighEntity()
    {
        EnumSet<LoA> implied = LoAs.withImpliedLoA(Optional.of(EntityDefinition.PERSON), EnumSet.of(LoA.REFEDS_IAP_HIGH));

        assertThat(implied, containsInAnyOrder(LoA.REFEDS_IAP_LOW, LoA.REFEDS_IAP_MEDIUM, LoA.REFEDS_IAP_HIGH));
    }

    @Test
    public void shouldImplyRefedsIapLowFromRefedsIapMediumForUnknownEntity()
    {
        EnumSet<LoA> implied = LoAs.withImpliedLoA(Optional.empty(), EnumSet.of(LoA.REFEDS_IAP_MEDIUM));

        assertThat(implied, containsInAnyOrder(LoA.REFEDS_IAP_LOW, LoA.REFEDS_IAP_MEDIUM));
        assertThat(implied, not(contains(LoA.REFEDS_IAP_HIGH)));
    }

    @Test
    public void shouldImplyRefedsIapLowFromRefedsIapMediumForPersonEntity()
    {
        EnumSet<LoA> implied = LoAs.withImpliedLoA(Optional.of(EntityDefinition.PERSON), EnumSet.of(LoA.REFEDS_IAP_MEDIUM));

        assertThat(implied, containsInAnyOrder(LoA.REFEDS_IAP_LOW, LoA.REFEDS_IAP_MEDIUM));
        assertThat(implied, not(contains(LoA.REFEDS_IAP_HIGH)));
    }

    @Test
    public void shouldNotImplyAnythingFromRefedsIapLowForUnknownEntity()
    {
        EnumSet<LoA> implied = LoAs.withImpliedLoA(Optional.empty(), EnumSet.of(LoA.REFEDS_IAP_LOW));

        assertThat(implied, contains(LoA.REFEDS_IAP_LOW));
        assertThat(implied, not(containsInAnyOrder(LoA.REFEDS_IAP_MEDIUM, LoA.REFEDS_IAP_HIGH)));
    }

    @Test
    public void shouldNotImplyAnythingFromRefedsIapLowForPersonEntity()
    {
        EnumSet<LoA> implied = LoAs.withImpliedLoA(Optional.of(EntityDefinition.PERSON), EnumSet.of(LoA.REFEDS_IAP_LOW));

        assertThat(implied, contains(LoA.REFEDS_IAP_LOW));
        assertThat(implied, not(containsInAnyOrder(LoA.REFEDS_IAP_MEDIUM, LoA.REFEDS_IAP_HIGH)));
    }
}
