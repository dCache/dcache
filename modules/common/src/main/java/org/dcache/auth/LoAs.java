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

import com.google.common.collect.ImmutableMap;

import java.util.Collection;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.dcache.auth.EntityDefinition.PERSON;

/**
 * A class containing utility methods for working with LoA statements.
 */
public class LoAs
{
    /**
     * A set of universal equivalent LoAs.  These relationships are transitive
     * but not symmetric.
     */
    private static final Map<LoA,LoA> GENERIC_EQUIVALENT_LOA = ImmutableMap.<LoA,LoA>builder()

            /* From https://www.igtf.net/ap/authn-assurance/ */
            .put(LoA.IGTF_AP_SLCS, LoA.IGTF_LOA_ASPEN)
            .put(LoA.IGTF_AP_MICS, LoA.IGTF_LOA_BIRCH)
            .put(LoA.IGTF_AP_CLASSIC, LoA.IGTF_LOA_CEDAR)
            .put(LoA.IGTF_AP_IOTA, LoA.IGTF_LOA_DOGWOOD)

            /* From https://wiki.refeds.org/display/ASS/REFEDS+Assurance+Framework+ver+1.0 */
            .put(LoA.REFEDS_IAP_HIGH, LoA.REFEDS_IAP_MEDIUM)
            .put(LoA.REFEDS_IAP_MEDIUM, LoA.REFEDS_IAP_LOW)
            .build();

    /**
     * These equivalent LoAs if the identified entity is a natural
     * person.  This mapping contains all the generic equivalent mappings.
     */
    private static final Map<LoA,LoA> PERSONAL_EQUIVALENT_LOA = ImmutableMap.<LoA,LoA>builder()
            /* From https://wiki.refeds.org/display/ASS/REFEDS+Assurance+Framework+ver+1.0 */
            .put(LoA.IGTF_LOA_ASPEN, LoA.REFEDS_IAP_LOW)
            .put(LoA.IGTF_LOA_DOGWOOD, LoA.REFEDS_IAP_LOW)
            .put(LoA.IGTF_LOA_BIRCH, LoA.REFEDS_IAP_MEDIUM)
            .put(LoA.IGTF_LOA_CEDAR, LoA.REFEDS_IAP_MEDIUM)
            .putAll(GENERIC_EQUIVALENT_LOA)
            .build();

    private LoAs()
    {
        // prevent instantiation.
    }

    /**
     * Convert a set of asserted LoAs so it includes all equivalent LoAs.
     * @param entity the kind of identity asserted, if known.
     * @param asserted a collection of LoA asserted by some external agent.
     * @return all LoAs for this identity.
     */
    public static EnumSet<LoA> withImpliedLoA(Optional<EntityDefinition> entity,
            Collection<LoA> asserted)
    {
        Map<LoA,LoA> mapping = entity.filter(PERSON::equals)
                .map(e -> PERSONAL_EQUIVALENT_LOA)
                .orElse(GENERIC_EQUIVALENT_LOA);

        EnumSet<LoA> result = EnumSet.copyOf(asserted);

        Collection<LoA> considered = asserted;
        do {
            EnumSet<LoA> additional = considered.stream()
                    .map(mapping::get)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toCollection(() -> EnumSet.noneOf(LoA.class)));
            result.addAll(additional);
            considered = additional;
        } while (!considered.isEmpty());

        return result;
    }
}
