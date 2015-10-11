/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2015 Deutsches Elektronen-Synchrotron
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


/**
 * A "Level of authentication Assurance" (commonly abbreviated to
 * "Level of Assurance" or just "LoA") provides a measure of the
 * likelihood that the person who is authenticating is the person
 * described in public credentials used in that authentication process.
 * <p>
 * Public credentials, such as a SAML assertion or an X.509 certificate carry
 * information about a person, such as their name.  When authenticating (and
 * ultimately authorising) a user, there should be some appreciation of the
 * likelihood that this information is wrong; for example, that the user has
 * stolen the corresponding private material (private key for X.509, usually
 * username & password for SAML), by establishing a false identity when
 * registering with the registrar (the Certificate Authority for X.509, the IdP
 * for SAML), or simply by authenticating with a mechanism where the registrar
 * does not investigate the person's identity.
 * <p>
 * Different profiles exist that provide some common appreciation of this risk.
 * These are described as one of the LoA profiles described below.
 */
public enum LoA
{
    /*
     * The International Grid Trust Federation (IGTF) Authentication Profiles
     * (APs) describe the different policies a Certificate Authority must
     * undertake.  This is a combination of an LoA and implementation
     * requirements; i.e., all IGTF_AP_* LoAs are inherently bound to
     * X.509-based authentication.
     */

    /**
     * Classic X.509 Authorities with secured infrastructure.
     * @see <a href="https://www.eugridpma.org/guidelines/classic">IGTF
     * documentation</a>.
     */
    IGTF_AP_CLASSIC("IGTF-AP:Classic"),

    /**
     * Short-Lived Credential Services (SLCS).
     * @see <a href="https://www.igtf.net/ap/slcs/">IGTF documentation</a>.
     */
    IGTF_AP_SLCS("IGTF-AP:SLCS"),

    /**
     * Member Integrated X.509 Credential Services (MICS) with Secured
     * Infrastructure.
     * @see <a href="https://www.igtf.net/ap/mics/>IGTF documentation</a>.
     */
    IGTF_AP_MICS("IGTF-AP:MICS"),

    /**
     * Identifier-Only Trust Assurance (IOTA) with Secured Infrastructure.
     * @see <a href="https://www.igtf.net/ap/iota/">IGTF documentation</a>.
     */
    IGTF_AP_IOTA("IGTF-AP:IOTA"),

    /**
     * Short-lived Credential Generation Services (SGCS). Discontinued.
     * @see <a href="http://www.tagpma.org/node/5">IGTF documentation</a>.
     */
    IGTF_AP_SGCS("IGTF-AP:SGCS"),

    /**
     * Experimental Authorities.
     * Currently undocumented by IGTF.
     */
    IGTF_AP_EXPERIMENTAL("IGTF-AP:Experimental"),


    /*
     * IGTF has identified that the LoA statements are useful independent from
     * the underlying technology; i.e., the LoA described in IGTF_AP_IOTA is
     * potentially useful in non-X.509 context (such as SAML).
     *
     * To faciliate this, they have provided LoA profiles that are technology
     * agnostic and based on their existing Authentication Profiles (APs): each
     * LoA profile has a corresponding AP.
     *
     * http://wiki.eugridpma.org/Main/IGTFLoAGeneralisation
     */

    /**
     * The IGTF ASPEN LoA profile.  This is derived from {@link #IGTF_AP_SLCS}.
     * @see <a href="http://wiki.eugridpma.org/Main/IGTFLoAGeneralisation">Overview
     * of generalisation</a>.
     */
    IGTF_LOA_ASPEN("IGTF:ASPEN"),

    /**
     * The IGTF BIRCH LoA profile.  This is derived from {@link #IGTF_AP_MICS}.
     * @see <a href="http://wiki.eugridpma.org/Main/IGTFLoAGeneralisation">Overview
     * of generalisation</a>.
     */
    IGTF_LOA_BIRCH("IGTF:BIRCH"),

    /**
     * The IGTF CEDER LoA profile.  This is derived from {@link #IGTF_AP_CLASSIC}.
     * @see <a href="http://wiki.eugridpma.org/Main/IGTFLoAGeneralisation">Overview
     * of generalisation</a>.
     */
    IGTF_LOA_CEDER("IGTF:CEDER"),

    /**
     * The IGTF DOGWOOD LoA profile.  This is derived from {@link #IGTF_AP_IOTA}.
     * @see <a href="http://wiki.eugridpma.org/Main/IGTFLoAGeneralisation">Overview
     * of generalisation</a>.
     */
    IGTF_LOA_DOGWOOD("IGTF:DOGWOOD");

    private static final ImmutableMap<String,LoA> NAME_MAP;

    static {
        ImmutableMap.Builder<String,LoA> builder = ImmutableMap.builder();
        for (LoA loa :LoA.values()) {
            builder.put(loa.getName(), loa);
        }
        NAME_MAP = builder.build();
    }

    private final String _name;

    LoA(String name)
    {
        _name = name;
    }

    public static LoA forName(String name)
    {
        return NAME_MAP.get(name);
    }

    public String getName()
    {
        return _name;
    }
}
