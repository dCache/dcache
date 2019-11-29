/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2015-2019 Deutsches Elektronen-Synchrotron
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
     * The IGTF CEDAR LoA profile.  This is derived from {@link #IGTF_AP_CLASSIC}.
     * @see <a href="http://wiki.eugridpma.org/Main/IGTFLoAGeneralisation">Overview
     * of generalisation</a>.
     */
    IGTF_LOA_CEDAR("IGTF:CEDAR"),

    /**
     * The IGTF DOGWOOD LoA profile.  This is derived from {@link #IGTF_AP_IOTA}.
     * @see <a href="http://wiki.eugridpma.org/Main/IGTFLoAGeneralisation">Overview
     * of generalisation</a>.
     */
    IGTF_LOA_DOGWOOD("IGTF:DOGWOOD"),

    /*
     * LoA statements from the REFEDS Assurance Framework v1.0
     *
     * https://wiki.refeds.org/display/ASS/REFEDS+Assurance+Framework+ver+1.0
     */

    /*
     * Statements about Identifier uniqueness.
     */
    REFEDS_ID_UNIQUE("REFEDS:ID:unique"),
    REFEDS_ID_EPPN_UNIQUE_NO_REASSIGN("REFEDS:ID:eppn-unique-no-reassign"),
    REFEDS_ID_EPPN_UNIQUE_REASSIGN_1Y("REFEDS:ID:eppn-unique-reassign-1y"),

    /*
     * Statements about identity proofing and credential issuance, renewal
     * and replacement.
     */
    REFEDS_IAP_LOW("REFEDS:IAP:low"),
    REFEDS_IAP_MEDIUM("REFEDS:IAP:medium"),
    REFEDS_IAP_HIGH("REFEDS:IAP:high"),
    REFEDS_IAP_LOCAL_ENTERPRISE("REFEDS:IAP:local-enterprise"),

    /*
     * Refeds statements about quality and freshness.
     */
    REFEDS_ATP_1M("REFEDS:ATP:ePA-1m"),
    REFEDS_ATP_1D("REFEDS:ATP:ePA-1d"),

    /*
     * Refeds profiles
     */
    REFEDS_PROFILE_CAPPUCCINO("REFEDS:profile:cappuccino"),
    REFEDS_PROFILE_ESPRESSO("REFEDS:profile:espresso"),


    /*
     * Policies from AARC.
     */

    /**
     * Identity substantially derived from social media or self-signup identity
     * providers (outside the R&E community) on which no further policy controls
     * or qualities are placed. Identity proofing and authenticator are
     * substantially derived from upstream CSPs that are not under the control
     * of the Infrastructure. The Infrastructure ensures uniqueness on the
     * identifiers based on proprietary heuristics.
     * @see <a href="https://aarc-project.eu/guidelines/aarc-g021/">AARC-G021</a>
     */
    AARC_PROFILE_ASSAM("AARC:profile:assam"),


    /*
     * Policies from EGI.
     *
     * EGI AAI currently distinguishes between three LoA levels, similarly to
     * the eID Assurance Framework (eIDAF).
     *
     * https://wiki.egi.eu/wiki/AAI_guide_for_SPs#Level_of_Assurance
     */

    /**
     * Authentication through a social identity provider or other low identity
     * assurance provider.
     */
    EGI_LOW("EGI:low"),

    /**
     * Password/X.509 authentication at the user's home IdP.
     */
    EGI_SUBSTANTIAL("EGI:substantial"),

    /**
     * Substantial + multi-factor authn (not yet supported, TBD).
     */
    EGI_HIGH("EGI:high");

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
