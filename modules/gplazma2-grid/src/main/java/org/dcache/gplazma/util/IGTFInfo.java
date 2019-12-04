/*
 * dCache - http://www.dcache.org/
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
package org.dcache.gplazma.util;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.globus.gsi.gssapi.jaas.GlobusPrincipal;

import java.math.BigInteger;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkState;
import static org.dcache.gplazma.util.IGTFInfo.Type.POLICY;
import static org.dcache.gplazma.util.IGTFInfo.Type.TRUST_ANCHOR;

/**
 * Represents the information contains within an IGTF .info file; either
 * a profile or a trust-anchor file.  The semantics of this class' fields
 * are defined here:
 *
 *     http://wiki.eugridpma.org/Main/IGTFInfoFile
 */
public class IGTFInfo
{
    public enum Type {
        /**
         * Information about a policy.  This is typically used to identify
         * CAs that have been accepted by some grid infrastructure.
         */
        POLICY,

        /** Information about a specific Certificate Authority. */
        TRUST_ANCHOR
    }

    /**
     * The current status of a trust anchor.
     */
    public static enum Status
    {
        DISCONTINUED(false),
        EXPERIMENTAL(false),
        UNACCREDITED(false),
        ACCREDITED_CLASSIC(true),
        ACCREDITED_MICS(true),
        ACCREDITED_SLCS(true),
        ACCREDITED_IOTA(true);

        private final boolean isAccredited;

        Status(boolean isAccredited)
        {
            this.isAccredited = isAccredited;
        }

        public boolean isAccredited()
        {
            return isAccredited;
        }
    }

    private static final Map<String,Status> TO_STATUS;

    static {
        ImmutableMap.Builder<String,Status> mapping = ImmutableMap.builder();
        mapping.put("discontinued", Status.DISCONTINUED);
        mapping.put("experimental", Status.EXPERIMENTAL);
        mapping.put("unaccredited", Status.UNACCREDITED);
        mapping.put("accredited:classic", Status.ACCREDITED_CLASSIC);
        mapping.put("accredited:mics", Status.ACCREDITED_MICS);
        mapping.put("accredited:slcs", Status.ACCREDITED_SLCS);
        mapping.put("accredited:iota", Status.ACCREDITED_IOTA);
        TO_STATUS = mapping.build();
    }

    private static final CharMatcher VALID_HEX = CharMatcher.inRange('0', '9').or(CharMatcher.inRange('A', 'F'));

    private final Type type;

    private boolean immutable;
    private String name;
    private String alias;
    private Version version;
    private URI caUrl;
    private List<URI> crlUrl = ImmutableList.of();
    private URI policyUrl;
    private URI email;
    private Status status;
    private URI url;
    private BigInteger sha1fp0;
    private ImmutableList<GlobusPrincipal> dns = ImmutableList.of();
    private Map<String,String> policyRequires = ImmutableMap.of();
    private List<String> trustAnchorRequires = ImmutableList.of();
    private List<String> obsoletes;
    private List<String> problems = null;

    private IGTFInfo(Type type)
    {
        this.type = type;
    }

    public static IGTFInfo.Builder builder(Type type)
    {
        return new IGTFInfo(type).new Builder();
    }

    public Type getType()
    {
        return type;
    }

    /**
     * The name is some non-null string that represents this TrustAnchor
     * or Policy.  The alias is used, if available, otherwise it is a name
     * derived from the filename.
     */
    public String getName()
    {
        if (alias != null) {
            return alias;
        } else if (name != null) {
            return name;
        }

        throw new IllegalStateException("info file has no alias and filename was not specified");
    }

    public String getAlias()
    {
        return alias;
    }

    public Version getVersion()
    {
        return version;
    }

    public URI getCAUrl()
    {
        return caUrl;
    }

    public List<URI> getCRLUrls()
    {
        return crlUrl;
    }

    public URI getPolicyUrl()
    {
        return policyUrl;
    }

    public URI getEmail()
    {
        return email;
    }

    public Status getStatus()
    {
        return status;
    }

    public URI getUrl()
    {
        return url;
    }

    public BigInteger getSHA1FP0()
    {
        return sha1fp0;
    }

    public GlobusPrincipal getSubjectDN()
    {
        return dns.isEmpty() ? null : dns.get(0);
    }

    public List<GlobusPrincipal> getSubjectDNs()
    {
        return dns;
    }

    public Map<String,String> getPolicyRequires()
    {
        return policyRequires;
    }

    public List<String> getTrustAnchorRequires()
    {
        return trustAnchorRequires;
    }

    public List<String> getObsoletes()
    {
        return obsoletes;
    }

    private void require(boolean isDefined, String name, Type... types)
    {
        for (Type type : types) {
            if (this.type == type && !isDefined) {
                if (problems == null) {
                    problems = new ArrayList<>();
                }
                problems.add("missing '" + name + "'");
            }
        }
    }

    private void checkValid() throws ParserException
    {
        require(version != null, "version", POLICY, TRUST_ANCHOR);
        require(!dns.isEmpty(), "subjectdn", POLICY, TRUST_ANCHOR);
        require(!policyRequires.isEmpty(), "requires", POLICY);
        require(alias != null, "alias", TRUST_ANCHOR);
        require(crlUrl != null, "crl_url", TRUST_ANCHOR);
        require(email != null, "email", TRUST_ANCHOR);
        require(status != null, "status", TRUST_ANCHOR);

        if (problems != null) {
            String description = problems.size() == 1 ? problems.get(0) : problems.toString();
            throw new ParserException("bad info file: " + description);
        }
    }

    public class Builder
    {
        private void checkMutable()
        {
            checkState(!IGTFInfo.this.immutable, "IGTFPolicy.Builder#build has been called");
        }

        public void setAlias(String alias)
        {
            checkMutable();
            IGTFInfo.this.alias = alias;
        }

        public void setFilename(String name)
        {
            checkMutable();

            if (name.startsWith("policy-")) {
                name = name.substring(7);
            }
            if (name.endsWith(".info")) {
                name = name.substring(0, name.length()-5);
            }

            IGTFInfo.this.name = name;
        }

        public void setVersion(String version) throws ParserException
        {
            checkMutable();
            IGTFInfo.this.version = new Version(version);
        }

        public void setCAUrl(String url) throws ParserException
        {
            checkMutable();
            try {
                IGTFInfo.this.caUrl = new URI(url);
            } catch (URISyntaxException e) {
                throw new ParserException(e);
            }
        }

        public void setCRLUrl(String urlList) throws ParserException
        {
            checkMutable();
            try {
                ImmutableList.Builder<URI> urls = ImmutableList.builder();
                for (String url : Splitter.on(';').trimResults().split(urlList)) {
                    urls.add(new URI(url));
                }
                IGTFInfo.this.crlUrl = urls.build();
            } catch (URISyntaxException e) {
                throw new ParserException(e);
            }
        }

        public void setPolicyUrl(String url) throws ParserException
        {
            checkMutable();
            try {
                IGTFInfo.this.policyUrl = new URI(url);
            } catch (URISyntaxException e) {
                throw new ParserException(e);
            }
        }

        public void setEmail(String address) throws ParserException
        {
            checkMutable();
            try {
                IGTFInfo.this.email = new URI("mailto:" + address);
            } catch (URISyntaxException e) {
                throw new ParserException(e);
            }
        }

        public void setStatus(String status) throws ParserException
        {
            checkMutable();
            IGTFInfo.this.status = TO_STATUS.get(status);
            if (IGTFInfo.this.status == null) {
                throw new ParserException("Unknown value '" + status + "'");
            }
        }

        public void setUrl(String url) throws ParserException
        {
            checkMutable();
            try {
                IGTFInfo.this.url = new URI(url);
            } catch (URISyntaxException e) {
                throw new ParserException(e);
            }
        }

        public void setSHA1FP0(String value) throws ParserException
        {
            checkMutable();
            StringBuilder onlyHex = new StringBuilder();
            for (int i = 0; i < value.length(); i++) {
                char c = value.charAt(i);
                if (VALID_HEX.matches(c)) {
                    onlyHex.append(c);
                } else if (c != ':') {
                    throw new ParserException("Invalid character '" + c + "'");
                }
            }

            try {
                IGTFInfo.this.sha1fp0 = new BigInteger(onlyHex.toString(), 16);
            } catch (NumberFormatException e) {
                throw new ParserException("Invalid value: " + e.getMessage(), e);
            }
        }

        public void setSubjectDN(String value) throws ParserException
        {
            checkMutable();

            ImmutableList.Builder<GlobusPrincipal> dns = ImmutableList.builder();
            if (type == TRUST_ANCHOR) {
                GlobusPrincipal p = new GlobusPrincipal(checkValidQuotedDn(value));
                dns.add(p);
                IGTFInfo.this.dns = dns.build();
            } else {
                // REVISIT: how to handle DNs with a double-quote?
                for (String dn : Splitter.on(',').omitEmptyStrings().trimResults().split(value)) {
                    GlobusPrincipal p = new GlobusPrincipal(checkValidQuotedDn(dn));
                    dns.add(p);
                }

                ImmutableList<GlobusPrincipal> list = dns.build();
                checkValid(!list.isEmpty(), "no Distinguished Names");
                IGTFInfo.this.dns = list;
            }
        }

        private String checkValidQuotedDn(String value) throws ParserException
        {
            checkMutable();
            checkValid(value.startsWith("\""), "value does not start with '\"'");
            checkValid(value.endsWith("\""), "value does not end with '\"'");
            checkValid(value.length() > 2, "missing quoted content");
            return value.substring(1, value.length()-1);
        }

        public void setRequires(String value) throws ParserException
        {
            checkMutable();
            switch (type) {
            case POLICY:
                Map<String,String> pr = Splitter.on(',').trimResults().
                        withKeyValueSeparator(Splitter.on('=').trimResults()).split(value);
                IGTFInfo.this.policyRequires = ImmutableMap.copyOf(pr);
                break;
            case TRUST_ANCHOR:
                IGTFInfo.this.trustAnchorRequires =
                        ImmutableList.copyOf(Splitter.on(',').trimResults().split(value));
                break;
            }
        }

        public void setObsoletes(String value)
        {
            checkMutable();
            ImmutableList.Builder<String> obsoletes = ImmutableList.builder();
            for (String item : Splitter.on(',').trimResults().split(value)) {
                obsoletes.add(item);
            }
            IGTFInfo.this.obsoletes = obsoletes.build();
        }

        public IGTFInfo build() throws ParserException
        {
            IGTFInfo.this.immutable = true;
            IGTFInfo.this.checkValid();
            return IGTFInfo.this;
        }
    }

    public static class ParserException extends Exception
    {
        public ParserException(String message) {
            super(message);
        }

        public ParserException(String message, Throwable t) {
            super(message, t);
        }

        public ParserException(Throwable t) {
            super(t.getMessage(), t);
        }
    }

    public static class Version
    {
        private final int major;
        private final int minor;
        private final String pkg;
        private final String value;

        public Version(String value) throws ParserException
        {
            this.value = value;

            int dot = value.indexOf('.');
            if (dot == -1) {
                throw new ParserException("Malformed version: missing '.'");
            }

            try {
                major = Integer.parseInt(value.substring(0, dot));
            } catch (NumberFormatException e) {
                throw new ParserException("Malformed major version: " + e.getMessage(), e);
            }

            int dash = value.indexOf('-');

            String minorValue;
            if (dash == -1) {
                minorValue = value.substring(dot+1);
                pkg = null;
            } else {
                minorValue = value.substring(dot+1, dash);
                pkg = value.substring(dash+1);
            }

            try {
                this.minor = Integer.parseInt(minorValue);
            } catch (NumberFormatException e) {
                throw new ParserException("Malformed minor version: " + e.getMessage(), e);
            }
        }

        public String getVersion()
        {
            return value;
        }

        public int getMajor()
        {
            return major;
        }

        public int getMinor()
        {
            return minor;
        }

        public String getPackage()
        {
            return pkg;
        }

        @Override
        public int hashCode()
        {
            return value.hashCode();
        }

        @Override
        public boolean equals(Object other)
        {
            if (other == this) {
                return true;
            }

            if (!(other instanceof Version)) {
                return false;
            }

            Version v = (Version) other;
            return v.major == this.major &&
                    v.minor == this.minor &&
                    Objects.equals(v.pkg, this.pkg);
        }
    }

    public static void checkValid(boolean isOK, String message) throws ParserException
    {
        if (!isOK) {
            throw new ParserException(message);
        }
    }
}
