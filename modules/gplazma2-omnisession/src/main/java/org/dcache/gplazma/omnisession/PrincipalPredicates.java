/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2021 Deutsches Elektronen-Synchrotron
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
package org.dcache.gplazma.omnisession;

import com.google.common.base.CharMatcher;
import com.google.common.collect.ImmutableMap;
import java.security.Principal;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.security.auth.kerberos.KerberosPrincipal;
import org.dcache.auth.EmailAddressPrincipal;
import org.dcache.auth.EntitlementPrincipal;
import org.dcache.auth.FQANPrincipal;
import org.dcache.auth.GidPrincipal;
import org.dcache.auth.GroupNamePrincipal;
import org.dcache.auth.GroupPrincipal;
import org.dcache.auth.OidcSubjectPrincipal;
import org.dcache.auth.OpenIdGroupPrincipal;
import org.dcache.auth.UidPrincipal;
import org.dcache.auth.UserNamePrincipal;
import org.dcache.util.Exceptions;
import org.globus.gsi.gssapi.jaas.GlobusPrincipal;

/**
 * A utility class to support {@code Predicate<Principal>}.
 */
public class PrincipalPredicates {

    private static final Pattern PRINCIPAL_PREDICATE =
          Pattern.compile("^(?<type>[^:]+):(?<value>([^\"][^ ]*)|(\"[^\"]*\"?)) *");
    private static final Map<String, TestablePrincipal> TESTABLE_PRINCIPAL_BY_LABEL;

    static {
        var builder = ImmutableMap.<String, TestablePrincipal>builder();
        Arrays.stream(TestablePrincipal.values()).forEach(p -> builder.put(p.label, p));
        TESTABLE_PRINCIPAL_BY_LABEL = builder.build();
    }

    /**
     * Indicates a problem parsing a predicate's String representation.
     */
    public static class PredicateParserException extends Exception {

        public PredicateParserException(String message) {
            super(message);
        }
    }

    private PrincipalPredicates() {
        // Prevent instantiation.
    }


    /**
     * State when parsing a predicate.
     */
    private enum ParserState {
        SKIP_WHITESPACE_BEFORE_TYPE,
        READ_TYPE,
        READ_VALUE_INITIAL_CHAR,
        READ_SIMPLE_VALUE,
        READ_QUOTED_VALUE,
    }

    public static class ParsedLine {

        private final Predicate<Principal> predicate;
        private final String remaining;

        ParsedLine(Predicate<Principal> predicate, String remaining) {
            this.predicate = predicate;
            this.remaining = remaining;
        }

        public Predicate<Principal> predicate() {
            return predicate;
        }

        public String remaining() {
            return remaining;
        }
    }

    /**
     * Information about the principals that may be be used to build predicates.
     */
    private static enum TestablePrincipal {
        DISTINGUISHED_NAME("dn", GlobusPrincipal.class) {
            @Override
            void checkName(String name) throws PredicateParserException {
                checkParsable(name.startsWith("/"), "DN does not start with '/'");
            }
        },

        EMAIL("email", EmailAddressPrincipal.class) {
            @Override
            void checkName(String name) throws PredicateParserException {
                checkParsable(EmailAddressPrincipal.isValid(name), "Invalid email address");
            }
        },

        GID("gid", GidPrincipal.class) {
            @Override
            Predicate<Principal> buildNamePredicate(String expectedName)
                  throws PredicateParserException {
                long expectedGid;
                try {
                    expectedGid = Long.parseLong(expectedName);
                } catch (NumberFormatException e) {
                    throw new PredicateParserException(expectedName + " is not an integer");
                }

                return p -> ((GidPrincipal) p).getGid() == expectedGid;
            }
        },

        GROUP_NAME("group", GroupNamePrincipal.class),
        FQAN("fqan", FQANPrincipal.class) {
            @Override
            void checkName(String name) throws PredicateParserException {
                checkParsable(org.dcache.auth.FQAN.isValid(name), "Invalid FQAN");
            }
        },

        KERBEROS_PRINCIPAL("kerberos", KerberosPrincipal.class) {
            @Override
            void checkName(String name) throws PredicateParserException {
                checkParsable(name.contains("@"), "Invalid Kerberos principal");
            }
        },

        OIDC("oidc", OidcSubjectPrincipal.class) {
            @Override
            Predicate<Principal> buildNamePredicate(String expectedName)
                  throws PredicateParserException {
                int lastAt = expectedName.lastIndexOf('@');

                checkParsable(lastAt > -1, "Missing '@' in oidc predicate");
                checkParsable(lastAt > 0, "Last '@' cannot be first character in oidc predicate");
                checkParsable(lastAt < expectedName.length() - 1,
                      "Last '@' cannot be last character in oidc predicate");

                String expectedSubClaim = expectedName.substring(0, lastAt);
                String expectedOP = expectedName.substring(lastAt + 1);

                return p -> {
                    OidcSubjectPrincipal principal = (OidcSubjectPrincipal) p;
                    return principal.getSubClaim().equals(expectedSubClaim)
                          && principal.getOP().equals(expectedOP);
                };
            }
        },

        OIDC_GROUP("oidcgrp", OpenIdGroupPrincipal.class),
        UID("uid", UidPrincipal.class) {
            @Override
            Predicate<Principal> buildNamePredicate(String expectedName)
                  throws PredicateParserException {
                long expectedUid;
                try {
                    expectedUid = Long.parseLong(expectedName);
                } catch (NumberFormatException e) {
                    throw new PredicateParserException(expectedName + " is not a valid uid");
                }

                return p -> ((UidPrincipal) p).getUid() == expectedUid;
            }
        },

        USER_NAME("username", UserNamePrincipal.class),
        ENTITLEMENT("entitlement", EntitlementPrincipal.class);

        private final String label;
        private final Class<? extends Principal> clazz;

        TestablePrincipal(String label, Class<? extends Principal> type) {
            this.label = label;
            this.clazz = type;
        }

        void checkName(String value) throws PredicateParserException {
        }

        Predicate<Principal> buildNamePredicate(String expectedName)
              throws PredicateParserException {
            checkName(expectedName);
            return p -> p.getName().equals(expectedName);
        }

        public Predicate<Principal> buildPredicate(String value)
              throws PredicateParserException {
            Predicate<Principal> predicate = p -> clazz.isAssignableFrom(p.getClass());

            if (GroupPrincipal.class.isAssignableFrom(clazz) && value.contains(",")) {
                int idx = value.lastIndexOf(',');
                String expectedName = value.substring(0, idx);
                boolean isPrimary = parseQualifier(value.substring(idx + 1));

                predicate = predicate.and(buildNamePredicate(expectedName));
                predicate = predicate.and(p -> ((GroupPrincipal) p).isPrimaryGroup() == isPrimary);
            } else {
                predicate = predicate.and(buildNamePredicate(value));
            }

            return predicate;
        }
    }

    private static void checkParsable(boolean isOk, String format, Object... args)
          throws PredicateParserException {
        Exceptions.genericCheck(isOk, PredicateParserException::new, format, args);
    }

    private static boolean parseQualifier(String value) throws PredicateParserException {
        switch (value) {
            case "primary":
                return true;
            case "nonprimary":
                return false;
            default:
                throw new PredicateParserException("Unexpected value \""
                      + value + "\", should be either 'true' or 'false'");
        }
    }

    private static String remaining(String line, int index) {
        int i = index;
        while (i < line.length() && CharMatcher.whitespace().matches(line.charAt(i))) {
            i++;
        }

        if (i == line.length()) {
            return "";
        }
        return line.substring(i);
    }

    public static ParsedLine parseFirstPredicate(String line)
          throws PredicateParserException {
        Matcher m = PRINCIPAL_PREDICATE.matcher(line);

        if (!m.find()) {
            String error = line.isBlank() ? "line is empty" : "missing colon in predicate";
            throw new PredicateParserException(error);
        }

        String typeLabel = m.group("type");
        var type = TESTABLE_PRINCIPAL_BY_LABEL.get(typeLabel);
        checkParsable(type != null, "Unknown principal type \"%s\"", typeLabel);

        String value = m.group("value");
        if (value.charAt(0) == '\"') {
            checkParsable(value.charAt(value.length() - 1) == '\"', "Missing close quote");
            value = value.substring(1, value.length() - 1);
        }

        var predicate = type.buildPredicate(value);
        return new ParsedLine(predicate, line.substring(m.end()));
    }
}
