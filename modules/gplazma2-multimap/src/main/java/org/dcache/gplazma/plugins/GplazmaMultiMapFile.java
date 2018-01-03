package org.dcache.gplazma.plugins;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import org.globus.gsi.gssapi.jaas.GlobusPrincipal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.kerberos.KerberosPrincipal;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.Principal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.dcache.auth.EmailAddressPrincipal;
import org.dcache.auth.FQANPrincipal;
import org.dcache.auth.GidPrincipal;
import org.dcache.auth.GroupNamePrincipal;
import org.dcache.auth.GroupPrincipal;
import org.dcache.auth.OidcSubjectPrincipal;
import org.dcache.auth.OpenIdGroupPrincipal;
import org.dcache.auth.UidPrincipal;
import org.dcache.auth.UserNamePrincipal;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.plugins.exceptions.GplazmaParseMapFileException;
import org.dcache.util.Args;
import org.dcache.util.Exceptions;

import static org.dcache.gplazma.plugins.exceptions.GplazmaParseMapFileException.checkFormat;

public class GplazmaMultiMapFile
{
    /**
     * Information about the principals that may be mapped.
     */
    private enum MappablePrincipal
    {
        DISTINGUISHED_NAME("dn", GlobusPrincipal.class),
        EMAIL("email", EmailAddressPrincipal.class),

        GID("gid", GidPrincipal.class) {
            @Override
            public void checkValue(String value) throws GplazmaParseMapFileException
            {
                try {
                    Long.parseLong(value);
                } catch (NumberFormatException e) {
                    throw new GplazmaParseMapFileException("gid value " +
                            value + " is not an integer: " + e.getMessage());
                }
            }
        },

        GROUP_NAME("group", GroupNamePrincipal.class),
        FQAN("fqan", FQANPrincipal.class),
        KERBEROS_PRINCIPAL("kerberos", KerberosPrincipal.class),
        OIDC("oidc", OidcSubjectPrincipal.class),
        OIDC_GROUP("oidcgrp", OpenIdGroupPrincipal.class),
        UID("uid", UidPrincipal.class),
        USER_NAME("username", UserNamePrincipal.class);

        private final String label;
        private final Class<? extends Principal> groupType;

        MappablePrincipal(String label, Class<? extends Principal> type)
        {
            this.label = label;
            this.groupType = type;
        }

        public void checkValue(String value) throws GplazmaParseMapFileException
        {
        }

        public Principal buildPrincipal(String value)
                throws GplazmaParseMapFileException
        {
            try {
                if (GroupPrincipal.class.isAssignableFrom(groupType)) {
                    List<String> parts = Splitter.on(',').splitToList(value);
                    checkFormat(parts.size() < 3,
                            "Too many commas in \"%s\" for %s", value, label);
                    boolean isPrimary = parts.size() == 2 ? Boolean.parseBoolean(parts.get(1)) : false;
                    return groupType.getConstructor(String.class, Boolean.TYPE)
                            .newInstance(parts.get(0), isPrimary);
                } else {
                    return groupType.getConstructor(String.class).newInstance(value);
                }
            } catch (NoSuchMethodException|IllegalAccessException|InstantiationException e) {
                throw new RuntimeException("Failed to create principal: " + e);
            } catch (InvocationTargetException e) {
                throw new GplazmaParseMapFileException("Failed to create "
                        + label + " principal \"" + value + "\": "
                        + Exceptions.messageOrClassName((Exception)e.getCause()));
            }
        }

        public PrincipalMatcher buildMatcher(String value)
                throws GplazmaParseMapFileException
        {
            if (GroupPrincipal.class.isAssignableFrom(groupType) && !value.contains(",")) {
                checkValue(value);
                return p -> groupType.isAssignableFrom(p.getClass())
                        && p.getName().equals(value);
            }
            return buildPrincipal(value)::equals;
        }
    }

    @FunctionalInterface
    public interface PrincipalMatcher {
        boolean matches(Principal principal);
    }

    private static final Logger LOG = LoggerFactory.getLogger(GplazmaMultiMapFile.class);
    private static final Map<String,MappablePrincipal> LABEL_TO_PRINCIPAL;

    static {
        ImmutableMap.Builder<String,MappablePrincipal> builder = ImmutableMap.<String,MappablePrincipal>builder();
        for (MappablePrincipal p : MappablePrincipal.values()) {
            builder.put(p.label, p);
        }
        LABEL_TO_PRINCIPAL = builder.build();
    }

    private final Path file;
    private Instant lastLoaded = Instant.EPOCH;
    private Instant nextStat = Instant.now();
    private Consumer<String> warningsConsumer = LOG::warn;

    private Map<PrincipalMatcher,Set<Principal>> map = Collections.emptyMap();

    public GplazmaMultiMapFile(Path file)
    {
        this.file = file;
    }

    public synchronized void setWarningConsumer(Consumer<String> consumer)
    {
        warningsConsumer = consumer;
    }

    public synchronized Map<PrincipalMatcher,Set<Principal>> mapping()
            throws AuthenticationException
    {
        if (!Instant.now().isBefore(nextStat)) {
            nextStat = Instant.now().plusMillis(100);

            try {
                Instant mtime = Files.readAttributes(file, BasicFileAttributes.class)
                        .lastModifiedTime().toInstant();

                if (!lastLoaded.equals(mtime)) {
                    lastLoaded = mtime;
                    map = parseMapFile();
                }
            } catch (IOException e) {
                 throw new AuthenticationException("failed to read " + file + ": "
                         + Exceptions.messageOrClassName(e));
            }
        }

        return map;
    }

    private Map<PrincipalMatcher,Set<Principal>> parseMapFile() throws IOException
    {
        LOG.debug("Reading file {}", file);

        Map<PrincipalMatcher,Set<Principal>> map = new LinkedHashMap<>();

        int lineCount = 0;
        for (String line : Files.readAllLines(file)) {
            lineCount++;
            line = line.trim();
            if (!line.isEmpty() && line.charAt(0) != '#') {
                try {
                    Args args = new Args(line);
                    checkFormat(args.argc() > 0, "Missing predicate matcher");
                    checkFormat(args.argc() > 1, "Missing mapped principals");
                    String matcherDescription = args.argv(0);
                    List<String> mappedPrincipalDescriptions = args.getArguments().subList(1, args.argc());
                    map.put(asMatcher(matcherDescription),
                            asPrincipals(mappedPrincipalDescriptions));
                } catch (GplazmaParseMapFileException e) {
                    warningsConsumer.accept(file.getFileName() + ":" + lineCount + ": " + e.getMessage());
                }
            }
        }

        return map;
    }

    private static PrincipalMatcher asMatcher(String description)
            throws GplazmaParseMapFileException
    {
        List<String> parts = Splitter.on(':').limit(2).splitToList(description);
        checkFormat(parts.size() == 2, "Missing ':' in \"%s\"", description);

        String type = parts.get(0);
        MappablePrincipal p = LABEL_TO_PRINCIPAL.get(type);
        checkFormat(p != null, "Unknown principal type \"%s\" in \"%s\"", type,
                description);
        return p.buildMatcher(parts.get(1));
    }

    private static Set<Principal> asPrincipals(List<String> descriptions)
            throws GplazmaParseMapFileException
    {
        List<String> problems = new ArrayList<>();
        Set<Principal> principals = new HashSet<>();

        for (String description : descriptions) {
            try {
                List<String> parts = Splitter.on(':').limit(2).splitToList(description);
                checkFormat(parts.size() == 2, "Missing ':' in \"%s\"", description);

                String type = parts.get(0);
                MappablePrincipal p = LABEL_TO_PRINCIPAL.get(type);
                checkFormat(p != null, "Unknown principal type \"%s\" in \"%s\"",
                        type, description);

                principals.add(p.buildPrincipal(parts.get(1)));
            } catch (GplazmaParseMapFileException e) {
                problems.add(e.getMessage());
            }
        }

        if (!problems.isEmpty()) {
            String problem = problems.stream().collect(Collectors.joining(", "));
            throw new GplazmaParseMapFileException(problem);
        }

        return principals;
    }
}
