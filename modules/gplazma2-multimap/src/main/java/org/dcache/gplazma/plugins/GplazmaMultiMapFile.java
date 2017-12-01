package org.dcache.gplazma.plugins;

import com.google.common.base.Splitter;
import org.globus.gsi.gssapi.jaas.GlobusPrincipal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.kerberos.KerberosPrincipal;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.security.Principal;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.dcache.auth.EmailAddressPrincipal;
import org.dcache.auth.FQANPrincipal;
import org.dcache.auth.GidPrincipal;
import org.dcache.auth.GroupNamePrincipal;
import org.dcache.auth.OidcSubjectPrincipal;
import org.dcache.auth.OpenIdGroupPrincipal;
import org.dcache.auth.UidPrincipal;
import org.dcache.auth.UserNamePrincipal;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.plugins.exceptions.GplazmaParseMapFileException;
import org.dcache.util.Args;

import static org.dcache.gplazma.plugins.exceptions.GplazmaParseMapFileException.checkFormat;

public class GplazmaMultiMapFile
{
    private static final Logger LOG = LoggerFactory.getLogger(GplazmaMultiMapFile.class);

    private File file;
    private long lastLoaded;
    private Map<Principal,Set<Principal>> map = Collections.emptyMap();

    public GplazmaMultiMapFile(String path)
    {
        this(new File(path));
    }

    public GplazmaMultiMapFile(File file)
    {
        this.file = file;
    }

    public synchronized void ensureUpToDate() throws AuthenticationException
    {
        if (lastLoaded <= file.lastModified()) {
             LOG.debug("Reading file {}", file);
             try {
                BufferedReader reader = new BufferedReader(new FileReader(file));
                map = parseMapFile(reader);
                lastLoaded = System.currentTimeMillis();
            } catch (IOException e) {
                 throw new AuthenticationException(
                         String.format("failed to read %s: %s", file.getName(), e.getMessage()));
            }
        }
    }

    private static Map<Principal,Set<Principal>> parseMapFile(BufferedReader reader) throws IOException
    {
        Map<Principal,Set<Principal>> map = new HashMap<>();
        String line;
        String lineOrig;
        int lineCount = 0;

        while ((line = reader.readLine()) != null) {
            lineOrig = line = line.trim();
            lineCount++;
            if (line.isEmpty() || line.charAt(0) == '#') continue;

            try {
                List<Principal> principals = parse(line);

                if (!principals.isEmpty()) {
                    Iterator<Principal> iterator = principals.iterator();
                    Set<Principal> mapSet = new LinkedHashSet<>();

                    Principal key = iterator.next();
                    while (iterator.hasNext()) {
                        mapSet.add(iterator.next());
                    }

                    if (!mapSet.isEmpty()) {
                        if (!map.containsKey(key)) {
                            map.put(key, mapSet);
                        } else {
                            LOG.warn("{}: Ignored Additional Mapping for key {}",
                                        lineCount, lineOrig, key);
                        }
                    } else {
                        LOG.warn("{}: Empty Map", lineCount, lineOrig);
                    }
                }
            } catch (GplazmaParseMapFileException e) {
                LOG.warn("{}: {}", lineCount, e.getMessage());
            }
        }
        return map;
    }

    private static Principal createPrincipal(String predicate, String principal) throws GplazmaParseMapFileException {
        try {
            switch (predicate) {
                case "oidc":
                    return new OidcSubjectPrincipal(principal);
                case "oidcgrp":
                    return new OpenIdGroupPrincipal(principal);
                case "email":
                    return new EmailAddressPrincipal(principal);
                case "username":
                    return new UserNamePrincipal(principal);
                case "group":
                    return new GroupNamePrincipal(principal);
                case "dn":
                    return new GlobusPrincipal(principal);
                case "kerberos":
                    return new KerberosPrincipal(principal);
                case "uid":
                    return new UidPrincipal(principal);
                case "gid":
                    return createGidPrincipal(principal);
                case "fqan":
                    return createFqanPrincipal(principal);
                default:
                    throw new GplazmaParseMapFileException("Not supported predicate [" + predicate + "]");
            }
        } catch (IllegalArgumentException e) {
            throw new GplazmaParseMapFileException("Illegal Value [" +
                                                    principal +
                                                    "] to predicate [" +
                                                    predicate +
                                                    "]");
        }

    }

    private static GidPrincipal createGidPrincipal(String description) throws GplazmaParseMapFileException
    {
        checkFormat(countCommas(description) <= 1, "Illegal Value [%s] for gid", description);

        List<String> parts = Splitter.on(',').splitToList(description);
        boolean isPrimary = parts.size() == 2 ? Boolean.parseBoolean(parts.get(1)) : false;
        return new GidPrincipal(parts.get(0), isPrimary);
    }

    private static FQANPrincipal createFqanPrincipal(String description) throws GplazmaParseMapFileException
    {
        checkFormat(countCommas(description) <= 1, "Illegal Value [%s] for FQAN", description);

        List<String> parts = Splitter.on(',').splitToList(description);
        boolean isPrimary = parts.size() == 2 ? Boolean.parseBoolean(parts.get(1)) : false;
        return new FQANPrincipal(parts.get(0), isPrimary);
    }

    private static long countCommas(String principal) {
        return principal.chars().filter(c -> c == ',').count();
    }

    private static List<Principal> parse(CharSequence line) throws GplazmaParseMapFileException
    {
        final List<Principal> principals = new LinkedList<>();
        Args args = new Args(line);
        for (int i = 0; i < args.argc(); i++) {
            String argument = args.argv(i);
            int colon = argument.indexOf(':');
            if (colon != -1) {
                principals.add(createPrincipal(argument.substring(0, colon),
                                               argument.substring(colon + 1, argument.length())));
            } else {
                throw new GplazmaParseMapFileException("Missing colon in \"" + argument+"\"");
            }
        }
        return principals;
    }

    public synchronized Set<Principal> getMappedPrincipals(Principal principal)
    {
        Principal key;

        // Always match against non-primary values
        if (principal instanceof GidPrincipal) {
            key = ((GidPrincipal)principal).withPrimaryGroup(false);
        } else if (principal instanceof GroupNamePrincipal) {
            key = ((GroupNamePrincipal)principal).withPrimaryGroup(false);
        } else if (principal instanceof FQANPrincipal) {
            key = ((FQANPrincipal)principal).withPrimaryGroup(false);
        } else {
            key = principal;
        }

        Set<Principal> out = map.get(key);
        return (out == null) ? Collections.emptySet(): out;
    }
}
