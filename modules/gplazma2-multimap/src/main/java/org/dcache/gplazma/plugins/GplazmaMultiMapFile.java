package org.dcache.gplazma.plugins;

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
import org.dcache.auth.GidPrincipal;
import org.dcache.auth.OidcSubjectPrincipal;
import org.dcache.auth.UidPrincipal;
import org.dcache.auth.UserNamePrincipal;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.plugins.exceptions.GplazmaParseMapFileException;
import org.dcache.util.Args;

public class GplazmaMultiMapFile
{
    private static final Logger LOG = LoggerFactory.getLogger(GplazmaMultiMapFile.class);

    private File file;
    private long lastLoaded;
    private Map<Principal,Set<Principal>> map = Collections.emptyMap();
    private static final String[] principalTypes = new String[]{"dn",
                                                                "email",
                                                                "username",
                                                                "kerberos",
                                                                "oidc",
                                                                "uid",
                                                                "gid" };

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
                case "email":
                    return new EmailAddressPrincipal(principal);
                case "username":
                    return new UserNamePrincipal(principal);
                case "dn":
                    return new GlobusPrincipal(principal);
                case "kerberos":
                    return new KerberosPrincipal(principal);
                case "uid":
                    return new UidPrincipal(principal);
                case "gid":
                    return createGidPrincipal(principal);
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

    private static Principal createGidPrincipal(String principal) throws GplazmaParseMapFileException
    {
        if (countNumCommas(principal) > 1) {
                throw new GplazmaParseMapFileException("Illegal Value [" + principal +"] for gid");
        } else {
            String[] splits = principal.split(",");
            if (splits.length == 2) {
                return new GidPrincipal(splits[0], Boolean.parseBoolean(splits[1]));
            } else {
                return new GidPrincipal(principal, false);
            }
        }
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

    private static int countNumCommas(String principal) {
        return principal.length() - principal.replace(",", "").length();
    }

    public synchronized Set<Principal> getMappedPrincipals(Principal principal)
    {
        Set<Principal> out = map.get(principal);
        return (out == null) ? Collections.emptySet(): out;
    }
}
