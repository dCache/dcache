package org.dcache.gplazma.plugins;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.function.Predicate.not;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.Principal;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.dcache.auth.Subjects;
import org.dcache.gplazma.AuthenticationException;

/**
 * The banfile plug-in bans users by their principal class and the associated name. It is configured
 * via a simple plain text file.
 * <pre>
 *   # Ban users by principal
 *   alias dn=org.globus.gsi.gssapi.jaas.GlobusPrincipal
 *   alias kerberos=javax.security.auth.kerberos.KerberosPrincipal
 *   alias fqan=org.dcache.auth.FQANPrincipal
 *   alias name=org.dcache.auth.LoginNamePrincipal
 *
 *   ban name:ernie
 *   ban kerberos:BERT@EXAMPLE.COM
 *   ban com.example.SomePrincipal:Samson
 * </pre>
 */
public class BanFilePlugin implements GPlazmaAccountPlugin {

    private final static Pattern BAN_PATTERN = Pattern.compile("^ban\\s+([^:]+):(.*)$");
    private final static Pattern ALIAS_PATTERN = Pattern.compile("^alias\\s+([^:]+)=(.*)$");

    static final String BAN_FILE = "gplazma.banfile.path";
    private final Path configFile;

    private Set<Principal> bannedPrincipals;
    private Instant lastFileRead;

    public BanFilePlugin(Properties properties) {

        checkArgument(properties != null, "property is null");
        String configFileName = properties.getProperty(BAN_FILE);
        checkArgument(configFileName != null, BAN_FILE + " property is not set");

        configFile = Path.of(configFileName);
        checkState(Files.exists(configFile), "config file doesn't exist or not a file");

    }

    List<String> loadConfigLines() throws IOException {
        return Files.readAllLines(configFile, StandardCharsets.UTF_8);
    }

    /**
     * Create a list of principals from the source file. The empty lines and comments, i.e., lines
     * starting with '#' are ignored. It expects the file to be of the format: alias <alias>=<full
     * qualified classname> ban <full qualified classname or alias>:<principal string> e.g., alias
     * username=org.dcache.auth.UserNamePrincipal ban username:Someuser or ban
     * org.dcache.auth.UserNamePrincipal:Someuser
     *
     * @return a set of banned principals
     */
    private synchronized Set<Principal> loadConfigIfNeeded() throws AuthenticationException {
        try {

            if (bannedPrincipals == null ||
                  lastFileRead.isBefore(
                        Files.readAttributes(configFile, BasicFileAttributes.class)
                              .lastModifiedTime().toInstant())) {

                // alias -> value, like uid=org.dcache.auth.UidPrincipal
                Map<String, String> aliases = new HashMap<>();

                // class/alias -> value, like uid:123 or org.dcache.auth.UidPrincipal:123
                Map<String, String> bans = new HashMap<>();

                // group all 'alias' and all 'ban' records, skip comments and empty lines
                Map<String, List<String>> config = loadConfigLines().stream()
                      .map(String::strip)
                      .filter(not(String::isEmpty))
                      .filter(l -> l.charAt(0) != '#')
                      .collect(Collectors.groupingBy(l -> l.split("\\s")[0]));

                // process aliases  as they might be used in the next step
                List<String> configuredAliases = config.remove("alias");
                if (configuredAliases != null) {
                    configuredAliases.forEach(a -> {
                        Matcher m = ALIAS_PATTERN.matcher(a);
                        if (!m.matches()) {
                            throw new IllegalArgumentException("Bad alias line format: '" + a
                                  + "', expected 'alias <alias>=<class>'");
                        }
                        String alias = m.group(1);
                        String clazz = m.group(2);
                        aliases.put(alias, clazz);
                    });
                }

                // process ban records. substitute aliases, if needed
                List<String> configuredBans = config.remove("ban");
                if (configuredBans != null) {
                    configuredBans.forEach(a -> {
                        Matcher m = BAN_PATTERN.matcher(a);
                        if (!m.matches()) {
                            throw new IllegalArgumentException("Bad ban line format: '" + a
                                  + "', expected 'ban <classOrAlias>:<value>'");
                        }
                        String clazz = m.group(1);
                        String value = m.group(2);
                        bans.put(aliases.getOrDefault(clazz, clazz), value);
                    });
                }

                // any other key is an error
                if (!config.isEmpty()) {
                    String badLines = config.values().stream().flatMap(List::stream)
                          .collect(Collectors.joining(",", "[", "]"));
                    throw new IllegalArgumentException("Line has bad format: '" + badLines
                          + "', expected '[alias|ban] <key>:<value>'");
                }

                // construct lines suitable for Subjects.principalsFromArgs
                // class:value or shortname:value
                List<String> bannedNames = bans.entrySet().stream()
                      .map(e -> e.getKey() + ":" + e.getValue())
                      .collect(Collectors.toList());

                bannedPrincipals = Subjects.principalsFromArgs(bannedNames);
                lastFileRead = Instant.now();
            }
        } catch (IOException e) {
            throw new AuthenticationException(e);
        }

        return bannedPrincipals;
    }


    /**
     * Check if any of the principals in authorizedPrincipals is blacklisted in the file specified
     * by the dCache property gplazma.banfile.uri.
     *
     * @param authorizedPrincipals principals associated with a user
     * @throws AuthenticationException indicating a banned user
     */
    @Override
    public void account(Set<Principal> authorizedPrincipals) throws AuthenticationException {
        if (!Collections.disjoint(authorizedPrincipals, loadConfigIfNeeded())) {
            throw new AuthenticationException("user banned");
        }
    }
}
