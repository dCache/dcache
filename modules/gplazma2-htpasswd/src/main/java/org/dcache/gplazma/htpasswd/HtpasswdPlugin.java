package org.dcache.gplazma.htpasswd;

import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Reader;
import java.security.Principal;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.dcache.auth.PasswordCredential;
import org.dcache.auth.UserNamePrincipal;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.plugins.GPlazmaAuthenticationPlugin;

import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.getFirst;
import static org.dcache.gplazma.util.Preconditions.checkAuthentication;

public class HtpasswdPlugin implements GPlazmaAuthenticationPlugin
{
    private static final Logger LOGGER = LoggerFactory.getLogger(HtpasswdPlugin.class);

    private final Map<String, String> users = new HashMap<>();
    private final long refreshPeriod;
    private final MutableInputSupplier<? extends Reader> htpasswdSource;
    private long loadTime;

    public HtpasswdPlugin(Properties properties)
    {
        this(new FileMultableInputSupplier(new File(properties.getProperty("gplazma.htpasswd.file")), Charsets.US_ASCII),
                TimeUnit.valueOf(properties.getProperty("gplazma.htpasswd.file.cache-period.unit")).toMillis(
                        Long.parseLong(properties.getProperty("gplazma.htpasswd.file.cache-period"))));
    }

    public HtpasswdPlugin(MutableInputSupplier<? extends Reader> htpasswdSource, long refreshPeriod)
    {
        this.htpasswdSource = htpasswdSource;
        this.refreshPeriod = refreshPeriod;
    }

    private synchronized String getHash(String user) throws IOException
    {
        if (loadTime + refreshPeriod <= System.currentTimeMillis()) {
            try {
                long lastModified = htpasswdSource.lastModified();
                if (lastModified == 0 || loadTime <= lastModified) {
                    users.clear();
                    for (String line : CharStreams.readLines(htpasswdSource)) {
                        String[] elements = line.split(":", 2);
                        users.put(elements[0], elements[1].trim());
                    }
                }
                loadTime = System.currentTimeMillis();
            } catch (FileNotFoundException e) {
                LOGGER.warn("htpasswd file not found");
            }
        }
        return users.get(user);
    }

    @Override
    public void authenticate(Set<Object> publicCredentials, Set<Object> privateCredentials,
                             Set<Principal> identifiedPrincipals) throws AuthenticationException
    {
        try {
            PasswordCredential credential =
                    getFirst(filter(privateCredentials, PasswordCredential.class), null);
            checkAuthentication(credential != null, "no username and password");
            String name = credential.getUsername();
            String hash = getHash(name);
            checkAuthentication(hash != null, name + " is unknown");
            checkAuthentication(MD5Crypt.verifyPassword(credential.getPassword(), hash), "wrong password");
            identifiedPrincipals.add(new UserNamePrincipal(name));
        } catch (IOException e) {
            throw new AuthenticationException("Authentication failed due to I/O error: " + e.getMessage(), e);
        }
    }
}
