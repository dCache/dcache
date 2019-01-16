package org.dcache.gplazma.htpasswd;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.security.Principal;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.dcache.auth.PasswordCredential;
import org.dcache.auth.UserNamePrincipal;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.plugins.GPlazmaAuthenticationPlugin;

import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.getFirst;
import static java.util.stream.Collectors.*;
import static org.dcache.gplazma.util.Preconditions.checkAuthentication;
import static org.dcache.util.TimeUtils.getMillis;

public class HtpasswdPlugin implements GPlazmaAuthenticationPlugin
{
    private static final Logger LOGGER = LoggerFactory.getLogger(HtpasswdPlugin.class);

    private final Supplier<Stream<String>> htpasswdFile;
    private Map<String, String> users = Collections.emptyMap();

    public HtpasswdPlugin(Properties properties)
    {
        this(new FileSupplier(
                Paths.get(properties.getProperty("gplazma.htpasswd.file")),
                getMillis(properties, "gplazma.htpasswd.file.cache-period"),
                StandardCharsets.US_ASCII));
    }

    public HtpasswdPlugin(Supplier<Stream<String>> htpasswdFile)
    {
        this.htpasswdFile = htpasswdFile;
    }

    private synchronized String getHash(String user) throws IOException
    {
        try (Stream<String> stream = htpasswdFile.get()) {
            if (stream != null) {
                users = stream.map(s -> s.split(":", 2)).collect(toMap(e -> e[0], e -> e[1].trim(), (a, b) -> b));
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

    private static class FileSupplier implements Supplier<Stream<String>>
    {
        private final long refreshPeriod;
        private final Path file;
        private final Charset charset;
        private long lastCheckedAt;

        public FileSupplier(Path file, long refreshPeriod, Charset charset)
        {
            this.refreshPeriod = refreshPeriod;
            this.file = file;
            this.charset = charset;
        }

        @Override
        public Stream<String> get()
        {
            Stream<String> lines = null;
            long now = System.currentTimeMillis();
            if (lastCheckedAt + refreshPeriod <= now) {
                try {
                    FileTime lastModified = Files.getLastModifiedTime(file);
                    if (lastCheckedAt <= lastModified.toMillis()) {
                        lines = Files.lines(file, charset);
                    }
                    lastCheckedAt = now;
                } catch (IOException e) {
                    LOGGER.warn("{} cannot be opened: {}", file, e.getMessage());
                }
            }
            return lines;
        }
    }
}
