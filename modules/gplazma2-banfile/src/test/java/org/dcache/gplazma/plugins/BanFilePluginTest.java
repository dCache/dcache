package org.dcache.gplazma.plugins;

import org.dcache.auth.LoginNamePrincipal;
import org.dcache.auth.UserNamePrincipal;
import org.dcache.gplazma.AuthenticationException;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class BanFilePluginTest {

    private BanFilePlugin plugin;
    private Path configFile;
    private AtomicInteger reloadCount = new AtomicInteger();

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailWithNoProperties() {
        new BanFilePlugin(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailWithMissingConfigProperty() {
        new BanFilePlugin(new Properties());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailWithMissingConfigFile() {
        Properties properties = new Properties();
        properties.put(BanFilePlugin.BAN_FILE, "/some/missing/file");
        new BanFilePlugin(new Properties());
    }

    @Test
    public void shouldAllowNotBannedPrincipal() throws IOException, AuthenticationException {
        givenConfig("ban org.dcache.auth.UserNamePrincipal:bert");
        plugin.account(Set.of(new UserNamePrincipal("ernie")));
    }

    @Test
    public void shouldAllowSameNameDifferentClasses() throws IOException, AuthenticationException {
        givenConfig("ban org.dcache.auth.LoginNamePrincipal:bert");
        plugin.account(Set.of(new UserNamePrincipal("bert")));
    }

    @Test(expected = AuthenticationException.class)
    public void shouldFailForBannedUser() throws IOException, AuthenticationException {
        givenConfig("ban org.dcache.auth.UserNamePrincipal:bert");
        plugin.account(Set.of(new UserNamePrincipal("bert")));
    }

    @Test(expected = AuthenticationException.class)
    public void shouldFailIfAnyPrincipalIsBanned() throws IOException, AuthenticationException {
        givenConfig("ban org.dcache.auth.UserNamePrincipal:bert");
        plugin.account(Set.of(new LoginNamePrincipal("ernie"), new UserNamePrincipal("bert")));
    }

    @Test
    public void shouldWorkMultipleInvocations() throws IOException, AuthenticationException {
        givenConfig("ban org.dcache.auth.UserNamePrincipal:bert");
        try {
            plugin.account(Set.of(new UserNamePrincipal("bert")));
            fail();
        } catch (AuthenticationException expected) {
        }
        plugin.account(Set.of(new UserNamePrincipal("ernie")));

        try {
            plugin.account(Set.of(new UserNamePrincipal("bert")));
            fail();
        } catch (AuthenticationException expected) {
        }
        plugin.account(Set.of(new UserNamePrincipal("ernie")));
    }

    @Test(expected = AuthenticationException.class)
    public void shouldFailForBannedUserByAlias() throws IOException, AuthenticationException {
        givenConfig("alias foo=org.dcache.auth.UserNamePrincipal\nban foo:bert");
        plugin.account(Set.of(new UserNamePrincipal("bert")));
    }

    @Test(expected = AuthenticationException.class)
    public void shouldFailForBannedUserByStandardAlias() throws IOException, AuthenticationException {
        givenConfig("ban name:bert");
        plugin.account(Set.of(new LoginNamePrincipal("bert")));
    }

    @Test
    public void shouldIgnoreComments() throws IOException, AuthenticationException {
        givenConfig("# ban name:bert");
        plugin.account(Set.of(new LoginNamePrincipal("bert")));
    }

    @Test(expected = AuthenticationException.class)
    public void shouldAcceptFileWithComments() throws IOException, AuthenticationException {
        givenConfig("# this is a ban file\nban name:bert");
        plugin.account(Set.of(new LoginNamePrincipal("bert")));
    }

    @Test(expected = AuthenticationException.class)
    public void shouldFailForBannedUserByAliasAndBlanks() throws IOException, AuthenticationException {
        givenConfig("alias foo=org.dcache.auth.UserNamePrincipal\n   \n  ban foo:bert  ");
        plugin.account(Set.of(new UserNamePrincipal("bert")));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailOnMalformedConfig() throws IOException, AuthenticationException {
        givenConfig("hello world!");
        plugin.account(Set.of(new UserNamePrincipal("bert")));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailOnMalformedBanLine() throws IOException, AuthenticationException {
        givenConfig("ban foo");
        plugin.account(Set.of(new UserNamePrincipal("bert")));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailOnMalformedAliasLine() throws IOException, AuthenticationException {
        givenConfig("alias foo");
        plugin.account(Set.of(new UserNamePrincipal("bert")));
    }

    @Test(expected = AuthenticationException.class)
    public void shouldBanAllIfConfigDeleted() throws IOException, AuthenticationException {
        givenConfig("");
        Files.deleteIfExists(configFile);
        plugin.account(Set.of(new UserNamePrincipal("bert")));
    }

    @Test
    public void shouldNotReReadConfigFileIfNotChanged() throws IOException, AuthenticationException, InterruptedException {
        givenConfig("ban org.dcache.auth.UserNamePrincipal:bert");
        plugin.account(Set.of(new UserNamePrincipal("ernie")));
        TimeUnit.SECONDS.sleep(1);
        plugin.account(Set.of(new UserNamePrincipal("ernie")));
        assertEquals("broken caching", 1, reloadCount.get());
    }

    @Test
    public void shouldReReadConfigFileIfChanged() throws IOException, AuthenticationException, InterruptedException {
        givenConfig("ban org.dcache.auth.UserNamePrincipal:bert");
        plugin.account(Set.of(new UserNamePrincipal("ernie")));

        TimeUnit.SECONDS.sleep(1);

        Files.writeString(configFile, "ban org.dcache.auth.LoginNamePrincipal:bert",
                StandardCharsets.UTF_8, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
        plugin.account(Set.of(new UserNamePrincipal("ernie")));
        assertEquals("broken caching", 2, reloadCount.get());
    }

    private void givenConfig(String config) throws IOException {
        configFile = Files.createTempFile("dcache.gplazma.ban", "conf");
        Files.writeString(configFile, config, StandardCharsets.UTF_8, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);

        Properties properties = new Properties();
        properties.put(BanFilePlugin.BAN_FILE, configFile.toString());
        plugin = new BanFilePlugin(properties) {
            @Override
            List<String> loadConfigLines() throws IOException {
                reloadCount.incrementAndGet();
                return super.loadConfigLines();
            }
        };
    }

    @After
    public void tearDown() throws IOException {
        if (configFile != null) {
            Files.deleteIfExists(configFile);
        }
    }
}
