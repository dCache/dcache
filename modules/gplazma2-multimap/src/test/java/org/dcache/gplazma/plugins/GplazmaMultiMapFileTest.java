package org.dcache.gplazma.plugins;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import javax.security.auth.kerberos.KerberosPrincipal;
import org.dcache.auth.EmailAddressPrincipal;
import org.dcache.auth.FQANPrincipal;
import org.dcache.auth.GidPrincipal;
import org.dcache.auth.GroupNamePrincipal;
import org.dcache.auth.OAuthProviderPrincipal;
import org.dcache.auth.OidcSubjectPrincipal;
import org.dcache.auth.OpenIdGroupPrincipal;
import org.dcache.auth.UidPrincipal;
import org.dcache.auth.UserNamePrincipal;
import org.globus.gsi.gssapi.jaas.GlobusPrincipal;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import org.junit.Before;
import org.junit.Test;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import static java.util.Objects.requireNonNull;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.slf4j.LoggerFactory;

public class GplazmaMultiMapFileTest {

    private GplazmaMultiMapFile mapFile;
    private Set<Principal> mappedPrincipals;
    private Path config;
    private List<String> warnings;
    private List<ILoggingEvent> logEvents;

    @Before
    public void setup() throws Exception {
        FileSystem fileSystem = Jimfs.newFileSystem(Configuration.unix());
        config = fileSystem.getPath("/etc/dcache/multimap.conf");
        Files.createDirectories(config.getParent());
        mapFile = new GplazmaMultiMapFile(config);
        warnings = new ArrayList<>();
        mapFile.setWarningConsumer(warnings::add);

        // Capture logging activity
        Logger logger = (Logger) LoggerFactory.getLogger(GplazmaMultiMapFile.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        logEvents = appender.list;
    }

    @Test
    public void shouldFailWhenWrongMapFormatDN() throws Exception {
        givenConfig("dn:kermit@dcache.org    username:kermit");

        whenMapping(new GlobusPrincipal("\"dn:/C=DE/S=Hamburg/OU=desy.de/CN=Kermit The Frog\""));

        // REVISIT should warn of invalid DN syntax
        assertThat(mappedPrincipals, is(empty()));
    }

    @Test
    public void shouldFailWhenWrongMapFormatDN1() throws Exception {
        givenConfig("dn:/C=DE/S=Hamburg/OU=desy.de/CN=Kermit The Frog    username:kermit");

        whenMapping(new GlobusPrincipal("dn:/C=DE/S=Hamburg/OU=desy.de/CN=Kermit The Frog"));

        assertThat(warnings, is(not(empty())));
        assertThat(mappedPrincipals, is(empty()));
    }

    public void shouldFailWhenWrongMapFormatKerberos() throws Exception {
        givenConfig("krb:kermit@DESY.DE    username:kermit");

        whenMapping(new KerberosPrincipal("kermit@DESY.DE"));

        assertThat(warnings, is(not(empty())));
        assertThat(mappedPrincipals, is(empty()));
    }

    @Test
    public void shouldFailWhenWrongMapFormatOidc() throws Exception {
        givenConfig("oid:googleopenidsubject    username:kermit");

        whenMapping(new OidcSubjectPrincipal("googleopenidsubject", "GOOGLE"));

        assertThat(warnings, is(not(empty())));
        assertThat(mappedPrincipals, is(empty()));
    }

    @Test
    public void shouldFailWhenWrongMapFormatEmail() throws Exception {
        givenConfig("mail:kermit@dcache.org    username:kermit");

        whenMapping(new EmailAddressPrincipal("kermit@dcache.org"));

        assertThat(warnings, is(not(empty())));
        assertThat(mappedPrincipals, is(empty()));
    }

    @Test
    public void shouldFailWhenWrongMapFormatEmail2() throws Exception {
        givenConfig("email:kermit.dcache.org    username:kermit");

        whenMapping(new EmailAddressPrincipal("kermit@dcache.org"));

        assertThat(warnings, is(not(empty())));
        assertThat(mappedPrincipals, is(empty()));
    }

    @Test
    public void shouldPassWhenEmailMapped() throws Exception {
        givenConfig("email:kermit@dcache.org    username:kermit");

        whenMapping(new EmailAddressPrincipal("kermit@dcache.org"));

        assertThat(warnings, is(empty()));
        assertThat(mappedPrincipals, hasItem(new UserNamePrincipal("kermit")));
    }

    @Test
    public void shouldPassWhenEmailMapped1() throws Exception {
        givenConfig("\"email:kermit@dcache.org\"    username:kermit");

        whenMapping(new EmailAddressPrincipal("kermit@dcache.org"));

        assertThat(warnings, is(empty()));
        assertThat(mappedPrincipals, hasItem(new UserNamePrincipal("kermit")));
    }

    @Test
    public void shouldPassWhenEmailMapped2() throws Exception {
        givenConfig("\"email:kermit@dcache.org\"    \"username:kermit\"");

        whenMapping(new EmailAddressPrincipal("kermit@dcache.org"));

        assertThat(warnings, is(empty()));
        assertThat(mappedPrincipals, hasItem(new UserNamePrincipal("kermit")));
    }

    @Test
    public void shouldPassWhenDNMapped() throws Exception {
        givenConfig("\"dn:/C=DE/O=Hamburg/OU=desy.de/CN=Kermit The Frog\"    username:kermit");

        whenMapping(new GlobusPrincipal("/C=DE/O=Hamburg/OU=desy.de/CN=Kermit The Frog"));

        assertThat(warnings, is(empty()));
        assertThat(mappedPrincipals, hasItem(new UserNamePrincipal("kermit")));
    }

    @Test
    public void shouldPassWhenOidcMappedWithoutOP() throws Exception {
        givenConfig("oidc:googleoidcsubject    username:kermit");

        whenMapping(new OidcSubjectPrincipal("googleoidcsubject", "GOOGLE"));

        assertThat(warnings, is(empty()));
        assertThat(mappedPrincipals, hasItem(new UserNamePrincipal("kermit")));
    }

    @Test
    public void shouldPassWhenOidcMappedWithOP() throws Exception {
        givenConfig("oidc:googleoidcsubject@GOOGLE    username:kermit");

        whenMapping(new OidcSubjectPrincipal("googleoidcsubject", "GOOGLE"));

        assertThat(warnings, is(empty()));
        assertThat(mappedPrincipals, hasItem(new UserNamePrincipal("kermit")));
    }

    @Test
    public void shouldPassWhenOidcWithAtMappedWithoutOP() throws Exception {
        givenConfig("oidc:sub-claim@test    username:kermit");

        whenMapping(new OidcSubjectPrincipal("sub-claim@test", "OP"));

        assertThat(warnings, is(empty()));
        assertThat(mappedPrincipals, hasItem(new UserNamePrincipal("kermit")));
    }

    @Test
    public void shouldPassWhenOidcWithAtMappedWithOP() throws Exception {
        givenConfig("oidc:sub-claim@test@OP    username:kermit");

        whenMapping(new OidcSubjectPrincipal("sub-claim@test", "OP"));

        assertThat(warnings, is(empty()));
        assertThat(mappedPrincipals, hasItem(new UserNamePrincipal("kermit")));
    }

    @Test
    public void shouldIgnoreMappingWithWrongOP() throws Exception {
        givenConfig("oidc:googleoidcsubject@GOOGLE    username:kermit");

        whenMapping(new OidcSubjectPrincipal("googleoidcsubject", "NOT-GOOGLE"));

        assertThat(warnings, is(empty()));
        assertThat(mappedPrincipals, is(empty()));
    }

    @Test
    public void shouldPassWhenOpenIdGroupMapped() throws Exception {
        givenConfig("oidcgrp:Users    group:desy");

        whenMapping(new OpenIdGroupPrincipal("Users"));

        assertThat(warnings, is(empty()));
        assertThat(mappedPrincipals, hasItem(new GroupNamePrincipal("desy")));
    }

    @Test
    public void shouldPassWhenUidMappedFromOidcWithoutOP() throws Exception {
        givenConfig("oidc:googleoidcsubject    uid:1000");

        whenMapping(new OidcSubjectPrincipal("googleoidcsubject", "GOOGLE"));

        assertThat(warnings, is(empty()));
        assertThat(mappedPrincipals, hasItem(new UidPrincipal("1000")));
    }

    @Test
    public void shouldPassWhenUidMappedFromOidcWithOP() throws Exception {
        givenConfig("oidc:googleoidcsubject@GOOGLE    uid:1000");

        whenMapping(new OidcSubjectPrincipal("googleoidcsubject", "GOOGLE"));

        assertThat(warnings, is(empty()));
        assertThat(mappedPrincipals, hasItem(new UidPrincipal("1000")));
    }

    @Test
    public void shouldPassWhenUidPrimaryGidTrueMappedFromOidcWithoutOP() throws Exception {
        givenConfig("oidc:googleoidcsubject   gid:1000,true  uid:1000  ");

        whenMapping(new OidcSubjectPrincipal("googleoidcsubject", "GOOGLE"));

        assertThat(warnings, is(empty()));
        assertThat(mappedPrincipals, hasItem(new UidPrincipal("1000")));
        assertThat(mappedPrincipals, hasItem(new GidPrincipal("1000", true)));
    }

    @Test
    public void shouldPassWhenUidPrimaryGidTrueMappedFromOidcWithOP() throws Exception {
        givenConfig("oidc:googleoidcsubject@GOOGLE   gid:1000,true  uid:1000  ");

        whenMapping(new OidcSubjectPrincipal("googleoidcsubject", "GOOGLE"));

        assertThat(warnings, is(empty()));
        assertThat(mappedPrincipals, hasItem(new UidPrincipal("1000")));
        assertThat(mappedPrincipals, hasItem(new GidPrincipal("1000", true)));
    }

    @Test
    public void shouldPassWhenUidPrimaryGidFalseMappedFromOidcWithoutOP() throws Exception {
        givenConfig("oidc:googleoidcsubject   gid:1000,false  uid:1000  ");

        whenMapping(new OidcSubjectPrincipal("googleoidcsubject", "GOOGLE"));

        assertThat(warnings, is(empty()));
        assertThat(mappedPrincipals, hasItem(new UidPrincipal("1000")));
        assertThat(mappedPrincipals, hasItem(new GidPrincipal("1000", false)));
    }

    @Test
    public void shouldPassWhenUidPrimaryGidFalseMappedFromOidcWithOP() throws Exception {
        givenConfig("oidc:googleoidcsubject@GOOGLE   gid:1000,false  uid:1000  ");

        whenMapping(new OidcSubjectPrincipal("googleoidcsubject", "GOOGLE"));

        assertThat(warnings, is(empty()));
        assertThat(mappedPrincipals, hasItem(new UidPrincipal("1000")));
        assertThat(mappedPrincipals, hasItem(new GidPrincipal("1000", false)));
    }

    @Test
    public void shouldPassWhenUidGidMappedFromOidcWithoutOP() throws Exception {
        givenConfig("oidc:googleoidcsubject   gid:1000  uid:1000  ");

        whenMapping(new OidcSubjectPrincipal("googleoidcsubject", "GOOGLE"));

        assertThat(warnings, is(empty()));
        assertThat(mappedPrincipals, hasItem(new UidPrincipal("1000")));
        assertThat(mappedPrincipals, hasItem(new GidPrincipal("1000", false)));
    }

    @Test
    public void shouldPassWhenUidGidMappedFromOidcWithOP() throws Exception {
        givenConfig("oidc:googleoidcsubject@GOOGLE   gid:1000  uid:1000  ");

        whenMapping(new OidcSubjectPrincipal("googleoidcsubject", "GOOGLE"));

        assertThat(warnings, is(empty()));
        assertThat(mappedPrincipals, hasItem(new UidPrincipal("1000")));
        assertThat(mappedPrincipals, hasItem(new GidPrincipal("1000", false)));
    }

    @Test
    public void shouldFailWhenGidFormatWrongFromOidcWithoutOP() throws Exception {
        givenConfig("oidc:googleoidcsubject   gid:1000,,true  uid:1000  ");

        whenMapping(new OidcSubjectPrincipal("googleoidcsubject", "GOOGLE"));

        assertThat(warnings, is(not(empty())));
        assertThat(mappedPrincipals, is(empty()));
    }

    @Test
    public void shouldFailWhenGidFormatWrongFromOidcWithOP() throws Exception {
        givenConfig("oidc:googleoidcsubject@GOOGLE   gid:1000,,true  uid:1000  ");

        whenMapping(new OidcSubjectPrincipal("googleoidcsubject", "GOOGLE"));

        assertThat(warnings, is(not(empty())));
        assertThat(mappedPrincipals, is(empty()));
    }

    @Test
    public void shouldFailWhenGidFormatWrong2FromOidcWithoutOP() throws Exception {
        givenConfig("oidc:googleoidcsubject   gid:1000,true,  uid:1000  ");

        whenMapping(new OidcSubjectPrincipal("googleoidcsubject", "GOOGLE"));

        assertThat(warnings, is(not(empty())));
        assertThat(mappedPrincipals, is(empty()));
    }

    @Test
    public void shouldFailWhenGidFormatWrong2FromOidcWithOP() throws Exception {
        givenConfig("oidc:googleoidcsubject@GOOGLE   gid:1000,true,  uid:1000  ");

        whenMapping(new OidcSubjectPrincipal("googleoidcsubject", "GOOGLE"));

        assertThat(warnings, is(not(empty())));
        assertThat(mappedPrincipals, is(empty()));
    }

    @Test
    public void shouldPassWhenUidGidMapped2FromOidcWithoutOP() throws Exception {
        givenConfig("oidc:googleoidcsubject   gid:1000,true  gid:2000 uid:1000  uid:2000");

        whenMapping(new OidcSubjectPrincipal("googleoidcsubject", "GOOGLE"));

        assertThat(warnings, is(empty()));
        assertThat(mappedPrincipals, hasItem(new UidPrincipal("2000")));
        assertThat(mappedPrincipals, hasItem(new UidPrincipal("1000")));
        assertThat(mappedPrincipals, hasItem(new GidPrincipal("2000", false)));
        assertThat(mappedPrincipals, hasItem(new GidPrincipal("1000", true)));
    }

    @Test
    public void shouldPassWhenUidGidMapped2FromOidcWithOP() throws Exception {
        givenConfig("oidc:googleoidcsubject@GOOGLE   gid:1000,true  gid:2000 uid:1000  uid:2000");

        whenMapping(new OidcSubjectPrincipal("googleoidcsubject", "GOOGLE"));

        assertThat(warnings, is(empty()));
        assertThat(mappedPrincipals, hasItem(new UidPrincipal("2000")));
        assertThat(mappedPrincipals, hasItem(new UidPrincipal("1000")));
        assertThat(mappedPrincipals, hasItem(new GidPrincipal("2000", false)));
        assertThat(mappedPrincipals, hasItem(new GidPrincipal("1000", true)));
    }

    @Test
    public void testRefresh() throws Exception {
        givenConfig("  \n");
        givenConfigHasBeenRead();
        givenConfig("email:kermit@dcache.org    username:kermit\n");

        whenMapping(new EmailAddressPrincipal("kermit@dcache.org"));

        assertThat(warnings, is(empty()));
        assertThat(mappedPrincipals, hasItem(new UserNamePrincipal("kermit")));
    }

    @Test
    public void shouldMatchNonPrimarySpecificGroupWithPrimaryGroup() throws Exception {
        givenConfig("group:test gid:1000,false");

        whenMapping(new GroupNamePrincipal("test", true));

        assertThat(warnings, is(empty()));
        assertThat(mappedPrincipals, hasItem(new GidPrincipal(1000, false)));
    }

    @Test
    public void shouldMatchNonPrimarySpecificGroupWithNonPrimaryGroup() throws Exception {
        givenConfig("group:test gid:1000,false");

        whenMapping(new GroupNamePrincipal("test", false));

        assertThat(warnings, is(empty()));
        assertThat(mappedPrincipals, hasItem(new GidPrincipal(1000, false)));
    }

    @Test
    public void shouldMatchNonPrimarySpecificFqanWithPrimaryFqan() throws Exception {
        givenConfig("fqan:/dcache gid:1000,false");

        whenMapping(new FQANPrincipal("/dcache", true));

        assertThat(warnings, is(empty()));
        assertThat(mappedPrincipals, hasItem(new GidPrincipal(1000, false)));
    }

    @Test
    public void shouldMatchNonPrimarySpecificFqanWithNonPrimaryFqan() throws Exception {
        givenConfig("fqan:/dcache gid:1000,false");

        whenMapping(new FQANPrincipal("/dcache", false));

        assertThat(warnings, is(empty()));
        assertThat(mappedPrincipals, hasItem(new GidPrincipal(1000, false)));
    }

    @Test
    public void shouldMatchNonPrimarySpecificGidWithPrimaryGid() throws Exception {
        givenConfig("gid:1000 uid:2000");

        whenMapping(new GidPrincipal(1000, true));

        assertThat(warnings, is(empty()));
        assertThat(mappedPrincipals, hasItem(new UidPrincipal(2000)));
    }

    @Test
    public void shouldMatchNonPrimarySpecificGidWithNonPrimaryGid() throws Exception {
        givenConfig("gid:1000 uid:2000");

        whenMapping(new GidPrincipal(1000, false));

        assertThat(warnings, is(empty()));
        assertThat(mappedPrincipals, hasItem(new UidPrincipal(2000)));
    }

    @Test
    public void shouldMatchOpWithSameName() throws Exception {
        givenConfig("op:FOO uid:1000 gid:2000");

        whenMapping(new OAuthProviderPrincipal("FOO", URI.create("https://my-op.example.org/")));

        assertThat(warnings, is(empty()));
        assertThat(mappedPrincipals, hasItem(new UidPrincipal(1000)));
        assertThat(mappedPrincipals, hasItem(new GidPrincipal(2000, false)));
    }

    @Test
    public void shouldNotMatchOpWithDifferentName() throws Exception {
        givenConfig("op:FOO uid:1000 gid:2000");

        whenMapping(new OAuthProviderPrincipal("BAR", URI.create("https://my-op.example.org/")));

        assertThat(warnings, is(empty()));
        assertThat(mappedPrincipals, is(empty()));
    }

    @Test
    public void shouldNotMatchOpWithDn() throws Exception {
        givenConfig("op:FOO uid:1000 gid:2000");

        whenMapping(new GlobusPrincipal("\"dn:/C=DE/S=Hamburg/OU=desy.de/CN=Kermit The Frog\""));

        assertThat(warnings, is(empty()));
        assertThat(mappedPrincipals, is(empty()));
    }


    @Test
    public void shouldAddOpWithoutIssuer() throws Exception {
        givenConfig("email:kermit@dcache.org  op:FOO");

        whenMapping(new EmailAddressPrincipal("kermit@dcache.org"));

        assertThat(logEvents, hasItem(new LogEventMatcher(Level.WARN,
                allOf(containsString("op:FOO"),
                containsString("'iss' claim value")))));

        var opPrincipals = mappedPrincipals.stream()
                .filter(OAuthProviderPrincipal.class::isInstance)
                .toList();
        // Avoid asserting the OP's placeholder issuer URL because we don't
        // guarantee that value.
        assertThat(opPrincipals.size(), equalTo(1));
        var opPrincipal = opPrincipals.get(0);
        assertThat(opPrincipal.getName(), is(equalTo("FOO")));
    }

    @Test
    public void shouldAddOpWithIssuer() throws Exception {
        givenConfig("email:kermit@dcache.org  op:FOO:https://my-op.example.org/");

        whenMapping(new EmailAddressPrincipal("kermit@dcache.org"));

        assertThat(warnings, is(empty()));
        assertThat(logEvents, not(hasItem(new LogEventMatcher(Level.WARN,
                containsString("op:FOO")))));
        assertThat(mappedPrincipals, hasItem(new OAuthProviderPrincipal("FOO",
                URI.create("https://my-op.example.org/"))));
    }

    /*----------------------- Helpers -----------------------------*/

    private void givenConfig(String mapping) throws Exception {
        if (Files.exists(config)) {
            Thread.sleep(1); // Ensure file's mtime value is different.
            Files.write(config, mapping.getBytes(), StandardOpenOption.TRUNCATE_EXISTING);
            Thread.sleep(100); // sleep is necessary because mtime values are cached.
        } else {
            Files.write(config, mapping.getBytes(), StandardOpenOption.CREATE_NEW);
        }
    }

    private void givenConfigHasBeenRead() throws Exception {
        mapFile.mapping();
    }

    private void whenMapping(Principal principal) throws Exception {
        mappedPrincipals = mapFile.mapping().entrySet().stream()
              .filter(e -> e.getKey().matches(principal))
              .map(e -> e.getValue())
              .findFirst()
              .orElse(Collections.emptySet());
    }

    /**
     * A simple Hamcrest matcher that allows for assertions against Logback
     * events.
     */
    private static class LogEventMatcher extends TypeSafeMatcher<ILoggingEvent> {
        private final Level level;
        private final Matcher<String> messageMatcher;

        public LogEventMatcher(Level level, Matcher<String> messageMatcher) {
            this.level = requireNonNull(level);
            this.messageMatcher = requireNonNull(messageMatcher);
        }

        @Override
        protected boolean matchesSafely(ILoggingEvent item) {
            return item.getLevel() == level && messageMatcher.matches(item.getFormattedMessage());
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("a log entry logged at ").appendValue(level)
                    .appendText(" and with formatted message ")
                    .appendDescriptionOf(messageMatcher);
        }
    }
}