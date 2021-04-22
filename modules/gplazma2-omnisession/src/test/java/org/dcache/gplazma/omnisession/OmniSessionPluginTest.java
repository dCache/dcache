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

import org.junit.Before;
import org.junit.Test;
import org.mockito.BDDMockito;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import org.dcache.auth.attributes.HomeDirectory;
import org.dcache.auth.attributes.LoginAttribute;
import org.dcache.auth.attributes.RootDirectory;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.plugins.GPlazmaSessionPlugin;
import org.dcache.util.PrincipalSetMaker;

import static java.util.Arrays.asList;
import static org.dcache.util.PrincipalSetMaker.aSetOfPrincipals;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;

public class OmniSessionPluginTest
{
    private GPlazmaSessionPlugin plugin;
    private Set<Object> attributes;

    @Before
    public void setup()
    {
        attributes = new HashSet<>();
        plugin = null;
    }

    @Test(expected=IllegalArgumentException.class)
    public void shouldFailIfPropertyMissing()
    {
        new OmniSessionPlugin(new Properties());
    }

    @Test(expected=AuthenticationException.class)
    public void shouldPropagateError() throws Exception
    {
        given(aPlugin().throwsAuthenticationException());

        whenCalledWith(aSetOfPrincipals().withUsername("paul"));
    }

    @Test(expected=AuthenticationException.class)
    public void shouldRaiseErrorFileIsUnreadable() throws Exception
    {
        given(aPlugin().thatFailedToParse());

        whenCalledWith(aSetOfPrincipals().withUsername("paul"));
    }

    @Test
    public void shouldAddAllAttributesWhenNonExisting() throws Exception
    {
        given(aPlugin().thatReturns(new RootDirectory("/"),
                new HomeDirectory("/home")));

        whenCalledWith(aSetOfPrincipals().withUsername("paul"));

        assertThat(attributes, containsInAnyOrder(new RootDirectory("/"),
                new HomeDirectory("/home")));
    }

    @Test
    public void shouldSuppressExistingAttributes() throws Exception
    {
        given(aPlugin().thatReturns(new RootDirectory("/"),
                new HomeDirectory("/home")));

        whenCalledWith(aSetOfPrincipals().withUsername("paul"), new RootDirectory("/root-dir"));

        assertThat(attributes, containsInAnyOrder(new RootDirectory("/root-dir"),
                new HomeDirectory("/home")));
    }

    @Test
    public void shouldReadConfiguredFile() throws Exception
    {
        given(aFileBackedPlugin().withContents("DEFAULT home:/home-dir root:/root-dir"));

        whenCalledWith(aSetOfPrincipals().withUsername("paul"));

        assertThat(attributes, containsInAnyOrder(new RootDirectory("/root-dir"),
                new HomeDirectory("/home-dir")));
    }

    private void whenCalledWith(PrincipalSetMaker maker)
            throws AuthenticationException
    {
        plugin.session(maker.build(), attributes);
    }

    private void whenCalledWith(PrincipalSetMaker maker, LoginAttribute... existingAttributes)
            throws AuthenticationException
    {
        attributes.addAll(asList(existingAttributes));
        plugin.session(maker.build(), attributes);
    }

    private void given(PluginBuilder builder) throws Exception
    {
        plugin = builder.build();
    }

    private static MemoryBackedPluginBuilder aPlugin()
    {
        return new MemoryBackedPluginBuilder();
    }

    private FileBackedPluginBuilder aFileBackedPlugin() throws IOException
    {
        return new FileBackedPluginBuilder();
    }

    private interface PluginBuilder
    {
        GPlazmaSessionPlugin build() throws Exception;
    }

    /**
     * Build a plugin that uses a real file in the filesystem for its
     * configuration.
     */
    private static class FileBackedPluginBuilder implements PluginBuilder
    {
        private final Path tempFile;
        private final StringBuilder contents = new StringBuilder();

        public FileBackedPluginBuilder() throws IOException
        {
            tempFile = Files.createTempFile("omnisession-unittest", ".tmp");
            tempFile.toFile().deleteOnExit();
        }

        public FileBackedPluginBuilder withContents(String... lines)
        {
            Arrays.stream(lines).forEach(l -> contents.append(l).append('\n'));
            return this;
        }

        @Override
        public GPlazmaSessionPlugin build() throws IOException
        {
            Files.writeString(tempFile, contents);
            Properties properties = new Properties();
            properties.setProperty("gplazma.omnisession.file", tempFile.toAbsolutePath().toString());
            return new OmniSessionPlugin(properties);
        }
    }

    private static class MemoryBackedPluginBuilder implements PluginBuilder
    {
        private final Configuration configuration = mock(Configuration.class);

        private boolean isBad;

        public MemoryBackedPluginBuilder thatFailedToParse()
        {
            isBad = true;
            return this;
        }

        public MemoryBackedPluginBuilder throwsAuthenticationException() throws AuthenticationException
        {
            BDDMockito.given(configuration.attributesFor(any())).willThrow(AuthenticationException.class);
            return this;
        }

        public MemoryBackedPluginBuilder thatReturns(LoginAttribute... attributes) throws AuthenticationException
        {
            BDDMockito.given(configuration.attributesFor(any())).willReturn(asList(attributes));
            return this;
        }

        @Override
        public GPlazmaSessionPlugin build()
        {
            Optional<Configuration> parseResult = isBad ? Optional.empty() : Optional.of(configuration);
            return new OmniSessionPlugin(() -> parseResult);
        }
    }
}
