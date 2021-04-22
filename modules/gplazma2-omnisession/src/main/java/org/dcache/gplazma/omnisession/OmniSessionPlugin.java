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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;

import java.nio.file.FileSystems;
import java.security.Principal;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.dcache.auth.attributes.LoginAttribute;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.plugins.GPlazmaSessionPlugin;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * A generic session plugin that supports configuring simple session
 * information.
 */
public class OmniSessionPlugin implements GPlazmaSessionPlugin
{
    private static final String OMNISESSION_FILE = "gplazma.omnisession.file";

    private final Supplier<Optional<Configuration>> file;

    public OmniSessionPlugin(Properties properties)
    {
        this(configFileFrom(properties));
    }

    private static String requiredProperty(Properties properties, String name)
    {
        String value = properties.getProperty(name);
        checkArgument(!Strings.isNullOrEmpty(value), "Undefined property: " + name);
        return value;
    }

    private static ParsableFile<Configuration> configFileFrom(Properties properties)
    {
        var pathValue = requiredProperty(properties, OMNISESSION_FILE);
        var path = FileSystems.getDefault().getPath(pathValue);
        var parser = new LineByLineParser<Configuration>(ConfigurationParser::new);
        return new ParsableFile<>(parser, path);
    }

    @VisibleForTesting
    OmniSessionPlugin(Supplier<Optional<Configuration>> file)
    {
        this.file = file;
    }

    @Override
    public void session(Set<Principal> principals, Set<Object> sessionAttributes)
            throws AuthenticationException
    {
        Configuration config = file.get().orElseThrow(() -> new AuthenticationException("bad config file"));

        List<LoginAttribute> attributes = config.attributesFor(principals);

        Set<Class> existingSessionAttributes = sessionAttributes.stream()
                .map(Object::getClass)
                .collect(Collectors.toSet());

        attributes.stream()
                .filter(a -> !existingSessionAttributes.contains(a.getClass()))
                .forEach(sessionAttributes::add);
    }
}
