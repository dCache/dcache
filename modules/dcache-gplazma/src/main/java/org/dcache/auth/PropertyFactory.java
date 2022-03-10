/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2022 Deutsches Elektronen-Synchrotron
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
package org.dcache.auth;

import dmg.cells.nucleus.EnvironmentAware;
import dmg.util.Formats;
import dmg.util.Replaceable;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import static java.util.Objects.requireNonNull;
import java.util.Properties;

/**
 * This is a factory class for creating the Properties for gPlazma.
 */
public class PropertyFactory implements EnvironmentAware {

    private Map<String, Object> _environment;

    @Override
    public void setEnvironment(Map<String, Object> environment) {
        _environment = requireNonNull(environment);
    }

    public Properties build() {
        Replaceable replaceable = name -> Objects.toString(_environment.get(name), null);

        Properties properties = new Properties();
        for (Map.Entry<String, Object> e : _environment.entrySet()) {
            String key = e.getKey();
            String value = String.valueOf(e.getValue());
            properties.put(key, Formats.replaceKeywords(value, replaceable));
        }

        return properties;
    }
}
