/*
 * dCache - http://www.dcache.org/
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
package org.dcache.gplazma.oidc;

import java.util.Properties;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;

public class PropertiesUtilsTest {

    /**
     * A simple fluent builder class for properties.
     */
    private static class PropertiesBuilder {
        private final Properties properties = new Properties();

        public PropertiesBuilder with(String key, String value) {
            properties.setProperty(key, value);
            return this;
        }

        public Properties build() {
            return properties;
        }
    }

    @Test
    public void shouldParseValidInProperty() {
        var properties = given(properties().with("foo", "1234"));

        var result = PropertiesUtils.asInt(properties, "foo");

        assertThat(result, is(equalTo(1234)));
    }

    @Test(expected=IllegalArgumentException.class)
    public void shouldRejectInvalidProperty() {
        var properties = given(properties().with("foo", "NOT A NUMBER"));

        PropertiesUtils.asInt(properties, "foo");
    }

    @Test(expected=IllegalArgumentException.class)
    public void shouldRejectMissingProperty() {
        var properties = given(properties().with("foo", "1234"));

        PropertiesUtils.asInt(properties, "bar");
    }

    @Test
    public void shouldAcceptAsPresentOptionalIntProperty() {
        var properties = given(properties().with("foo", "42"));

        var value = PropertiesUtils.asOptionalInt(properties, "foo");

        assertThat(value.getAsInt(), is(equalTo(42)));
    }

    @Test
    public void shouldAcceptAsEmptyOptionalIntProperty() {
        var properties = given(properties());

        var value = PropertiesUtils.asOptionalInt(properties, "foo");

        assertFalse(value.isPresent());
    }

    @Test(expected=IllegalArgumentException.class)
    public void shouldRejectOptionalIntPropertyWithNonParsableValue() {
        var properties = given(properties().with("foo", "bar"));

        PropertiesUtils.asOptionalInt(properties, "foo");
    }

    private Properties given(PropertiesBuilder builder) {
        return builder.build();
    }

    private PropertiesBuilder properties() {
        return new PropertiesBuilder();
    }
}