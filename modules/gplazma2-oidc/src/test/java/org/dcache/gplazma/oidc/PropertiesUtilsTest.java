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

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Properties;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

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

        var value = PropertiesUtils.asIntOrDefault(properties, "foo", 1);

        assertThat(value, is(equalTo(42)));
    }

    @Test
    public void shouldAcceptAsEmptyOptionalIntProperty() {
        var properties = given(properties());

        var value = PropertiesUtils.asIntOrDefault(properties, "foo", 42);

        assertThat(value, is(equalTo(42)));
    }

    @Test(expected=IllegalArgumentException.class)
    public void shouldRejectOptionalIntPropertyWithNonParsableValue() {
        var properties = given(properties().with("foo", "bar"));

        PropertiesUtils.asIntOrDefault(properties, "foo", 42);
    }

    @Test(expected=IllegalArgumentException.class)
    public void shouldRejectChronoUnitWithMissingProperty() {
        var properties = given(properties());

        PropertiesUtils.asChronoUnit(properties, "foo");
    }

    @Test(expected=IllegalArgumentException.class)
    public void shouldRejectChronoUnitWithValidProperty() {
        var properties = given(properties().with("units", "JIFFIES"));

        PropertiesUtils.asChronoUnit(properties, "units");
    }

    @Test
    public void shouldAcceptChronoUnitWithValidProperty() {
        var properties = given(properties().with("units", "SECONDS"));

        var value = PropertiesUtils.asChronoUnit(properties, "units");

        assertThat(value, is(equalTo(ChronoUnit.SECONDS)));
    }

    @Test(expected=IllegalArgumentException.class)
    public void shouldRejectDurationWithMissingProperty() {
        var properties = given(properties().with("duration.unit", "SECONDS"));

        PropertiesUtils.asDuration(properties, "duration");
    }

    @Test(expected=IllegalArgumentException.class)
    public void shouldRejectDurationWithMissingUnitProperty() {
        var properties = given(properties().with("duration", "25"));

        PropertiesUtils.asDuration(properties, "duration");
    }

    @Test(expected=IllegalArgumentException.class)
    public void shouldRejectDurationWithNonNumericalProperty() {
        var properties = given(properties().with("duration", "a few")
                .with("duration.unit", "SECONDS"));

        PropertiesUtils.asDuration(properties, "duration");
    }

    @Test(expected=IllegalArgumentException.class)
    public void shouldRejectDurationWithInvalidUnits() {
        var properties = given(properties().with("duration", "42")
                .with("duration.unit", "JIFFIES"));

        PropertiesUtils.asDuration(properties, "duration");
    }

    @Test
    public void shouldAcceptValidDuration() {
        var properties = given(properties().with("duration", "42")
                .with("duration.unit", "SECONDS"));

        var duration = PropertiesUtils.asDuration(properties, "duration");

        assertThat(duration, is(equalTo(Duration.ofSeconds(42))));
    }

    private Properties given(PropertiesBuilder builder) {
        return builder.build();
    }

    private PropertiesBuilder properties() {
        return new PropertiesBuilder();
    }
}