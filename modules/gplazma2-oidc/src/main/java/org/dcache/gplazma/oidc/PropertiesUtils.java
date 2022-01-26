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
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * A set of utility methods for handling Properties objects.
 */
public class PropertiesUtils {

    private PropertiesUtils() {} // No instantiating: static utility methods only!

    public static int asInt(Properties properties, String key) {
        try {
            String value = properties.getProperty(key);
            checkArgument(value != null, "Missing " + key + " property");
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Bad " + key + "value: " + e.getMessage());
        }
    }

    public static Duration asDuration(Properties properties, String key) {
        ChronoUnit units = asChronoUnit(properties, key + ".unit");
        return Duration.of(asInt(properties, key), units);
    }

    public static ChronoUnit asChronoUnit(Properties properties, String key) {
        String value = properties.getProperty(key);
        checkArgument(value != null, "Missing " + key + " property");
        return TimeUnit.valueOf(value).toChronoUnit();
    }

    public static int asIntOrDefault(Properties properties, String key, int defaultValue) {
        try {
            String value = properties.getProperty(key);
            if (value == null) {
                return defaultValue;
            }
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Bad " + key + "value: " + e.getMessage());
        }
    }
}
