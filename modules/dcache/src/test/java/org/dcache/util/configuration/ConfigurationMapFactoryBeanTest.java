/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2019  Deutsches Elektronen-Synchrotron
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
package org.dcache.util.configuration;

import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;

public class ConfigurationMapFactoryBeanTest {

    ConfigurationMapFactoryBean configuration;

    @Before
    public void setUp() {
        configuration = new ConfigurationMapFactoryBean();
    }

    @Test
    public void testPrefixSubstitution() {


        String prefix = "someprefix";
        String realKey = "key1";
        String envKey = prefix + "!" + realKey;
        String value = "value1";

        Map<String, Object> env = ImmutableMap.of(
                envKey, value
        );

        configuration.setPrefix(prefix);
        configuration.setEnvironment(env);
        configuration.buildMap();

        ImmutableMap<String, String> effectiveEnv = configuration.getObject();

        assertFalse(effectiveEnv.containsKey(envKey));
        assertEquals(value, effectiveEnv.get(realKey));


    }


    @Test
    public void testObjectType() {


        String prefix = "someprefix";
        String realKey = "key1";
        String envKey = prefix + "!" + realKey;
        String value = "value1";

        Map<String, Object> env = ImmutableMap.of();

        configuration.setPrefix(prefix);
        configuration.setEnvironment(env);
        configuration.buildMap();


        Class<?> effectiveEnvType = configuration.getObjectType();

        assertEquals(ImmutableMap.class, effectiveEnvType);


    }

    @Test
    public void testIsSingleton() {


        String prefix = "someprefix";
        String realKey = "key1";
        String envKey = prefix + "!" + realKey;
        String value = "value1";

        Map<String, Object> env = ImmutableMap.of();

        configuration.setPrefix(prefix);
        configuration.setEnvironment(env);
        configuration.buildMap();

        assertTrue(configuration.isSingleton());


    }




    @Test
    public void testKeyNull() {


        String prefix = "someprefix";
        String realKey = "key1";
        String envKey = prefix + "!";
        String value = "value1";

        Map<String, Object> env = ImmutableMap.of(
                envKey, value
        );

        configuration.setPrefix(prefix);
        configuration.setEnvironment(env);
        configuration.buildMap();

        ImmutableMap<String, String> effectiveEnv = configuration.getObject();

        assertEquals(null, effectiveEnv.get(realKey));


    }

    @Test
    public void testWrongPrefix() {


        String prefix = "someprefix";
        String realKey = "key1";
        String envKey = "*" + realKey;
        String value = "value1";


        String envKey2 = "blabla";
        Integer value2 = 2;


        Map<String, Object> env = ImmutableMap.of(
                envKey, value,
                envKey2, value2
        );

        configuration.setPrefix(prefix);
        configuration.setEnvironment(env);
        configuration.buildMap();

        ImmutableMap<String, String> effectiveEnv = configuration.getObject();

        assertTrue(effectiveEnv.isEmpty());


    }



}
