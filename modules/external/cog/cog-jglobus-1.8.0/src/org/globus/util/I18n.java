/*
 * Copyright 1999-2006 University of Chicago
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.globus.util;

import java.util.Map;
import java.util.HashMap;
import java.text.MessageFormat;
import java.util.ResourceBundle;
import java.util.Locale;
import java.util.MissingResourceException;

/**
 * An utility class for internationalized message handling.
 * Example usage::
 * <pre>
 * private static I18n i18n = I18n.getI18n("org.globus.resource");
 * ...
 * public void hello() {
 *    String filename = "file1";
 *    String msg = i18n.getMessage("noFile", new String[]{filename});
 *    ...
 * }
 * </pre>
 */
public class I18n {
    
    private static Map mapping = new HashMap();
    
    private ResourceBundle messages = null;

    protected I18n(ResourceBundle messages) {
        this.messages = messages;
    }

    /**
     * Retrieve a I18n instance by resource name.
     *
     * @param resource resource name. See {@link
     *        ResourceBundle#getBundle(String) ResourceBundle.getBundle()}
     */
    public static synchronized I18n getI18n(String resource) {
        I18n instance = (I18n)mapping.get(resource);
        if (instance == null) {
            instance = new I18n(ResourceBundle.getBundle(resource, 
                                                         Locale.getDefault(),
                                                         getClassLoader()));
            mapping.put(resource, instance);
        }
        return instance;
    }
    
    /**
     * Retrieve a I18n instance by resource name 
     *
     * @param resource resource name. See {@link
     *        ResourceBundle#getBundle(String) ResourceBundle.getBundle()}
     * @param loader the class loader to be used to load
     *        the resource. This parameter is only used
     *        initially to load the actual resource. Once the resource
     *        is loaded, this argument is ignored.
     */
    public static synchronized I18n getI18n(String resource,
                                            ClassLoader loader) {
        I18n instance = (I18n)mapping.get(resource);
        if (instance == null) {
            if (loader == null) {
                loader = getClassLoader();
            }
            instance = new I18n(ResourceBundle.getBundle(resource, 
                                                         Locale.getDefault(),
                                                         loader));
            mapping.put(resource, instance);
        }
        return instance;
    }
    
    private static ClassLoader getClassLoader() {
        // try to get caller's classloader otherwise use context classloader
        ClassLoader loader = ClassLoaderUtils.getClassLoaderContextAt(4);
        return (loader == null) ? 
            Thread.currentThread().getContextClassLoader() : loader;
    }
    
    /**
     * Gets a message from resource bundle.
     */
    public String getMessage(String key) 
        throws MissingResourceException {
        return messages.getString(key);
    }
    
    /**
     * Gets a formatted message from resource bundle
     */
    public String getMessage(String key, Object arg) 
        throws MissingResourceException {
        return getMessage(key, new Object[] {arg});
    }
    
    /**
     * Gets a formatted message from resource bundle
     */
    public String getMessage(String key, Object[] vars) 
        throws MissingResourceException {
        return MessageFormat.format(messages.getString(key), vars);
    }
    
}
