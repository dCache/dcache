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
package org.globus.mds.gsi.jndi;

import java.security.Provider;
import java.security.Security;


public final class SaslProvider extends Provider {

    private static SaslProvider provider = new SaslProvider();

    public SaslProvider() {
        super("Globus", 1.0, "Globus Security Sasl Provider");
        setProperty("SaslClientFactory.GSS-GSI",  
                    "org.globus.mds.gsi.jndi.SaslClientFactoryWrapper");
        setProperty("SaslClientFactory.GSS-OWNYQ6NTEOAUVGWG",
                    "org.globus.mds.gsi.jndi.SaslClientFactoryWrapper");
    }

    public static SaslProvider getInstance() {
        return provider;
    }

    public static void addProvider() {
        Security.addProvider(getInstance());
    }
}
