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
package org.globus.tools.proxy;

import java.security.cert.X509Certificate;

import org.globus.common.CoGProperties;
import org.globus.gsi.GlobusCredential;

public abstract class GridProxyModel {

    protected X509Certificate userCert;

    protected CoGProperties props = null;

    public abstract GlobusCredential createProxy(String pwd) throws Exception;

    public CoGProperties getProperties() {
        if (props == null) {
            props = CoGProperties.getDefault();
        }
        return props;
    }

    public boolean getLimited() {
        return false;
    }

}
