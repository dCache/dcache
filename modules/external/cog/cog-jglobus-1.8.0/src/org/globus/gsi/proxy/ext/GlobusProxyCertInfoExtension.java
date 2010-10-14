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
package org.globus.gsi.proxy.ext;

import org.globus.gsi.bc.BouncyCastleX509Extension;
import org.globus.util.I18n;

/**
 * Represents ProxyCertInfo X.509 extension.
 */
public class GlobusProxyCertInfoExtension extends BouncyCastleX509Extension {

    private static I18n i18n =
            I18n.getI18n("org.globus.gsi.gssapi.errors",
                         GlobusProxyCertInfoExtension.class.getClassLoader());

    public GlobusProxyCertInfoExtension(ProxyCertInfo value) {
    super(ProxyCertInfo.OLD_OID.getId(), true, null);
    if (value == null) {
        throw new IllegalArgumentException(i18n.getMessage("proxyErr22"));
    }
    setValue(value);
    }

    public void setOid(String oid) {
    throw new RuntimeException(i18n.getMessage("proxyErr23"));
    }

    public void setCritical(boolean critical) {
    throw new RuntimeException(i18n.getMessage("proxyErr24"));
    }
}
