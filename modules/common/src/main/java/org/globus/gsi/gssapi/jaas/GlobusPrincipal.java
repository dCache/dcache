/*
 * Copyright 1999-2010 University of Chicago
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS,WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied.
 *
 * See the License for the specific language governing permissions and limitations under the License.
 */
package org.globus.gsi.gssapi.jaas;

import eu.emi.security.authn.x509.impl.OpensslNameUtils;
import javax.security.auth.x500.X500Principal;
import org.dcache.auth.AuthenticationOutput;

/**
 * A Globus DN principal. The Globus DN is in the form: "/CN=foo/O=bar".
 *
 * @since 2.14
 */
@AuthenticationOutput
public class GlobusPrincipal
      extends SimplePrincipal {

    private static final long serialVersionUID = 302803142179565960L;

    public GlobusPrincipal(X500Principal principal) {
        super(OpensslNameUtils.convertFromRfc2253(principal.getName(), true));
    }

    public GlobusPrincipal(String globusDn) {
        super(globusDn);
    }
}
