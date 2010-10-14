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
package org.globus.gsi.gssapi.auth;

import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.GSSCredential;

/**
 * GSSAPI client authorization.
 */
public abstract class GSSAuthorization extends Authorization {
    
    /**
     * Returns expected <code>GSSName</code> used for authorization purposes.
     * Can returns null for self authorization.
     *
     * @param cred credentials used
     * @param host host address of the peer.
     * @exception GSSException if unable to create the name.
     */
    public abstract GSSName getExpectedName(GSSCredential cred, String host) 
	throws GSSException;
    
}
