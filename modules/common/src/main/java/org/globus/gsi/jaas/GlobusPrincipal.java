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
package org.globus.gsi.jaas;

/**
 * A Globus DN principal. The Globus DN is in the form: "/CN=foo/O=bar".
 *
 * To maintain compatibility with pools before 2.5, we cannot use
 * GlobusPrincipal from JGlobus 2, org.globus.gsi.jaas.GlobusPrincipal.
 * Older versions of dCache would be unable to deserialize messages
 * containing the new principal.
 *
 * The following is a possible migration strategy.
 *
 * 1. dCache 2.6 cannot include the new principal in messages as it needs
 *    to maintain compatibility with 2.2 pools.

 * 2. dCache 2.7 only needs to be compatible with dCache 2.6, and 2.6 can
 *    deserialize the new principal. Thus dCache 2.7 can include the new
 *    principal in messages.
 *
 * 3. dCache 2.7 head nodes must work with dCache 2.6 pools and pools
 *    of 2.6 and 2.7 must work together.
 *
 * 4. Pools neither access nor produce GlobusPrincipal. They only ever
 *    receive Subjects and pass on Subjects containing GlobusPrincipal.
 *
 * Thus in 2.7 we can safely replace all uses of GlobusPrincipal with
 * the new class from JGlobus 2. Pools using 2.6 will still be able
 * to deserialize and serialize instances of the new class, but will
 * otherwise not pay attention to them.
 */
@Deprecated // will be removed in 2.11
public class GlobusPrincipal
    extends SimplePrincipal
{
    public GlobusPrincipal(String globusDn)
    {
	super(globusDn);
    }
}
