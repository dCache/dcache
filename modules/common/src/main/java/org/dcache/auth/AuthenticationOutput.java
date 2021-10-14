/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2020 Deutsches Elektronen-Synchrotron
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
package org.dcache.auth;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * A tagging annotation that indicates the class provides information about the logged in user that
 * is available to dCache.  Such annotated objects are subject to Java serialisation.  Changes must
 * be forward- and backward compatible.
 * <p>
 * Forwards compatible means that older versions of dCache must be able to deserialise principals
 * that were serialised by a newer version of dCache. This is needed as the site may downgrade
 * (e.g., to recover quickly from some issue), or sites are running pool nodes with older versions
 * of dCache.
 * <p>
 * Backwards compatible means that newer versions of dCache must be able to deserialise principals
 * that were serialised by an older version of dCache. This is needed because login results are
 * stored by some dCache components (e.g., diskCacheV111.srm.dcache.PersistentLoginUserManager).
 * <p>
 * These requirements are easiest to achieve if a class annotated AuthenticationOutput is never
 * modified.  Therefore, care must be taken before annotating any principal with {@literal
 * @AuthenticationOutput}
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface AuthenticationOutput {

}
