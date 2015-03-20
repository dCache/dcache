/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2015 Deutsches Elektronen-Synchrotron
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
package org.dcache.srm.aspects;

import java.io.EOFException;
import java.io.OutputStream;

import org.apache.axis.utils.JavaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Advices Axis 1 to disable Attachment support; this suppresses the warning of missing classes.
 */
aspect DisableAttachmentSupport
{
    pointcut isAttachmentSupportedCalls() : call(boolean JavaUtils.isAttachmentSupported());

    boolean around() : isAttachmentSupportedCalls() {
        return false;
    }
}
