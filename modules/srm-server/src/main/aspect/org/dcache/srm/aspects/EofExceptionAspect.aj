/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2014 Deutsches Elektronen-Synchrotron
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

import org.apache.axis.Message;
import org.apache.axis.SOAPPart;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Advices Axis 1 to not log a stack trace when the client disconnects before a reply
 * was sent.
 */
aspect EofExceptionAspect
{
    private static final Logger LOGGER = LoggerFactory.getLogger(Message.class);

    pointcut writeToCalls() :
            withincode(void Message.writeTo(OutputStream)) && call(void SOAPPart.writeTo(OutputStream));

    void around() : writeToCalls() {
        try {
            proceed();
        } catch (EOFException e) {
            LOGGER.warn("Client disconnected before SRM response was sent.");
        }
    }
}
