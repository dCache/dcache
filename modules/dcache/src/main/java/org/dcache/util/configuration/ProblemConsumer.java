/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2011 - 2018 Deutsches Elektronen-Synchrotron
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
package org.dcache.util.configuration;

import java.io.LineNumberReader;

/**
 * A class that implement this interface, when registered, will accept
 * responsibility for handling the warnings and errors produced when
 * parsing dCache configuration.  These methods may throw an exception,
 * to terminate parsing; however, code using a ProblemsAware class must
 * not assume that this will happen.
 */
public interface ProblemConsumer {
    void setFilename(String name);
    void setLineNumberReader(LineNumberReader reader);
    void error(String message);
    void warning(String message);
    void info(String message);
}
