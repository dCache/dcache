/*
 * This library is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Library General Public License as
 * published by the Free Software Foundation; either version 2 of the
 * License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this program (see the file COPYING.LIB for more
 * details); if not, write to the Free Software Foundation, Inc.,
 * 675 Mass Ave, Cambridge, MA 02139, USA.
 */
package org.dcache.chimera;

import java.io.IOException;

public class ChimeraFsException extends IOException {

    private static final long serialVersionUID = 6626394459630111276L;

    /**
     * Creates a new instance of <code>ChimeraFsException</code> without detail message.
     */
    public ChimeraFsException() {
        super();
    }

    /**
     * Constructs an instance of <code>ChimeraFsException</code> with the specified detail message.
     * @param msg the detail message.
     */
    public ChimeraFsException(String msg) {
        super(msg);
    }

    public ChimeraFsException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
