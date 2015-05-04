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

public class FileExistsChimeraFsException extends ChimeraFsException {

    private static final long serialVersionUID = 7401509959112681879L;

    /** Creates a new instance of FileExistsChimeraFsException */
    public FileExistsChimeraFsException() {
        super();
    }

    public FileExistsChimeraFsException(Throwable cause) {
        super(null, cause);
    }

    public FileExistsChimeraFsException(String path) {
        this(path, null);
    }

    public FileExistsChimeraFsException(String path, Throwable cause) {
        super("path [" + path + "] already exist", cause);
    }
}
