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

import java.util.Arrays;

/**
 * Indicates the status of a file's data.
 * @since: 3.1
 */
public enum FileState {

    /**
     * Deprecated: file has been written iff file has non-zero size or level2 exists.
     */
    LEGACY(0),
    /**
     * Database file - file's data is stored in Chimera's internal t_inode_data table.
     */
    DB(1),
    /**
     * File's data is either not stored or is being uploaded.
     */
    CREATED(2),
    /**
     * File's data was successfully uploaded.
     */
    STORED(3);

    private final int state;

    private FileState(int state) {
        this.state = state;
    }

    public int getValue() {
        return state;
    }

    public static FileState valueOf(int state) {
        return Arrays.stream(values())
                .filter(s -> s.getValue() == state)
                .findAny()
                .orElseThrow(IllegalArgumentException::new);
    }
}
