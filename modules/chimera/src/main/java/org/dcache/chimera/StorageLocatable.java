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


/*
-- LOCATION INFO
--
-- Generic storage information
-- ipnfsid   : pnfsid of the inode
-- itype     : type of storage, e.g. tape, disk
-- ilocation : type specific information like pool name for disk and HSM connetion for tape
-- ipriority : in case of multiple locations of hint for performance or other needs
-- ictime    : location creation time
-- iatime    : last access time, probably performance killer, but nice to have for statistics
-- istate    : location status ONLINE/OFF-LINE


 CREATE TABLE t_locationinfo {
    ipnfsid CHAR(36),
    itype INT NOT NULL,
    ilocation VARCHAR(1024) NOT NULL,
    ipriority INT NOT NULL,
    ictime timestamp NOT NULL,
    iatime timestamp NOT NULL,
    istate INT NOT NULL,
    FOREIGN KEY (ipnfsid) REFERENCES t_inodes( ipnfsid ),
    PRIMARY KEY (iparent,itype,ilocation)
};
 */


public interface StorageLocatable {

    int type();

    String location();

    int priority();

    long creationTime();

    long accessTime();

    boolean isOnline();
}

