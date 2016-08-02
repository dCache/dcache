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
package org.dcache.chimera.posix;

import java.util.Arrays;

public class UnixUser implements User {

    private final int _uid;
    private final int _gid;
    private final int[] _gids;
    private final String _remoteHost;

    public UnixUser(int uid) {
        this(uid, -1);
    }

    public UnixUser(int uid, int gid) {
        this(uid, gid, new int[0]);
    }

    public UnixUser(int uid, int gid, int[] gids) {
        this(uid, gid, gids, null);
    }

    public UnixUser(int uid, int gid, int[] gids, String remoteHost) {
        _uid = uid;
        _gid = gid;
        _gids = gids == null ? new int[0] : gids.clone();
        _remoteHost = remoteHost;
    }

    public int getUID() {
        return _uid;
    }

    public int getGID() {
        return _gid;
    }

    public int[] getGIDS() {
        return _gids.clone();
    }

    public String getHost() {
        return _remoteHost;
    }

    @Override
    public String toString() {

        StringBuilder sb = new StringBuilder();

        sb.append("Uid = ").append(_uid).append("; ");
        sb.append("Gid = ").append(_gid).append("; ");
        if (_gids.length != 0) {
            sb.append("Gids = ").append(Arrays.toString(_gids));
        }
        sb.append("Remote Host = ").append(_remoteHost).append(';');

        return sb.toString();
    }

    @Override
    public String principal() {
        return Integer.toString(_uid);
    }

    @Override
    public String[] groups() {

        int ngroup = _gids == null ? 1 : 1 + _gids.length;

        String[] groups = new String[ngroup];
        groups[0] = Integer.toString(_gid);
        for (int i = 1; i < ngroup; i++) {
            groups[i] = Integer.toString(_gids[i - 1]);
        }

        return groups;
    }

    @Override
    public String role() {
        return this.principal();
    }

    @Override
    public String locations() {
        return this.getHost();
    }
}
