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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UnixPermissionHandler implements AclHandler {

    private static final Logger _log = LoggerFactory.getLogger(UnixPermissionHandler.class);
    private static final UnixPermissionHandler HANDLER = new UnixPermissionHandler();

    private UnixPermissionHandler() {
    }

    public static UnixPermissionHandler getInstance() {
        return HANDLER;
    }

    private static boolean inGroup(User user, int group) {

        int userGid = ((UnixUser) user).getGID();
        int[] userGids = ((UnixUser) user).getGIDS();

        boolean inGroup = false;

        if (group == userGid) {
            inGroup = true;
        } else {
            for (int gid : userGids) {
                if (group == gid) {
                    inGroup = true;
                    break;
                }
            }
        }

        return inGroup;
    }

    @Override
    public boolean isAllowed(Acl acl, User user, int requsetedAcl) {

        boolean isAllowed = false;
        int userUid = ((UnixUser) user).getUID();

        if (!(acl instanceof UnixAcl)) {
            return false;
        }

        int resourceUid = ((UnixAcl) acl).getOwner();
        int resourceGid = ((UnixAcl) acl).getGroup();
        int resourcePermissions = ((UnixAcl) acl).getPermission();



        if (_log.isDebugEnabled()) {
            StringBuilder sb = new StringBuilder("ACL request : ");
            sb.append("user=").append(((UnixUser) user).getUID()).append(':').append(((UnixUser) user).getGID());
            sb.append(' ');
            sb.append("file=").append(resourceUid).append(':').append(resourceGid);
            sb.append(' ');
            sb.append("action=");
            switch (requsetedAcl) {

                case ACL_READ:
                    sb.append("ACL_READ");
                    break;
                case ACL_WRITE:
                    sb.append("ACL_WRITE");
                    break;
                case ACL_DELETE:
                    sb.append("ACL_DELETE");
                    break;
                case ACL_LOOKUP:
                    sb.append("ACL_LOOKUP");
                    break;
                case ACL_ADMINISTER:
                    sb.append("ACL_ADMINISTER");
                    break;
                case ACL_INSERT:
                    sb.append("ACL_INSERT");
                    break;
                case ACL_LOCK:
                    sb.append("ACL_LOCK");
                    break;
                case ACL_EXECUTE:
                    sb.append("ACL_EXECUTE");
                    break;
                default:
                    sb.append("ACL_UNKNOWN");

            } // switch( requsetedAcl )

            _log.debug(sb.toString());
        }

        switch (userUid) {

            case 0:
                // root has an access to everything
                isAllowed = true;
                break;
            case -12:
                // bad (?) user
                isAllowed = false;
                break;
            default:
                // regular users

                switch (requsetedAcl) {

                    case ACL_READ:
                        if (resourceUid == userUid) {
                            isAllowed = ((resourcePermissions & 0400) == 0400);
                        } else if (inGroup(user, resourceGid)) {
                            // check for group
                            isAllowed = ((resourcePermissions & 0040) == 0040);
                        } else {
                            // check for others
                            isAllowed = ((resourcePermissions & 0004) == 0004);
                        }
                        break;
                    case ACL_WRITE:
                        if (resourceUid == userUid) {
                            isAllowed = ((resourcePermissions & 0200) == 0200);
                        } else if (inGroup(user, resourceGid)) {
                            // check for group
                            isAllowed = ((resourcePermissions & 0020) == 0020);
                        } else {
                            // check for others
                            isAllowed = ((resourcePermissions & 0002) == 0002);
                        }
                        break;
                    case ACL_DELETE:
                        isAllowed = isAllowed(acl, user, ACL_WRITE);
                        // if there is a sticky bit, only owner allowed to remove
                        if ((resourcePermissions & 01000) == 01000) {
                            System.out.println("Sticky bit set!");
                            isAllowed = isAllowed && isAllowed(acl, user, ACL_ADMINISTER);
                        }
                        break;
                    case ACL_LOOKUP:
                        isAllowed = isAllowed(acl, user, ACL_READ) && isAllowed(acl, user, ACL_EXECUTE);
                        break;
                    case ACL_ADMINISTER:
                        isAllowed = (resourceUid == userUid);
                        break;
                    case ACL_INSERT:
                        // isAllowed = isAllowed(acl, user, ACL_WRITE) && isAllowed(acl, user, ACL_EXECUTE);
                        isAllowed = isAllowed(acl, user, ACL_WRITE);
                        break;
                    case ACL_LOCK:
                        isAllowed = true;
                        break;
                    case ACL_EXECUTE:
                        if (resourceUid == userUid) {
                            isAllowed = ((resourcePermissions & 0100) == 0100);
                        } else if (inGroup(user, resourceGid)) {
                            // check for group
                            isAllowed = ((resourcePermissions & 0010) == 0010);
                        } else {
                            // check for others
                            isAllowed = ((resourcePermissions & 0001) == 0001);
                        }
                        break;

                } // switch( requsetedAcl )


        } // switch( userUid )

        _log.debug("IsAllowed: " + isAllowed);
        return isAllowed;
    }
}
