package org.dcache.acl;

/**
 * The Subject is the internal object that identifies the subject that is trying
 * to access a resource. It contains the (unique) user’s virtual ID and a list
 * of virtual group IDs. The first element in the list of virtGids is called
 * primary virtual group ID.
 *
 * @author David Melkumyan, DESY Zeuthen
 */
public class Subject {

    /**
     * Virtual user ID.
     */
    private int _uid;

    /**
     * List of virtual group IDs.
     */
    private int[] _gids;

    /**
     * @param uid
     *            Virtual user ID.
     * @param gid
     *            Primary virtual group ID
     */
    public Subject(int uid, int gid) {
        _uid = uid;
        _gids = new int[1];
        _gids[0] = gid;
    }

    /**
     * @param uid
     *            Virtual user ID.
     * @param gids
     *            List of virtual group IDs.
     */
    public Subject(int uid, int[] gids) {
        _uid = uid;
        _gids = gids;
    }

    /**
     * @param uid
     *            Virtual user ID.
     * @param gid
     *            primary virtual group ID
     * @param gids
     *            List of secondary virtual group IDs.
     */
    public Subject(int uid, int gid, int[] gids) {
        _uid = uid;
        if ( gids == null || gids.length == 0 ) {
            _gids = new int[1];
            _gids[0] = gid;

        } else {
            _gids = new int[gids.length + 1];
            _gids[0] = gid;
            for (int i = 0; i < gids.length; i++)
                _gids[i + 1] = gids[i];
        }
    }

    public int getGid() {
        return _gids[0];
    }

    public void setGid(int gid) {
        if (_gids.length > 1)
            _gids = new int[1];
        _gids[0] = gid;
    }

    public int[] getGids() {
        return _gids;
    }

    public void setGids(int[] gids) {
        _gids = gids;
    }

    public int getUid() {
        return _uid;
    }

    public void setUid(int uid) {
        _uid = uid;
    }

    /**
     * @param gid
     *            Virtual group ID.
     * @return Returns true id list of virtual group IDs contains group
     *         specified by gid.
     */
    public boolean inGroup(int gid) {
        for (int id : _gids)
            if ( id == gid )
                return true;
        return false;
    }

    /**
     * @return Returns true if subject is a root.
     */
    public boolean isRoot() {
        return (_uid == 0);
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("uid = ").append(_uid);

        StringBuilder sb2 = new StringBuilder();
        for (int gid : _gids) {
            if ( sb2.length() != 0 )
                sb2.append(", ");
            sb2.append(gid);
        }
        sb.append(", gids = [").append(sb2).append("]");
        return sb.toString();
    }

}
