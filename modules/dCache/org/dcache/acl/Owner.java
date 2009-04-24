package org.dcache.acl;

/**
 * An object of type Origin contains information about the owner of a resource.
 *
 * @author David Melkumyan, DESY Zeuthen
 */
public class Owner {

    /**
     * Owning user’s virtual user ID.
     */
    private int _uid;

    /**
     * Owning group’s virtual group ID.
     */
    private int _gid;

    /**
     * @param uid
     *            Owning user’s virtual user ID.
     * @param gid
     *            Owning group’s virtual group ID.
     */
    public Owner(int uid, int gid) {
        super();
        _uid = uid;
        _gid = gid;
    }

    public int getGid() {
        return _gid;
    }

    public void setGid(int gid) {
        _gid = gid;
    }

    public int getUid() {
        return _uid;
    }

    public void setUid(int uid) {
        _uid = uid;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("owner.uid = ").append(_uid);
        sb.append(", owner.gid = ").append(_gid);
        return sb.toString();
    }

}
