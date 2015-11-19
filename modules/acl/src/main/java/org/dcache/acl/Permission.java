package org.dcache.acl;

import org.dcache.acl.enums.AccessMask;
import org.dcache.acl.enums.RsType;

/**
 * Objects of type Permission are returned by the AclMapper to the AclMatcher
 * and contains the result obtained by evaluating a request.
 *
 * @author David Melkumyan, DESY Zeuthen
 */
public class Permission {

    // 111110000000111111111 = 1F01FF = 2032127
    public static final int ALLOW_ALL = 0x001F01FF;

    /**
     * The defMsk defines the flags which have been set to either “allow” or
     * “deny”.
     */
    private int _defMsk = 0;

    /**
     * The allowMsk defines the flags which have been set to “allow”.
     */
    private int _allowMsk = 0;

    public Permission() {
    }

    public Permission(int defMsk, int allowMsk) {
        _defMsk = defMsk;
        _allowMsk = allowMsk;
    }

    /**
     * @return Returns mask which defines all defined access operations.
     */
    public int getDefMsk() {
        return _defMsk;
    }

    public void setDefMsk(int defMsk) {
        _defMsk = defMsk;
    }

    public void appendDefMsk(int defMsk) {
        _defMsk |= defMsk;
    }

    /**
     * @return Returns mask which defines all allowed access operations.
     */
    public int getAllowMsk() {
        return _allowMsk;
    }

    public void setAllowMsk(int allowMsk) {
        _allowMsk = allowMsk;
    }

    public void appendAllowMsk(int allowMsk) {
        _allowMsk |= allowMsk;
    }

    /**
     * @return Returns mask which defines all denied access operations.
     */
    public int getDenyMsk() {
        return _defMsk ^ _allowMsk;
    }

    /**
     * Sets root access rights access masks
     */
    public void setAll() {
        _defMsk = _allowMsk = ALLOW_ALL;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("defMsk = ").append(Integer.toBinaryString(_defMsk));
        sb.append(", allowMsk = ").append(Integer.toBinaryString(_allowMsk));
        sb.append(", denyMsk = ").append(Integer.toBinaryString(_defMsk ^ _allowMsk));
        return sb.toString();
    }

    public String asString(RsType rsType) {
        if ( _defMsk == 0 ) {
            return "has not been defined";
        }

        StringBuilder sb = new StringBuilder();
        sb.append("defMsk = ").append(AccessMask.asString(_defMsk, rsType));
        if ( _allowMsk != 0) {
            sb.append(", allowMsk = ")
                    .append(AccessMask.asString(_allowMsk, rsType));
        }
        if ( (_defMsk ^ _allowMsk) != 0 ) {
            sb.append(", denyMsk = ")
                    .append(AccessMask.asString(_defMsk ^ _allowMsk, rsType));
        }
        return sb.toString();
    }
}
