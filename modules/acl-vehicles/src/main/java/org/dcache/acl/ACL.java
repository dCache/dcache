package org.dcache.acl;

import com.google.common.collect.ImmutableList;

import java.io.Serializable;
import java.util.List;

import org.dcache.acl.enums.RsType;

/**
 * An access control list (ACL) is an array of access control entries (ACE).
 *
 * @author David Melkumyan, DESY Zeuthen
 *
 */
public class ACL implements Serializable
{
    private static final long serialVersionUID = -1883807712749350105L;

    private static final String LINE_SEPARATOR = System.getProperty("line.separator", "\n");

    private static final String SPACE_SEPARATOR = " ";

    private static final String SEPARATOR = ":";


    /**
     * The resource type identifies to resource type to which this ACE applies.
     */
    private final RsType _rsType;

    /**
     * List of ACEs
     */
    private final List<ACE> _list;

    /**
     * @param rsId
     *            Resource Identifier
     * @param rsType
     *            Resource Type
     * @param list
     *            List of ACEs
     */
    public ACL(RsType rsType, List<ACE> list) {
        _rsType = rsType;
        _list = ImmutableList.copyOf(list);
    }

    /**
     * @return Returns list of ACEs
     */
    public List<ACE> getList() {
        return _list;
    }

    /**
     * @return Returns ACL resource type
     */
    public RsType getRsType() {
        return _rsType;
    }

    /**
     * @return <code>true</code> if ACEs list is empty, otherwise <code>false</code>
     */
    public boolean isEmpty() {
        return _list.isEmpty();
    }

    public String toNFSv4String() {
        StringBuilder sb = new StringBuilder();
        sb.append(_rsType).append(SPACE_SEPARATOR);
        for (int index = 0; index < _list.size(); index++) {
            if (index > 0) {
                sb.append(SPACE_SEPARATOR);
            }
            sb.append(_list.get(index).toNFSv4String(_rsType));
        }
        return sb.toString();
    }

    public String toOrgString() {
        StringBuilder sb = new StringBuilder();
        sb.append(_rsType.getValue()).append(LINE_SEPARATOR);
        for (ACE ace : _list) {
            sb.append(ace.toOrgString()).append(LINE_SEPARATOR);
        }
        return sb.toString();
    }

    /**
     * Represents ACL in the extra format.
     * <p>
     * Example: rsId = 000062D672D6F693417AABEF42308CF69D85, rsType = DIR
     * <p>
     * GROUP:5063:+fD
     * <p>
     * GROUP:7777:+fD
     * <p>
     * GROUP@:+l:f
     *
     * @return ACL in extra format
     * @throws ACLException
     *             if ACE cannot be represented in extra format
     */
    public String toExtraFormat() throws ACLException {
        StringBuilder sb = new StringBuilder();
        sb.append(_rsType);
        for (ACE ace : _list) {
            sb.append(LINE_SEPARATOR).append(ace.toExtraFormat(_rsType));
        }

        return sb.toString();
    }

    public String toString() {
        StringBuilder sb = new StringBuilder("ACL: ");
        sb.append("rsType = ").append(_rsType);
        for (ACE ace : _list) {
            sb.append(LINE_SEPARATOR).append(ace.toString(_rsType));
        }

        return sb.toString();
    }
}
