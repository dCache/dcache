package org.dcache.acl;

import java.io.Serializable;
import java.util.ArrayList;
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
    static final long serialVersionUID = -1883807712749350105L;

    private static final String LINE_SEPARATOR = System.getProperty("line.separator", "\n");

    private static final String SPACE_SEPARATOR = " ";

    private static final String SEPARATOR = ":";

    /**
     * ACL Identifier
     */
    private String _rsId;

    /**
     * The resource type identifies to resource type to which this ACE applies.
     */
    private RsType _rsType;

    /**
     * List of ACEs
     */
    private List<ACE> _list;

    public ACL() {
        super();
    }

    /**
     * @param rsId
     *            Resource Identifier
     * @param rsType
     *            Resource Type
     */
    public ACL(String rsId, RsType rsType) {
        _rsId = rsId;
        _rsType = rsType;
    }

    /**
     * @param rsId
     *            Resource Identifier
     * @param rsType
     *            Resource Type
     * @param list
     *            List of ACEs
     */
    public ACL(String rsId, RsType rsType, List<ACE> list) {
        this(rsId, rsType);
        _list = list;
    }

    /**
     * @param rsId
     *            Resource Identifier
     * @param rsType
     *            Resource Type
     * @param ace
     *            only one ACE in ACEs list
     */
    public ACL(String rsId, RsType rsType, ACE ace) {
        this(rsId, rsType);
        _list = new ArrayList<ACE>(1);
        _list.add(ace);
    }

    /**
     * @return Returns list of ACEs
     */
    public List<ACE> getList() {
        return _list;
    }

    /**
     * Sets list of ACEs to ACL
     *
     * @param list
     *            List of ACEs
     */
    public void setList(List<ACE> list) {
        _list = list;
    }

    /**
     * @return Returns ACL resource identifier
     */
    public String getRsId() {
        return _rsId;
    }

    /**
     * Sets ACL resource identifier
     *
     * @param rsId
     *            ACL resource identifier
     */
    public void setRsId(String rsId) throws IllegalArgumentException {
        if (rsId == null || rsId.length() == 0)
            throw new IllegalArgumentException("rsId is " + (rsId == null ? "NULL" : "Empty"));

        _rsId = rsId;
    }

    /**
     * @return Returns ACL resource type
     */
    public RsType getRsType() {
        return _rsType;
    }

    /**
     * Sets ACL resource type
     *
     * @param rsType
     *            ACL resource type
     */
    public void setRsType(RsType rsType) throws IllegalArgumentException {
        if (rsType == null)
            throw new IllegalArgumentException("Invalid rsType.");

        _rsType = rsType;
    }

    /**
     * @return Returns the number of ACEs in ACL
     */
    public int getSize() {
        return _list.size();
    }

    /**
     * @return <code>true</code> if ACEs list is empty, otherwise <code>false</code>
     */
    public boolean isEmpty() {
        return _list.size() == 0;
    }

    public String toNFSv4String() {
        StringBuilder sb = new StringBuilder();
        sb.append(_rsId).append(SEPARATOR);
        sb.append(_rsType).append(SPACE_SEPARATOR);
        for (int index = 0; index < _list.size(); index++) {
            if (index > 0)
                sb.append(SPACE_SEPARATOR);
            sb.append(_list.get(index).toNFSv4String(_rsType));
        }
        return sb.toString();
    }

    public String toOrgString() {
        StringBuilder sb = new StringBuilder();
        sb.append(_rsId).append(SPACE_SEPARATOR);
        sb.append(_rsType.getValue()).append(LINE_SEPARATOR);
        for (ACE ace : _list)
            sb.append(ace.toOrgString()).append(LINE_SEPARATOR);
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
        sb.append(_rsId).append(SEPARATOR).append(_rsType);
        for (ACE ace : _list)
            sb.append(LINE_SEPARATOR).append(ace.toExtraFormat(_rsType));

        return sb.toString();
    }

    public String toString() {
        StringBuilder sb = new StringBuilder("ACL: ");
        sb.append("rsId = ").append(_rsId);
        sb.append(", rsType = ").append(_rsType);
        for (ACE ace : _list)
            sb.append(LINE_SEPARATOR).append(ace.toString(_rsType));

        return sb.toString();
    }
}
