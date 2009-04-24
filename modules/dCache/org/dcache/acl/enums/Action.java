package org.dcache.acl.enums;

/**
 * Enumeration of context handler depended actions.
 *
 * @author David Melkumyan, DESY Zeuthen
 *
 */
public enum Action {
    /**
     * Check access permission. ACE flags: ADD FILE, ADD SUBDIRECTORY
     */
    ACCESS(3),

    /**
     * Create a Non-Regular File Object. ACE flags: ADD FILE, ADD SUBDIRECTORY
     */
    CREATE(6),

    /**
     * Get Attributes. ACE flags: READ ACL, READ ATTRIBUTES
     */
    GETATTR(9),

    /**
     * Create link to a file. ACE flags: ADD FILE
     */
    LINK(11),

    /**
     * Lookup filename. ACE flags: EXECUTE
     */
    LOOKUP(15),

    /**
     * Lookup parent directory. ACE flags: EXECUTE
     */
    LOOKUPP(16),

    /**
     * Open a regular file. ACE flags: ADD FILE, APPEND DATA, EXECUTE, READ DATA, WRITE DATA
     * <dl>Notes:<ul>
     * <li>that append operations might not be supported at all.
     * <li>OPEN operation MUST be used to create a regular file or a named attribute.
     */
    OPEN(18),

    /**
     * Read from a file. ACE flags : EXECUTE, READ DATA
     */
    READ(25),

    /**
     * Read directory. ACE flags : LIST DIRECTORY
     */
    READDIR(26),

    /**
     * Read symbolic link. ACE flags : EXECUTE
     */
    READLINK(27),

    /**
     * Remove file system object. ACE flags : DELETE, DELETE CHILD Comments : Need both the right
     * for object to delete it and for parent to delete a child.
     */
    REMOVE(28),

    /**
     * Rename directory entry. ACE flags : DELETE CHILD, ADD FILE Comments : Rename operation is
     * interpreted as composed operation: delete old child and add new child.
     */
    RENAME(29),

    /**
     * Set attributes. ACE flags: APPEND DATA, WRITE ATTRIBUTES, WRITE ACL, WRITE DATA, WRITE OWNER
     * Comments: Which access flags is relevant depends on attribute which is modified: Note: that
     * setting attribute ARCHIVE and SUPPORTED ATTRS (aka SUPP ATTRS) is not defined in NFS4.1 draft
     * 13.
     *
     */
    SETATTR(34),

    /**
     * Write to file. ACE flags: APPEND DATA, WRITE DATA Comments: Currently only WRITE DATA is
     * checked
     */
    WRITE(38);

    private final int _value;

    private Action(int value) {
        _value = value;
    }

    public int getValue() {
        return _value;
    }

    public boolean equals(int value) {
        return _value == value;
    }
}
