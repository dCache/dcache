package org.dcache.acl.parser;

import java.util.ArrayList;
import java.util.List;

import org.dcache.acl.ACE;
import org.dcache.acl.ACL;
import org.dcache.acl.enums.RsType;

public class ACLParser {

    private static final String LINE_SEPARATOR = System.getProperty("line.separator", "\n");

    private static final String SEPARATOR = ":";

    private static ACLParser _SINGLETON;
    static {
        _SINGLETON = new ACLParser();
    }

    private ACLParser() {
        super();
    }

    public static ACLParser instance() {
        return _SINGLETON;
    }

    /**
     * format:
     * 	rs_id:rs_type
     * 	who[:who_id]:access_msk[:flags]:type[:address_msk]
     * 	who[:who_id]:access_msk[:flags]:type[:address_msk]
     *
     * acl_spec example:
     * 	USER:7:rlwfx:o:ALLOW:FFFF
     * 	EVERYONE@:w:DENY
     *
     * @param acl_spec
     *            String representation of ACL
     * @return List of ACEs
     */
    public static ACL parse(String acl_spec) throws IllegalArgumentException {
        if ( acl_spec == null || acl_spec.length() == 0 ) {
            throw new IllegalArgumentException("Invalid acl_spec.");
        }

        String[] split = acl_spec.split(LINE_SEPARATOR);
        if ( split == null ) {
            throw new IllegalArgumentException("acl_spec can't be splitted: " + acl_spec);
        }

        int len = split.length;
        if ( len < 2 ) {
            throw new IllegalArgumentException("Count tags invalid in acl_spec: " + acl_spec);
        }

        String rsInfo = split[0];
        if ( rsInfo.length() == 0 ) {
            throw new IllegalArgumentException("Invalid acl_spec: " + acl_spec);
        }

        String[] splitRsInfo = rsInfo.split(SEPARATOR);
        if ( splitRsInfo.length != 2 ) {
            throw new IllegalArgumentException("Invalid acl_spec: " + acl_spec);
        }

        String rsID = splitRsInfo[0];
        if ( rsID.length() == 0 ) {
            throw new IllegalArgumentException("Invalid acl_spec: " + acl_spec);
        }

        RsType rsType = RsType.valueOf(splitRsInfo[1]);

        List<ACE> aces = new ArrayList<>();
        for (int index = 1; index < len; index++) {
            aces.add(ACEParser.parse(split[index]));
        }

        return new ACL(rsType, aces);
    }

    /**
     * aces_spec format:
     * 	who[:who_id]:+/-access_msk[:flags][:address_msk] who[:who_id]:+/-access_msk[:flags][:address_msk]
     *
     * aces_spec example:
     * 	USER:3750:+rlx:o:FFFF EVERYONE@:-w
     *
     * @param rsID
     *            resource ID
    * @param rsType
     *            resource type
     * @param aces_spec
     *            String representation of ACEs
     * @return Access Control Entry object
     */
    public static ACL parseAdm(RsType rsType, String aces_spec) throws IllegalArgumentException {
        return new ACL(rsType, ACEParser.parseAdm(aces_spec));
    }
}
