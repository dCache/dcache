package org.dcache.acl.parser;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import org.dcache.acl.ACE;
import org.dcache.acl.enums.AccessMask;
import org.dcache.acl.enums.AceFlags;
import org.dcache.acl.enums.AceType;
import org.dcache.acl.enums.Who;

public class ACEParser {

    private static final String LINE_SEPARATOR = System.getProperty("line.separator", "\n");

    private static final String SPACE_SEPARATOR = " ";

    private static final String SEPARATOR = ":";

    private static final int ACES_MIN = 1;

    private static final int ACE_MIN = 3, ACE_MAX = 6;

    private static final int ACE_MIN_ADM = 2, ACE_MAX_ADM = 5;

    private static ACEParser _SINGLETON;
    static {
        _SINGLETON = new ACEParser();
    }

    private ACEParser() {
        super();
    }

    public static ACEParser instance() {
        return _SINGLETON;
    }

    /**
     * ace_spec format:
     * 	who[:who_id]:access_msk[:flags]:type[:address_msk]
     *
     * ace_spec examples:
     * 	USER:7:rlwfx:o:ALLOW:FFFF
     * 	EVERYONE@:w:DENY
     *
     * @param ace_spec
     *            String representation of ACE
     */
    public static ACE parse(String ace_spec) throws IllegalArgumentException {
        if ( ace_spec == null || ace_spec.length() == 0 ) {
            throw new IllegalArgumentException("ace_spec is " + (ace_spec == null ? "NULL" : "Empty"));
        }

        if ( ace_spec.endsWith(SEPARATOR) ) {
            throw new IllegalArgumentException("ace_spec ends with \"" + SEPARATOR + "\"");
        }

        String[] split = ace_spec.split(SEPARATOR);
        if ( split == null ) {
            throw new IllegalArgumentException("ace_spec can't be splitted.");
        }

        int len = split.length;
        if ( len < ACE_MIN || len > ACE_MAX ) {
            throw new IllegalArgumentException("Count tags invalid.");
        }

        int index = 0;

        String sWho = split[index++];
        Who who = Who.fromAbbreviation(sWho);
        if ( who == null ) {
            throw new IllegalArgumentException("Invalid who abbreviation: " + sWho);
        }

        int whoID = -1;
        if ( who == Who.USER || who == Who.GROUP ) {
            String sWhoID = split[index++];
            try {
                whoID = Integer.parseInt(sWhoID);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid whoID. NumberFormatException: " + e.getMessage());
            }
        }

        String sAccessMsk = split[index++];
        int accessMsk = AccessMask.parseInt(sAccessMsk);
        if ( accessMsk == 0 ) {
            throw new IllegalArgumentException("Invalid accessMask: " + sAccessMsk);
        }

        if ( index >= len ) {
            throw new IllegalArgumentException("Unspecified ACE type.");
        }

        String s = split[index++];
        int flags = 0;
        try {
            flags = AceFlags.parseInt(s);
            s = null;
        } catch (IllegalArgumentException Ignore) {
        }

        if ( s == null ) {
            if ( index >= len ) {
                throw new IllegalArgumentException("Unspecified ACE type.");
            }
            s = split[index++];
        }

        AceType type = AceType.fromAbbreviation(s);

        String addressMsk = ACE.DEFAULT_ADDRESS_MSK;
        if ( index < len ) {
            addressMsk = split[index++];

            try {
                new BigInteger(addressMsk, 16);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid addressMask. NumberFormatException: " + e.getMessage());
            }
        }
        if ( index != len ) {
            throw new IllegalArgumentException("Check index failure. Invalid ace_spec: " + ace_spec);
        }

        return new ACE(type, flags, accessMsk, who, whoID, addressMsk);
    }

    /**
     * ace_spec format:
     * 	who[:who_id]:+/-access_msk[:flags][:address_msk]
     *
     * ace_spec examples:
     * 	USER:3750:+rlx:o:FFFF
     * 	EVERYONE@:-w
     *
     * @param order
     *            ACE's order in list
     * @param ace_spec
     *            String representation of ACE (without 'order')
     */

    public static ACE parseAdmACE(String ace_spec) throws IllegalArgumentException {

        if ( ace_spec == null || ace_spec.length() == 0 ) {
            throw new IllegalArgumentException("ace_spec is " + (ace_spec == null ? "NULL" : "Empty"));
        }

        if ( ace_spec.endsWith(SEPARATOR) ) {
            throw new IllegalArgumentException("ace_spec ends with \"" + SEPARATOR + "\"");
        }

        String[] split = ace_spec.split(SEPARATOR);
        if ( split == null ) {
            throw new IllegalArgumentException("ace_spec can't be splitted.");
        }

        int len = split.length;
        if ( len < ACE_MIN_ADM || len > ACE_MAX_ADM ) {
            throw new IllegalArgumentException("Count tags invalid.");
        }

        int index = 0;
        String sWho = split[index++];
        Who who = Who.fromAbbreviation(sWho);
        if ( who == null ) {
            throw new IllegalArgumentException("Invalid who abbreviation: " + sWho);
        }

        int whoID = -1;
        if ( Who.USER.equals(who) || Who.GROUP.equals(who) ) {
            String sWhoID = split[index++];
            if ( index >= len ) {
                throw new IllegalArgumentException("Unspecified accessMask.");
            }

            try {
                whoID = Integer.parseInt(sWhoID);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid whoID. NumberFormatException: " + e.getMessage());
            }
        }

        String sAccessMsk = split[index++];
        AceType type;
        char operator = sAccessMsk.charAt(0);
        if ( operator == '+' ) {
            type = AceType.ACCESS_ALLOWED_ACE_TYPE;
        } else if ( operator == '-' ) {
            type = AceType.ACCESS_DENIED_ACE_TYPE;
        } else {
            throw new IllegalArgumentException("Invalid operator: \"" + operator + "\"");
        }

        int accessMsk = AccessMask.parseInt(sAccessMsk.substring(1));
        if ( accessMsk == 0 ) {
            throw new IllegalArgumentException("Invalid accessMask: " + sAccessMsk);
        }

        String addressMsk = ACE.DEFAULT_ADDRESS_MSK;
        int flags = 0;
        if ( index < len ) {
            String s = split[index++];
            if ( s.trim().length() == 0 ) {
                throw new IllegalArgumentException("ACE flags is Empty.");
            }

            try {
                flags = AceFlags.parseInt(s);
                s = null;
            } catch (IllegalArgumentException Ignore) {
            }

            if ( s == null && index < len ) {
                s = split[index++];
            }

            if ( s != null ) {
                try {
                    new BigInteger(s, 16);
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Invalid addressMask. NumberFormatException: " + e.getMessage());
                }
                addressMsk = s;
            }
        }

        if ( index != len ) {
            throw new IllegalArgumentException("Check index failure. Invalid ace_spec: " + ace_spec);
        }

        return new ACE(type, flags, accessMsk, who, whoID, addressMsk);
    }

    /**
     * aces_spec format:
     * 	who[:who_id]:+/-access_msk[:flags][:address_msk] who[:who_id]:+/-access_msk[:flags][:address_msk]
     *
     * aces_spec example:
     * 	USER:3750:+rlx:o:FFFF EVERYONE@:-w
     *
     * @param aces_spec
     *            String representation of ACEs
     */
    public static List<ACE> parseAdm(String aces_spec) throws IllegalArgumentException {
        if ( aces_spec == null || aces_spec.length() == 0 ) {
            throw new IllegalArgumentException("aces_spec is " + (aces_spec == null ? "NULL" : "Empty"));
        }

        String[] split = aces_spec.split(SPACE_SEPARATOR);
        if ( split == null ) {
            throw new IllegalArgumentException("aces_spec can't be splitted.");
        }

        int len = split.length;
        if ( len < ACES_MIN ) {
            throw new IllegalArgumentException("Count ACEs invalid.");
        }

        List<ACE> aces = new ArrayList<>(len);
        for (String ace: split) {
            aces.add(ACEParser.parseAdmACE(ace));
        }

        return aces;
    }

}
