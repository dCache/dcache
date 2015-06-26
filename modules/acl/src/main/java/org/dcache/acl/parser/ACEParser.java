package org.dcache.acl.parser;

import com.google.common.base.Splitter;
import java.util.ArrayList;
import java.util.List;

import org.dcache.acl.ACE;
import org.dcache.acl.enums.AccessMask;
import org.dcache.acl.enums.AceFlags;
import org.dcache.acl.enums.AceType;
import org.dcache.acl.enums.Who;

import static com.google.common.base.Preconditions.checkArgument;

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
     * 	who[:who_id]:access_msk[:flags]:type
     *
     * ace_spec examples:
     * 	USER:7:rlwfx:o:ALLOW
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

        if (ace_spec.contains(":+") || ace_spec.contains(":-")) {
            return parseAdmACE(ace_spec);
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
        if ( index != len ) {
            throw new IllegalArgumentException("Check index failure. Invalid ace_spec: " + ace_spec);
        }

        return new ACE(type, flags, accessMsk, who, whoID);
    }

    /**
     * ace_spec format:
     * 	who[:who_id]:+/-access_msk[:flags]
     *
     * ace_spec examples:
     * 	USER:3750:+rlx:o
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
        }

        if ( index != len ) {
            throw new IllegalArgumentException("Check index failure. Invalid ace_spec: " + ace_spec);
        }

        return new ACE(type, flags, accessMsk, who, whoID);
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

    /**
     * Create an {@link ACE} from string representation. The string
     * must be in linux nfs4_acl format. The format refined as:
     *
     * <pre>
     *        4-field string in the following format:
     *                <i>type</i>:<i>flags</i>:<i>principal</i>:<i>permissions</i>
     *     <b>ACE</b> <b>TYPES:</b>
     *         There are four <i>types</i> of ACEs, each represented by a  single  character.
     *         An ACE must have exactly one <i>type</i>.
     *         <b>A</b>      Allow  -	allow  <i>principal</i>  to perform actions requiring <i>permis-</i>
     *                <i>sions</i>.
     *         <b>D</b>      Deny - prevent <i>principal</i> from performing actions requiring  <i>per-</i>
     *                <i>missions</i>.
     *         <b>U</b>      Audit  -	log  any  attempted access by <i>principal</i> which requires
     *                <i>permissions</i>.  Requires one or both of the successful-access  and
     *                failed-access  <i>flags</i>.   System-dependent;  not  supported by all
     *                servers.
     *         <b>L</b>      Alarm - generate a system alarm at any attempted access by <i>prin-</i>
     *                <i>cipal</i>  which  requires <i>permissions</i>.  Requires one or both of the
     *                successful-access and  failed-access  <i>flags</i>.   System-dependent;
     *                not supported by all servers.
     *     <b>ACE</b> <b>FLAGS:</b>
     *         There are three kinds of ACE <i>flags</i>: group, inheritance, and administra-
     *         tive.  An Allow or Deny ACE may contain zero or more  <i>flags</i>,  while  an
     *         Audit  or  Alarm ACE must contain at least one of the successful-access
     *         and failed-access <i>flags</i>.
     *         Note that ACEs are inherited from the parent  directory's  ACL  at  the
     *         time a file or subdirectory is created.	Accordingly, inheritance flags
     *         can be used only in ACEs  in  a	directory's  ACL  (and	are  therefore
     *         stripped  from  inherited  ACEs	in  a new file's ACL).	Please see the
     *         <b>INHERITANCE</b> <b>FLAGS</b> <b>COMMENTARY</b> section for more information.
     *         <b>GROUP</b> <b>FLAG</b> - can be used in any ACE
     *         <b>g</b>      group - indicates that <i>principal</i> represents a group instead of a
     *                user.
     *         <b>INHERITANCE</b> <b>FLAGS</b> - can be used in any directory ACE
     *         <b>d</b>      directory-inherit  -  newly-created  subdirectories will inherit
     *                the ACE.
     *         <b>f</b>      file-inherit - newly-created files will inherit the  ACE,  minus
     *                its   inheritance   <i>flags</i>.   Newly-created  subdirectories  will
     *                inherit the ACE; if directory-inherit is not also  specified  in
     *                the parent ACE, inherit-only will be added to the inherited ACE.
     *         <b>n</b>      no-propagate-inherit - newly-created subdirectories will inherit
     *                the ACE, minus its inheritance <i>flags</i>.
     *         <b>i</b>      inherit-only  - the ACE is not considered in permissions checks,
     *                but it is heritable; however, the inherit-only <i>flag</i> is  stripped
     *                from inherited ACEs.
     *         <b>ADMINISTRATIVE</b> <b>FLAGS</b> - can be used in <b>Audit</b> and <b>Alarm</b> ACEs
     *         <b>S</b>      successful-access  -  trigger  an  alarm/audit when <i>principal</i> is
     *                allowed to perform an action covered by <i>permissions</i>.
     *         <b>F</b>      failed-access - trigger an alarm/audit when  <i>principal</i>  is  pre-
     *                vented from performing an action covered by <i>permissions</i>.
     *     <b>ACE</b> <b>PRINCIPALS:</b>
     *         A  <i>principal</i>  is  either a named user (e.g., `myuser@nfsdomain.org') or
     *         group (provided the group <i>flag</i> is also set), or one  of	three  special
     *         <i>principals</i>:  `OWNER@',  `GROUP@',  and  `EVERYONE@', which are, respec-
     *         tively, analogous to the POSIX user/group/other distinctions  used  in,
     *         e.g., <a href="?query=chmod&amp;sektion=1&amp;apropos=0&amp;manpath=CentOS+Linux%2famd64+6.3"><b>chmod</b>(1)</a>.
     *     <b>ACE</b> <b>PERMISSIONS:</b>
     *         There  are a variety of different ACE <i>permissions</i> (13 for files, 14 for
     *         directories), each represented by a single character.   An  ACE	should
     *         have one or more of the following <i>permissions</i> specified:
     *         <b>r</b>      read-data (files) / list-directory (directories)
     *         <b>w</b>      write-data (files) / create-file (directories)
     *         <b>a</b>      append-data (files) / create-subdirectory (directories)
     *         <b>x</b>      execute (files) / change-directory (directories)
     *         <b>d</b>      delete  -  delete the file/directory.  Some servers will allow a
     *                delete to  occur	if  either  this  <i>permission</i>  is  set  in  the
     *                file/directory  or  if the delete-child <i>permission</i> is set in its
     *                parent direcory.
     *         <b>D</b>      delete-child - remove a file or  subdirectory  from  within  the
     *                given directory (directories only)
     *         <b>t</b>      read-attributes - read the attributes of the file/directory.
     *         <b>T</b>      write-attributes - write the attributes of the file/directory.
     *         <b>n</b>      read-named-attributes   -  read  the  named  attributes  of  the
     *                file/directory.
     *         <b>N</b>      write-named-attributes -	write  the  named  attributes  of  the
     *                file/directory.
     *         <b>c</b>      read-ACL - read the file/directory NFSv4 ACL.
     *         <b>C</b>      write-ACL - write the file/directory NFSv4 ACL.
     *         <b>o</b>      write-owner - change ownership of the file/directory.
     *         <b>y</b>      synchronize  -  allow  clients  to  use synchronous I/O with the
     *                server.
     *
     *         (from original man page by David Richter and J. Bruce Fields)
     * </pre>
     * @param s ace in linux nfs4_acl format
     * @return ACE represented by the provided string
     * @throws IllegalArgumentException if ACE can't be parsed
     */
    public static ACE parseLinuxAce(String s) throws IllegalArgumentException {
        Splitter splitter = Splitter.on(':');
        List<String> splitted = splitter.splitToList(s);

        checkArgument(splitted.size() == 4, "Invalid ACE format: expected <type:flags:principal:permissions> got: <" + s +">");

        checkArgument(splitted.get(0).length() == 1, "Invalid ACE format: type must be a single character. Got : <" + splitted.get(0).length() + ">");
        AceType type = AceType.fromAbbreviation(splitted.get(0).charAt(0));
        int flags = 0;
        int accessMask = 0;
        Who who;
        int id = -1;
        String principal;

        for(char c: splitted.get(1).toCharArray()) {
            flags |= AceFlags.fromAbbreviation(c).getValue();
        }

        principal = splitted.get(2);
        if (principal.charAt(principal.length() -1) == '@') {
            who = Who.fromAbbreviation(principal);
        } else {
            who = (flags & AceFlags.IDENTIFIER_GROUP.getValue()) == 0 ? Who.USER : Who.GROUP;
            id = Integer.parseInt(principal);
        }

        for (char c : splitted.get(3).toCharArray()) {
            accessMask |= AccessMask.fromAbbreviation(c).getValue();
        }

        return new ACE(type, flags,  accessMask, who, id);
    }
}
