package org.dcache.acl;

import org.dcache.acl.enums.*;
import org.dcache.acl.mapper.AclMapper;
import org.dcache.acl.matcher.AclNFSv4Matcher;
import org.dcache.auth.GidPrincipal;
import org.dcache.auth.Origin;
import org.dcache.auth.UidPrincipal;
import org.junit.Test;

import javax.security.auth.Subject;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * @author Irina Kozlova, David Melkumyan
 *
 */
public class ACLTest {

    private static final int GID = 100;
    private static final int UID_1 = 111;
    private static final int UID_2 = 222;
    private static final int UID_3 = 1001;

    @Test
    public void testAcl() throws Exception {

        RsType rsType = RsType.FILE;

        int masks1 = (AccessMask.READ_DATA.getValue());
        masks1 |= (AccessMask.WRITE_DATA.getValue());
        masks1 |= (AccessMask.EXECUTE.getValue());

        List<ACE> aces = new ArrayList<ACE>();
        // EXAMPLE: 0.ACE allow READ_DATA
        // 1.ACE allow READ_DATA, WRITE_DATA, EXECUTE
        // 2.ACE deny READ_DATA
        //
        // EXPECTED: action READ is allowed (as first READ_DATA - in ACE:0 - is
        // allowed)
        // action WRITE is allowed (as WRITE_DATA is allowed)
        // action REMOVE is undefined


        aces.add(new ACE(AceType.ACCESS_ALLOWED_ACE_TYPE, 0,
                AccessMask.READ_DATA.getValue(), Who.USER, 7,
                ACE.DEFAULT_ADDRESS_MSK));

        aces.add(new ACE(AceType.ACCESS_ALLOWED_ACE_TYPE, 0, masks1, Who.USER,
                7, ACE.DEFAULT_ADDRESS_MSK));

        aces.add(new ACE(AceType.ACCESS_DENIED_ACE_TYPE, 0,
                AccessMask.READ_DATA.getValue(), Who.USER, 7,
                ACE.DEFAULT_ADDRESS_MSK));
        //for another user:
        aces.add(new ACE(AceType.ACCESS_ALLOWED_ACE_TYPE, 0,
                AccessMask.DELETE.getValue(), Who.USER, 777,
                ACE.DEFAULT_ADDRESS_MSK));

        ACL newACL = new ACL(rsType, aces);

        // Create test user subjectNew. who_id=7 as above.
        Subject subjectNew = new Subject();
        subjectNew.getPrincipals().add(new UidPrincipal(7));
        subjectNew.getPrincipals().add(new GidPrincipal(100, true));

        Origin originNew = new Origin(Origin.AuthType.ORIGIN_AUTHTYPE_WEAK, "127.0.0.1");

        Owner ownerNew = new Owner(0, 0);

        Permission permissionNew = AclMapper.getPermission(subjectNew,
                originNew, ownerNew, newACL);

        //NEW 15.05.2008 permission for parent directory (in order to check 'remove this file from parent directory')
        RsType parentRsType = RsType.DIR;
        List<ACE> parentAces = new ArrayList<ACE>();
        parentAces.add(new ACE(AceType.ACCESS_ALLOWED_ACE_TYPE, 1,
                AccessMask.DELETE_CHILD.getValue(), Who.USER, 7,
                ACE.DEFAULT_ADDRESS_MSK));
        //for another user:
        parentAces.add(new ACE(AceType.ACCESS_ALLOWED_ACE_TYPE, 1,
                AccessMask.DELETE_CHILD.getValue(), Who.USER, 777,
                ACE.DEFAULT_ADDRESS_MSK));
        ACL parentACL = new ACL(parentRsType, parentAces);

        Permission permissionNewParentDir = AclMapper.getPermission(subjectNew,
                originNew, ownerNew, parentACL);
        //create another user (he is allowed to REMOVE file):
        Subject subjectNewUser777 = new Subject();
        subjectNewUser777.getPrincipals().add(new UidPrincipal(777));
        subjectNewUser777.getPrincipals().add(new GidPrincipal(100, true));

        //permissions of user 777 on parent directory
        Permission permissionParentDirUser777 = AclMapper.getPermission(subjectNewUser777,
                originNew, ownerNew, parentACL);
        //permissions of user 777 on file in this directory
        Permission permissionUser777 = AclMapper.getPermission(subjectNewUser777,
                originNew, ownerNew, newACL);
        //<-- end NEW

        // Action READ.
        Action actionREAD = Action.READ;
        Boolean check1 = AclNFSv4Matcher.isAllowed(permissionNew, actionREAD);
        assertTrue("user with who_id=7 is allowed to READ file", check1);

        // Action WRITE.
        Action actionWRITE = Action.WRITE;
        Boolean check2 = AclNFSv4Matcher.isAllowed(permissionNew, actionWRITE);
        assertTrue(
                "user with who_id=7 is allowed to WRITE file as WRITE_DATA is allowed",
                check2);

        // Action REMOVE. Allowed, because parent directory has DELETE_CHILD.
        Action actionREMOVE = Action.REMOVE;
        //use Boolean isAllowed(Permission perm1, Permission perm2, Action action, Boolean isDir)
        //where perm1 - permission for parent directory, perm2 - permission for child (file in this case)
        Boolean check3 = AclNFSv4Matcher.isAllowed(permissionNewParentDir, permissionNew, actionREMOVE,
                Boolean.FALSE);
        assertTrue("user who_id=7, action REMOVE is allowed", check3);

        // Action REMOVE. Allowed to remove file (For user 777, file: DELETE allowed, parentdir: DELETE_CHILD allowed).
        //use: Boolean isAllowed(Permission perm1, Permission perm2, Action action, Boolean isDir)
        //where perm1 - permission for parent directory, perm2 - permission for child (file in this case)
        Boolean check3a = AclNFSv4Matcher.isAllowed(permissionParentDirUser777, permissionUser777, actionREMOVE,
                Boolean.FALSE);
        assertTrue("user who_id=777, action REMOVE file is allowed ", check3a);


        // ALSO CHECK LOOUP "Lookup filename". Bit to check: EXECUTE
        Action actionLOOKUP = Action.LOOKUP;
        Boolean checkLOOKUP = AclNFSv4Matcher.isAllowed(permissionNew,
                actionLOOKUP);
        assertTrue(
                "user who_id=7 is allowed to LOOKUP filename as bit EXECUTE is allowed ",
                checkLOOKUP);

    }

    // ///////////////////////////////////////////
    @Test
    public void testAclOpen() throws Exception {

        // EXAMPLE.
        // action: OPEN (open a regular file)
        // ACE flags: ADD_FILE, APPEND_DATA, EXECUTE, READ_DATA, WRITE_DATA

        RsType rsType = RsType.FILE;

        int mask5 = (AccessMask.ADD_FILE.getValue());
        mask5 |= (AccessMask.APPEND_DATA.getValue());
        mask5 |= (AccessMask.EXECUTE.getValue());
        mask5 |= (AccessMask.READ_DATA.getValue());
        mask5 |= (AccessMask.WRITE_DATA.getValue());

        List<ACE> aces = new ArrayList<ACE>();

        aces.add(new ACE(AceType.ACCESS_ALLOWED_ACE_TYPE, 0, mask5, Who.USER,
                1000, ACE.DEFAULT_ADDRESS_MSK));

        aces.add(new ACE(AceType.ACCESS_DENIED_ACE_TYPE, 0,
                AccessMask.READ_DATA.getValue(), Who.USER, 1000,
                ACE.DEFAULT_ADDRESS_MSK));

        ACL newACL = new ACL(rsType, aces);

        // Create test user subjectNew. who_id=1000 as above.
        Subject subjectNew = new Subject();
        subjectNew.getPrincipals().add(new UidPrincipal(1000));
        subjectNew.getPrincipals().add(new GidPrincipal(100, true));

        Origin originNew = new Origin(Origin.AuthType.ORIGIN_AUTHTYPE_STRONG,
                "127.0.0.1");

        Owner ownerNew = new Owner(0, 0);

        Permission permissionNew = AclMapper.getPermission(subjectNew,
                originNew, ownerNew, newACL);

        // /////////////////////////////////////////////////////////////////////////////
        // Bits ADD_FILE, EXECUTE, READ_DATA, WRITE_DATA are allowed as defined
        // in mask5.
        // ////////////////////////////////////////////////////////////////////////////

        // Action LINK.
        Action actionLINK = Action.LINK;
        Boolean checkLINK = AclNFSv4Matcher.isAllowed(permissionNew, actionLINK);
        assertTrue(
                "For user who_id=1000 action LINK is allowed as bit ADD_FILE is set to allow",
                checkLINK);

        // Action WRITE
        Action actionWRITE = Action.WRITE;
        Boolean checkWRITE = AclNFSv4Matcher.isAllowed(permissionNew,
                actionWRITE);
        assertTrue(
                "For user who_id=1000 action WRITE is allowed as bit WRITE_DATA is allowed. APPEND_DATA is not checked for now.",
                checkWRITE);

        // Action READ
        Action actionREAD = Action.READ;
        Boolean checkREAD = AclNFSv4Matcher.isAllowed(permissionNew, actionREAD);
        assertTrue(
                "For user who_id=1000 action READ is allowed as bits EXECUTE, READ_DATA are allowed",
                checkREAD);

        // Action READLINK
        Action actionREADLINK = Action.READLINK;
        Boolean checkREADLINK = AclNFSv4Matcher.isAllowed(permissionNew,
                actionREADLINK);
        boolean isAllowedOrNot2 = (checkREADLINK != null && checkREADLINK == Boolean.TRUE);
        assertTrue(
                "For user who_id=1000 action READLINK is allowed as bit EXECUTE is allowed",
                isAllowedOrNot2);

        // //////////////////////////////////////////////////////////////////////
        // Action OPEN
        Action actionOPEN = Action.OPEN;
        //USE: Boolean isAllowed(Permission perm, Action action, OpenType opentype)
        Boolean checkOPEN = AclNFSv4Matcher.isAllowed(permissionNew,
                actionOPEN, OpenType.OPEN4_NOCREATE);
        assertTrue(
                "user who_id=1000 is allowed to OPEN file as READ_DATA in mask5 is allowed",
                checkOPEN);

        // //////////////////////////////////////////////////////////////////////
        // Action RENAME.
        //Action actionRENAME = Action.RENAME;
        // USE METHOD: isAllowed(Permission perm, Action action, Boolean isDir)
        //Boolean checkRENAME = AclNFSv4Matcher.isAllowed(permissionNew,
        //        actionRENAME, Boolean.FALSE);
        //assertTrue(
        //        "user who_id=1000, action RENAME is allowed as bits DELETE_CHILD and ADD_FILE are allowed",
        //        checkRENAME);

        // ALSO CHECK LOOUP "Lookup filename". Bit to check: EXECUTE
        Action actionLOOKUP = Action.LOOKUP;
        Boolean checkLOOKUP = AclNFSv4Matcher.isAllowed(permissionNew,
                actionLOOKUP);
        assertTrue(
                "user who_id=1000 is allowed to LOOKUP filename as bit EXECUTE is allowed ",
                checkLOOKUP);

    }

    // ///////////////////////////////////////////
    @Test
    public void testCREATEdirAllowDeny_testLINK() throws Exception {

        //ask for permission to create sub-directory in parent directory (having parentACL)
        RsType parentRsType = RsType.DIR;

        List<ACE> parentAces = new ArrayList<ACE>();

        parentAces.add(new ACE(AceType.ACCESS_ALLOWED_ACE_TYPE, 1,
                AccessMask.ADD_SUBDIRECTORY.getValue(), Who.USER, 111,
                ACE.DEFAULT_ADDRESS_MSK));
        parentAces.add(new ACE(AceType.ACCESS_DENIED_ACE_TYPE, 1,
                AccessMask.ADD_SUBDIRECTORY.getValue(), Who.USER, 222,
                ACE.DEFAULT_ADDRESS_MSK));
        parentAces.add(new ACE(AceType.ACCESS_ALLOWED_ACE_TYPE, 1,
                AccessMask.ADD_FILE.getValue(), Who.USER, 333,
                ACE.DEFAULT_ADDRESS_MSK));

        ACL parentACL = new ACL(parentRsType, parentAces);

        Subject subjectNew = new Subject();
        subjectNew.getPrincipals().add(new UidPrincipal(UID_1));
        subjectNew.getPrincipals().add(new GidPrincipal(100, true));

        Origin originNew = new Origin(Origin.AuthType.ORIGIN_AUTHTYPE_STRONG,
                "127.0.0.1");

        Owner ownerNew = new Owner(0, 0);

        //permission for user 111 on object 'parent directory'
        Permission permissionNew = AclMapper.getPermission(subjectNew,
                originNew, ownerNew, parentACL);

        Action actionCREATE = Action.CREATE;
        // USE: isAllowed(Permission perm, Action action, Boolean isDir)
        Boolean checkCREATE = AclNFSv4Matcher.isAllowed(permissionNew,
                actionCREATE, Boolean.TRUE);
        assertTrue(
                "User who_id=111 is allowed to create a new directory in parent directory (as bit ADD_SUBDIRECTORY is allowed fro parent directory)",
                checkCREATE);

        //user 222 is NOT ALLOWED to create a new directory in this parent directory,
        //as bit ADD_SUBDIRECTORY is denied for this user on 'parent directory'
        Subject subjectUser222 = new Subject();
        subjectUser222.getPrincipals().add(new UidPrincipal(UID_2));
        subjectUser222.getPrincipals().add(new GidPrincipal(100, true));

        Permission permissionUser222 = AclMapper.getPermission(subjectUser222,
                originNew, ownerNew, parentACL);

        Boolean checkCREATE2 = AclNFSv4Matcher.isAllowed(permissionUser222,
                actionCREATE, Boolean.TRUE);
        assertFalse(
                "User who_id=222 is NOT allowed to create a new directory in parent directory (as bit ADD_SUBDIRECTORY is DENIED for parent directory)",
                checkCREATE2);


        //for user 333 it is UNDEFINED whether he can create a new directory in this parent directory,
        //as only bit ADD_FILE is allowed for this user on 'parent directory', and ADD_SUBDIRECTORY is not defined.
        //That is action CREATE directory is UNDEFINED for this user.
        Subject subjectUser333 = new Subject();
        subjectUser333.getPrincipals().add(new UidPrincipal(333));
        subjectUser333.getPrincipals().add(new GidPrincipal(100, true));

        Permission permissionUser333 = AclMapper.getPermission(subjectUser333,
                originNew, ownerNew, parentACL);

        Boolean checkCREATE3 = AclNFSv4Matcher.isAllowed(permissionUser333,
                actionCREATE, Boolean.TRUE);
        assertNull(
                "User who_id=222 is NOT allowed to create a new directory in parent directory (as bit ADD_SUBDIRECTORY is DENIED for parent directory)",
                checkCREATE3);


        // ALSO CHECK action LINK for user 333. Bit to check on parent directory: ADD_FILE.
        Action actionLINK = Action.LINK;
        // USE: isAllowed(Permission perm, Action action)
        Boolean checkLINK1 = AclNFSv4Matcher.isAllowed(permissionUser333,
                actionLINK);
        assertTrue(
                "For user who_id=1001 action LINK is allowed: bit ADD_FILE is allowed",
                checkLINK1);



        /*
        // ALSO CHECK LOOKUPP "Lookup parent directory". Bit to check: EXECUTE
        Action actionLOOKUPP = Action.LOOKUPP;
        // USE: isAllowed(Permission perm, Action action)
        Boolean checkLOOKUPP = AclNFSv4Matcher.isAllowed(permissionNew,
        actionLOOKUPP);
        assertTrue(
        "user who_id=7 is allowed to LOOKUPP filename as bit EXECUTE is allowed ",
        checkLOOKUPP);

        // ALSO CHECK READLINK "Read symbolic link". Bit to check: EXECUTE
        Action actionREADLINK = Action.READLINK;
        Boolean checkREADLINK = AclNFSv4Matcher.isAllowed(permissionNew,
        actionREADLINK);
        boolean isAllowedOrNot = (checkREADLINK != null && checkREADLINK.equals( Boolean.TRUE) );
        assertTrue(
        "user who_id=1001 is allowed to READLINK : bit EXECUTE is allowed ",
        isAllowedOrNot);

         */
    }
    // ///////////////////////////////////////////

    @Test
    public void testCREATEfileAllow() throws Exception {

        RsType rsType = RsType.FILE;

        List<ACE> aces = new ArrayList<ACE>();

        aces.add(new ACE(AceType.ACCESS_DENIED_ACE_TYPE, 0,
                AccessMask.ADD_SUBDIRECTORY.getValue(), Who.USER, 1001,
                ACE.DEFAULT_ADDRESS_MSK));

        aces.add(new ACE(AceType.ACCESS_ALLOWED_ACE_TYPE, 0,
                AccessMask.ADD_FILE.getValue(), Who.USER, 1001,
                ACE.DEFAULT_ADDRESS_MSK));

        ACL newACL = new ACL(rsType, aces);

        Subject subjectNew = new Subject();
        subjectNew.getPrincipals().add(new UidPrincipal(UID_3));
        subjectNew.getPrincipals().add(new GidPrincipal(100, true));

        Origin originNew = new Origin(Origin.AuthType.ORIGIN_AUTHTYPE_STRONG,
                "127.0.0.1");

        Owner ownerNew = new Owner(0, 0);

        Permission permissionNew = AclMapper.getPermission(subjectNew,
                originNew, ownerNew, newACL);

        Action actionCREATE = Action.CREATE;
        // USE: isAllowed(Permission perm, Action action, Boolean isDir)
        Boolean checkCREATEfileAllow = AclNFSv4Matcher.isAllowed(permissionNew,
                actionCREATE, Boolean.FALSE);
        assertTrue(
                "For user who_id=1001 action CREATE (create new FILE) is allowed: ADD_FILE is allowed",
                checkCREATEfileAllow);

        // ALSO CHECK action LINK. Bit to check: ADD_FILE is allowed.
        Action actionLINK3 = Action.LINK;
        // USE: isAllowed(Permission perm, Action action)
        Boolean checkLINKallow = AclNFSv4Matcher.isAllowed(permissionNew,
                actionLINK3);
        assertTrue(
                "For user who_id=1001 action LINK is allowed: bit ADD_FILE is allowed",
                checkLINKallow);

    }

    // ///////////////////////////////////////////
    @Test
    public void testCREATEfileDeny() throws Exception {

        RsType rsType = RsType.FILE;

        List<ACE> aces = new ArrayList<ACE>();

        aces.add(new ACE(AceType.ACCESS_DENIED_ACE_TYPE, 0, AccessMask.ADD_FILE.getValue(), Who.USER, 1001, ACE.DEFAULT_ADDRESS_MSK));

        aces.add(new ACE(AceType.ACCESS_ALLOWED_ACE_TYPE, 0,
                AccessMask.ADD_SUBDIRECTORY.getValue(), Who.USER, 1001,
                ACE.DEFAULT_ADDRESS_MSK));

        ACL newACL = new ACL(rsType, aces);

        Subject subjectNew = new Subject();
        subjectNew.getPrincipals().add(new UidPrincipal(UID_3));
        subjectNew.getPrincipals().add(new GidPrincipal(100, true));

        Origin originNew = new Origin(Origin.AuthType.ORIGIN_AUTHTYPE_STRONG,
                "127.0.0.1");

        Owner ownerNew = new Owner(0, 0);

        Permission permissionNew = AclMapper.getPermission(subjectNew,
                originNew, ownerNew, newACL);

        Action actionCREATE = Action.CREATE;
        // USE: isAllowed(Permission perm, Action action, Boolean isDir)
        Boolean checkCREATE3 = AclNFSv4Matcher.isAllowed(permissionNew,
                actionCREATE, Boolean.FALSE);
        assertFalse(
                "For user who_id=1001 action CREATE (create new FILE) is denied: ADD_FILE is set to deny",
                checkCREATE3);

        // ALSO CHECK action LINK. Bit to check: ADD_FILE is denied.
        Action actionLINK2 = Action.LINK;
        // USE: isAllowed(Permission perm, Action action)
        Boolean checkLINK2 = AclNFSv4Matcher.isAllowed(permissionNew,
                actionLINK2);
        assertFalse(
                "For user who_id=1001 action LINK is denied: bit ADD_FILE is denied",
                checkLINK2);

    }

    // ///////////////////////////////////////////
    @Test
    public void testREADallow() throws Exception {

        RsType rsType = RsType.FILE;

        List<ACE> aces = new ArrayList<ACE>();

        aces.add(new ACE(AceType.ACCESS_ALLOWED_ACE_TYPE, 0, AccessMask.EXECUTE.getValue() + AccessMask.READ_DATA.getValue(), Who.USER, 1001,
                ACE.DEFAULT_ADDRESS_MSK));

        ACL newACL = new ACL(rsType, aces);

        Subject subjectNew = new Subject();
        subjectNew.getPrincipals().add(new UidPrincipal(UID_3));
        subjectNew.getPrincipals().add(new GidPrincipal(100, true));

        Origin originNew = new Origin(Origin.AuthType.ORIGIN_AUTHTYPE_STRONG,
                "127.0.0.1");

        Owner ownerNew = new Owner(0, 0);

        Permission permissionNew = AclMapper.getPermission(subjectNew,
                originNew, ownerNew, newACL);

        // Check READ. Bits: EXECUTE,READ_DATA
        Action actionREAD = Action.READ;
        Boolean checkREAD1 = AclNFSv4Matcher.isAllowed(permissionNew,
                actionREAD);
        assertTrue(
                "For who_id=1001 action READ is allowed: bits EXECUTE, READ_DATA are allowed",
                checkREAD1);

    }

    // ///////////////////////////////////////////
    @Test
    public void testREADdeny() throws Exception {

        RsType rsType = RsType.FILE;

        List<ACE> aces = new ArrayList<ACE>();

        aces.add(new ACE(AceType.ACCESS_ALLOWED_ACE_TYPE, 0, AccessMask.EXECUTE.getValue(), Who.USER, 1001, ACE.DEFAULT_ADDRESS_MSK));

        aces.add(new ACE(AceType.ACCESS_DENIED_ACE_TYPE, 0,
                AccessMask.READ_DATA.getValue(), Who.USER, 1001,
                ACE.DEFAULT_ADDRESS_MSK));

        ACL newACL = new ACL(rsType, aces);

        Subject subjectNew = new Subject();
        subjectNew.getPrincipals().add(new UidPrincipal(UID_3));
        subjectNew.getPrincipals().add(new GidPrincipal(100, true));

        Origin originNew = new Origin(Origin.AuthType.ORIGIN_AUTHTYPE_STRONG,
                "127.0.0.1");

        Owner ownerNew = new Owner(0, 0);

        Permission permissionNew = AclMapper.getPermission(subjectNew,
                originNew, ownerNew, newACL);

        // Check READ. Bits: EXECUTE,READ_DATA
        Action actionREAD = Action.READ;
        Boolean checkREAD2 = AclNFSv4Matcher.isAllowed(permissionNew,
                actionREAD);
        assertFalse(
                "For who_id=1001 action READ is denied: bits EXECUTE allowed, READ_DATA denied",
                checkREAD2);

    }

    // ///////////////////////////////////////////
    @Test
    public void testREADdeny2() throws Exception {

        RsType rsType = RsType.FILE;

        List<ACE> aces = new ArrayList<ACE>();

        aces.add(new ACE(AceType.ACCESS_DENIED_ACE_TYPE, 0, AccessMask.EXECUTE.getValue(), Who.USER, 1001, ACE.DEFAULT_ADDRESS_MSK));

        aces.add(new ACE(AceType.ACCESS_ALLOWED_ACE_TYPE, 0,
                AccessMask.READ_DATA.getValue(), Who.USER, 1001,
                ACE.DEFAULT_ADDRESS_MSK));

        ACL newACL = new ACL(rsType, aces);

        Subject subjectNew = new Subject();
        subjectNew.getPrincipals().add(new UidPrincipal(UID_3));
        subjectNew.getPrincipals().add(new GidPrincipal(100, true));

        Origin originNew = new Origin(Origin.AuthType.ORIGIN_AUTHTYPE_STRONG,
                "127.0.0.1");

        Owner ownerNew = new Owner(0, 0);

        Permission permissionNew = AclMapper.getPermission(subjectNew,
                originNew, ownerNew, newACL);

        // Check READ. Bits: EXECUTE,READ_DATA
        Action actionREAD = Action.READ;
        Boolean checkREAD3 = AclNFSv4Matcher.isAllowed(permissionNew,
                actionREAD);
        assertTrue(
                "For who_id=1001 action READ is allowed: bit EXECUTE denied, READ_DATA allowed",
                checkREAD3);

    }

    // ///////////////////////////////////////////
    @Test
    public void testREADDIRallow() throws Exception {

        RsType rsType = RsType.DIR;

        List<ACE> aces = new ArrayList<ACE>();

        aces.add(new ACE(AceType.ACCESS_ALLOWED_ACE_TYPE, 0,
                AccessMask.LIST_DIRECTORY.getValue(), Who.USER, 111,
                ACE.DEFAULT_ADDRESS_MSK));

        ACL newACL = new ACL(rsType, aces);

        Subject subjectNew = new Subject();
        subjectNew.getPrincipals().add(new UidPrincipal(UID_1));
        subjectNew.getPrincipals().add(new GidPrincipal(100, true));

        Origin originNew = new Origin(Origin.AuthType.ORIGIN_AUTHTYPE_STRONG,
                "127.0.0.1");

        Owner ownerNew = new Owner(0, 0);

        Permission permissionNew = AclMapper.getPermission(subjectNew,
                originNew, ownerNew, newACL);

        // Check READDIR. Bits: LIST_DIRECTORY
        Action actionREADDIR = Action.READDIR;
        Boolean checkREADDIR = AclNFSv4Matcher.isAllowed(permissionNew,
                actionREADDIR);
        assertTrue(
                "For who_id=111 action READDIR is allowed: bit LIST_DIRECTORY allowed",
                checkREADDIR);

    }

    // ///////////////////////////////////////////
    @Test
    public void testREMOVEDirectory() throws Exception {

        RsType rsType = RsType.DIR;
        RsType parentRsType = RsType.DIR;

        List<ACE> aces = new ArrayList<ACE>();
        List<ACE> parentAces = new ArrayList<ACE>();

        aces.add(new ACE(AceType.ACCESS_ALLOWED_ACE_TYPE, 0,
                AccessMask.DELETE.getValue(), Who.USER, 111,
                ACE.DEFAULT_ADDRESS_MSK));
        aces.add(new ACE(AceType.ACCESS_ALLOWED_ACE_TYPE, 0,
                AccessMask.DELETE.getValue(), Who.USER, 222,
                ACE.DEFAULT_ADDRESS_MSK));

        parentAces.add(new ACE(AceType.ACCESS_ALLOWED_ACE_TYPE, 1,
                AccessMask.DELETE_CHILD.getValue(), Who.USER, 111,
                ACE.DEFAULT_ADDRESS_MSK));
        parentAces.add(new ACE(AceType.ACCESS_DENIED_ACE_TYPE, 1,
                AccessMask.DELETE_CHILD.getValue(), Who.USER, 222,
                ACE.DEFAULT_ADDRESS_MSK));


        ACL childACL = new ACL(rsType, aces);
        ACL parentACL = new ACL(parentRsType, parentAces);

        Subject subjectNew = new Subject();
        subjectNew.getPrincipals().add(new UidPrincipal(UID_1));
        subjectNew.getPrincipals().add(new GidPrincipal(100, true));

        Subject subjectNextUser = new Subject();
        subjectNextUser.getPrincipals().add(new UidPrincipal(UID_2));
        subjectNextUser.getPrincipals().add(new GidPrincipal(100, true));

        Origin originNew = new Origin(Origin.AuthType.ORIGIN_AUTHTYPE_STRONG,
                "127.0.0.1");

        Owner ownerNew = new Owner(0, 0);

        Permission permissionChild = AclMapper.getPermission(subjectNew,
                originNew, ownerNew, childACL);

        Permission permissionParent = AclMapper.getPermission(subjectNew,
                originNew, ownerNew, parentACL);

        // Check REMOVE.
        ////use: Boolean isAllowed(Permission perm1, Permission perm2, Action action, Boolean isDir)
        Action actionREMOVE = Action.REMOVE;
        Boolean checkREMOVE = AclNFSv4Matcher.isAllowed(permissionParent, permissionChild,
                actionREMOVE, Boolean.TRUE);
        assertTrue(
                "For who_id=111 action REMOVE directory is allowed: bit DELETE_CHILD allowed for parent directory, bit DELETE allowed for directory itself",
                checkREMOVE);

        //for user 222:
        //For who_id=222 action REMOVE directory is allowed, as bit DELETE ALLOWED for child
        Permission permissionChildNextUser = AclMapper.getPermission(subjectNextUser,
                originNew, ownerNew, childACL);

        Permission permissionParentNextUser = AclMapper.getPermission(subjectNextUser,
                originNew, ownerNew, parentACL);

        Boolean checkREMOVENextUser = AclNFSv4Matcher.isAllowed(permissionParentNextUser, permissionChildNextUser,
                actionREMOVE, Boolean.TRUE);
        assertTrue(
                "For who_id=222 action REMOVE directory is allowed, as bit DELETE is set for child ",
                checkREMOVENextUser);
    }

    /////////////////////////////////////////////
    @Test
    public void testREMOVEFileAllow() throws Exception {

        RsType rsType = RsType.FILE;
        RsType parentRsType = RsType.DIR;

        List<ACE> aces = new ArrayList<ACE>();
        List<ACE> parentAces = new ArrayList<ACE>();

        aces.add(new ACE(AceType.ACCESS_ALLOWED_ACE_TYPE, 0, AccessMask.DELETE.getValue(), Who.USER, 111, ACE.DEFAULT_ADDRESS_MSK));

        parentAces.add(new ACE(AceType.ACCESS_ALLOWED_ACE_TYPE, 1,
                AccessMask.DELETE_CHILD.getValue(), Who.USER, 111,
                ACE.DEFAULT_ADDRESS_MSK));

        ACL newACL = new ACL(rsType, aces);

        Subject subjectNew = new Subject();
        subjectNew.getPrincipals().add(new UidPrincipal(UID_1));
        subjectNew.getPrincipals().add(new GidPrincipal(100, true));

        Origin originNew = new Origin(Origin.AuthType.ORIGIN_AUTHTYPE_STRONG,
                "127.0.0.1");

        Owner ownerNew = new Owner(0, 0);

        Permission permissionNew = AclMapper.getPermission(subjectNew,
                originNew, ownerNew, newACL);

        //for parent directory:
        ACL parentACL = new ACL(parentRsType, parentAces);

        Permission permissionParentDir = AclMapper.getPermission(subjectNew,
                originNew, ownerNew, parentACL);

        // Check REMOVE (for file). Bits: DELETE for file, DELETE_CHILD for directory are allowed
        //USE: Boolean isAllowed(Permission perm1, Permission perm2, Action action, Boolean isDir)
        //@param isDir is not used for action REMOVE
        Action actionREMOVE = Action.REMOVE;
        Boolean checkREMOVE = AclNFSv4Matcher.isAllowed(permissionParentDir, permissionNew,
                actionREMOVE, Boolean.FALSE);
        Boolean checkREMOVE2 = AclNFSv4Matcher.isAllowed(permissionParentDir, permissionNew,
                actionREMOVE, Boolean.TRUE);
        assertTrue(
                "For who_id=111 action REMOVE file is allowed: bit DELETE for file and DELETE_CHILD for directory are allowed",
                checkREMOVE);
        assertTrue(
                "For who_id=111 action REMOVE file is allowed: bit DELETE for file and DELETE_CHILD for directory are allowed",
                checkREMOVE2);

    }

    // ///////////////////////////////////////////
    @Test
    public void testREMOVEFileDeny() throws Exception {

        RsType rsType = RsType.FILE;
        RsType parentRsType = RsType.DIR;

        List<ACE> aces = new ArrayList<ACE>();
        List<ACE> parentAces = new ArrayList<ACE>();

        aces.add(new ACE(AceType.ACCESS_DENIED_ACE_TYPE, 0, AccessMask.DELETE.getValue(), Who.USER, 111, ACE.DEFAULT_ADDRESS_MSK));

        parentAces.add(new ACE(AceType.ACCESS_ALLOWED_ACE_TYPE, 1,
                AccessMask.DELETE_CHILD.getValue(), Who.USER, 111,
                ACE.DEFAULT_ADDRESS_MSK));

        ACL newACL = new ACL(rsType, aces);

        Subject subjectNew = new Subject();
        subjectNew.getPrincipals().add(new UidPrincipal(UID_1));
        subjectNew.getPrincipals().add(new GidPrincipal(100, true));

        Origin originNew = new Origin(Origin.AuthType.ORIGIN_AUTHTYPE_STRONG,
                "127.0.0.1");

        Owner ownerNew = new Owner(0, 0);

        Permission permissionNew = AclMapper.getPermission(subjectNew,
                originNew, ownerNew, newACL);

        //for parent directory:
        ACL parentACL = new ACL(parentRsType, parentAces);

        Permission permissionParentDir = AclMapper.getPermission(subjectNew,
                originNew, ownerNew, parentACL);

        // Check REMOVE (for file). Bits: DELETE for file denied, DELETE_CHILD for parent dir allowed.
        // then action REMOVE is allowed
        Action actionREMOVE = Action.REMOVE;
        Boolean checkREMOVE = AclNFSv4Matcher.isAllowed(permissionParentDir, permissionNew,
                actionREMOVE, Boolean.FALSE);
        assertTrue(
                "For who_id=111 action REMOVE file is allowed: bit DELETE_CHILD allowed",
                checkREMOVE);

    }

    // ///////////////////////////////////////////
    @Test
    public void testRENAMEDirAllow() throws Exception {

        RsType parentRsType = RsType.DIR;

        List<ACE> parentAces = new ArrayList<ACE>();

        parentAces.add(new ACE(AceType.ACCESS_ALLOWED_ACE_TYPE, 0,
                AccessMask.DELETE_CHILD.getValue() | AccessMask.ADD_SUBDIRECTORY.getValue(), Who.USER,
                111, ACE.DEFAULT_ADDRESS_MSK));


        ACL parentACL = new ACL(parentRsType, parentAces);

        Subject subjectNew = new Subject();
        subjectNew.getPrincipals().add(new UidPrincipal(UID_1));
        subjectNew.getPrincipals().add(new GidPrincipal(100, true));

        Origin originNew = new Origin(Origin.AuthType.ORIGIN_AUTHTYPE_STRONG,
                "127.0.0.1");

        Owner ownerNew = new Owner(0, 0);

        Permission parentPermission = AclMapper.getPermission(subjectNew,
                originNew, ownerNew, parentACL);

        // Check RENAME (directory). Bits that has to checked for parent directory: DELETE_CHILD, ADD_SUBDIRECTORY.
        //USE :Boolean isAllowed(Permission perm1, Permission perm2, Action action, Boolean isDir)
        //Example rename: a/b/c/dir1  ->  d/f/g/dir2
        //perm 1 - for source directory (parent of object that has to be renamed, in example above a/b/c),
        //perm2 - for destination directory (parent directory of 'new' object, in example above d/f/g)
        //isDir TRUE if action applied to directory (in example dir1)
        Action actionRENAMEdir = Action.RENAME;
        Boolean checkRENAMEdir = AclNFSv4Matcher.isAllowed(parentPermission, parentPermission,
                actionRENAMEdir, Boolean.TRUE);
        assertTrue(
                "For who_id=111 action RENAME directory is allowed: DELETE_CHILD, ADD_SUBDIRECTORY allowed. (assume parent directory is the same as destination directory)",
                checkRENAMEdir);

    }

    // ///////////////////////////////////////////
    @Test
    public void testRENAMEdirDeny() throws Exception {

        RsType parentRsType = RsType.DIR;

        List<ACE> parentAces = new ArrayList<ACE>();

        parentAces.add(new ACE(AceType.ACCESS_ALLOWED_ACE_TYPE, 0,
                AccessMask.DELETE_CHILD.getValue(), Who.USER, 111,
                ACE.DEFAULT_ADDRESS_MSK));

        parentAces.add(new ACE(AceType.ACCESS_DENIED_ACE_TYPE, 0,
                AccessMask.ADD_SUBDIRECTORY.getValue(), Who.USER, 111,
                ACE.DEFAULT_ADDRESS_MSK));

        ACL parentACL = new ACL(parentRsType, parentAces);

        Subject subjectNew = new Subject();
        subjectNew.getPrincipals().add(new UidPrincipal(UID_1));
        subjectNew.getPrincipals().add(new GidPrincipal(100, true));

        Origin originNew = new Origin(Origin.AuthType.ORIGIN_AUTHTYPE_STRONG,
                "127.0.0.1");

        Owner ownerNew = new Owner(0, 0);

        Permission parentPermission = AclMapper.getPermission(subjectNew,
                originNew, ownerNew, parentACL);

        // Check RENAME (directory). Bits that are checked for parent directory of the directory
        //that has to be renamed: DELETE_CHILD, ADD_SUBDIRECTORY
        Action actionRENAMEdir = Action.RENAME;
        Boolean checkRENAMEdir = AclNFSv4Matcher.isAllowed(parentPermission, parentPermission,
                actionRENAMEdir, Boolean.TRUE);
        assertFalse(
                "For who_id=111 action RENAME directory is denied: DELETE_CHILD allowed, ADD_SUBDIRECTORY denied.(assume parent directory is the same as destination directory) ",
                checkRENAMEdir);

    }

    // ///////////////////////////////////////////
    @Test
    public void testRENAMEdirDeny2() throws Exception {

        RsType rsType = RsType.DIR;

        List<ACE> aces = new ArrayList<ACE>();

        aces.add(new ACE(AceType.ACCESS_DENIED_ACE_TYPE, 0,
                AccessMask.DELETE_CHILD.getValue(), Who.USER, 111,
                ACE.DEFAULT_ADDRESS_MSK));

        aces.add(new ACE(AceType.ACCESS_ALLOWED_ACE_TYPE, 0,
                AccessMask.ADD_SUBDIRECTORY.getValue(), Who.USER, 111,
                ACE.DEFAULT_ADDRESS_MSK));

        ACL newACL = new ACL(rsType, aces);

        Subject subjectNew = new Subject();
        subjectNew.getPrincipals().add(new UidPrincipal(UID_1));
        subjectNew.getPrincipals().add(new GidPrincipal(100, true));

        Origin originNew = new Origin(Origin.AuthType.ORIGIN_AUTHTYPE_STRONG,
                "127.0.0.1");

        Owner ownerNew = new Owner(0, 0);

        Permission permissionNew = AclMapper.getPermission(subjectNew,
                originNew, ownerNew, newACL);

        // Check RENAME (directory). Bits checked for parent directory: DELETE_CHILD, ADD_SUBDIRECTORY
        Action actionRENAMEdir = Action.RENAME;
        Boolean checkRENAMEdir = AclNFSv4Matcher.isAllowed(permissionNew, permissionNew,
                actionRENAMEdir, Boolean.TRUE);
        assertFalse(
                "For who_id=111 action RENAME directory is denied: DELETE_CHILD denied, ADD_SUBDIRECTORY allowed. (assume parent directory is the same as destination directory)",
                checkRENAMEdir);

    }

    // ///////////////////////////////////////////
    @Test
    public void testRENAMEfileAllow() throws Exception {

        RsType rsType = RsType.FILE;

        List<ACE> aces = new ArrayList<ACE>();

        //these are ACEs of the directory, that contains a file to be renamed
        aces.add(new ACE(AceType.ACCESS_ALLOWED_ACE_TYPE, 1,
                AccessMask.DELETE_CHILD.getValue(), Who.USER, 111,
                ACE.DEFAULT_ADDRESS_MSK));

        aces.add(new ACE(AceType.ACCESS_ALLOWED_ACE_TYPE, 1,
                AccessMask.ADD_FILE.getValue(), Who.USER, 111,
                ACE.DEFAULT_ADDRESS_MSK));

        ACL newACL = new ACL(rsType, aces);

        Subject subjectNew = new Subject();
        subjectNew.getPrincipals().add(new UidPrincipal(UID_1));
        subjectNew.getPrincipals().add(new GidPrincipal(100, true));

        Origin originNew = new Origin(Origin.AuthType.ORIGIN_AUTHTYPE_STRONG,
                "127.0.0.1");

        Owner ownerNew = new Owner(0, 0);

        Permission permissionNew = AclMapper.getPermission(subjectNew,
                originNew, ownerNew, newACL);

        // Check RENAME (file). Bits: DELETE_CHILD, ADD_FILE
        Action actionRENAMEfile = Action.RENAME;
        Boolean checkRENAMEfile = AclNFSv4Matcher.isAllowed(permissionNew, permissionNew,
                actionRENAMEfile, Boolean.FALSE);
        assertTrue(
                "For who_id=111 action RENAME file is allowed: DELETE_CHILD, ADD_FILE are allowed for parent directory (assume parent directory is the same as destination directory)",
                checkRENAMEfile);

    }

    // ///////////////////////////////////////////
    @Test
    public void testRENAMEfileDeny() throws Exception {

        RsType rsType = RsType.FILE;

        List<ACE> aces = new ArrayList<ACE>();

        //these are ACEs of the directory, that contains a file to be renamed
        aces.add(new ACE(AceType.ACCESS_DENIED_ACE_TYPE, 1,
                AccessMask.DELETE_CHILD.getValue(), Who.USER, 111,
                ACE.DEFAULT_ADDRESS_MSK));

        aces.add(new ACE(AceType.ACCESS_ALLOWED_ACE_TYPE, 1,
                AccessMask.ADD_FILE.getValue(), Who.USER, 111,
                ACE.DEFAULT_ADDRESS_MSK));

        ACL newACL = new ACL(rsType, aces);

        Subject subjectNew = new Subject();
        subjectNew.getPrincipals().add(new UidPrincipal(UID_1));
        subjectNew.getPrincipals().add(new GidPrincipal(100, true));

        Origin originNew = new Origin(Origin.AuthType.ORIGIN_AUTHTYPE_WEAK,
                "127.0.0.1");

        Owner ownerNew = new Owner(0, 0);

        Permission permissionNew = AclMapper.getPermission(subjectNew,
                originNew, ownerNew, newACL);

        // Check RENAME (file). Bits: DELETE_CHILD, ADD_FILE for parent(source) directory.
        // in this test 'source directory'='destination directory'
        Action actionRENAMEfile = Action.RENAME;
        Boolean checkRENAMEfile = AclNFSv4Matcher.isAllowed(permissionNew, permissionNew,
                actionRENAMEfile, Boolean.FALSE);
        assertFalse(
                "For who_id=111 action RENAME file is denied: DELETE_CHILD denied, ADD_FILE allowed.(assume parent directory is the same as destination directory) ",
                checkRENAMEfile);

    }

    // ///////////////////////////////////////////
    @Test
    public void testRENAMEfileDeny2() throws Exception {

        RsType rsType = RsType.FILE;

        List<ACE> aces = new ArrayList<ACE>();

        aces.add(new ACE(AceType.ACCESS_ALLOWED_ACE_TYPE, 0,
                AccessMask.DELETE_CHILD.getValue(), Who.USER, 111,
                ACE.DEFAULT_ADDRESS_MSK));

        aces.add(new ACE(AceType.ACCESS_DENIED_ACE_TYPE, 0, AccessMask.ADD_FILE.getValue(), Who.USER, 111, ACE.DEFAULT_ADDRESS_MSK));

        ACL newACL = new ACL(rsType, aces);

        Subject subjectNew = new Subject();
        subjectNew.getPrincipals().add(new UidPrincipal(UID_1));
        subjectNew.getPrincipals().add(new GidPrincipal(100, true));

        Origin originNew = new Origin(Origin.AuthType.ORIGIN_AUTHTYPE_WEAK,
                "127.0.0.1");

        Owner ownerNew = new Owner(0, 0);

        Permission permissionNew = AclMapper.getPermission(subjectNew,
                originNew, ownerNew, newACL);

        // Check RENAME (file). Bits: DELETE_CHILD, ADD_FILE for parent(source) directory.
        // in this test 'source directory'='destination directory'
        Action actionRENAMEfile = Action.RENAME;
        Boolean checkRENAMEfile = AclNFSv4Matcher.isAllowed(permissionNew, permissionNew,
                actionRENAMEfile, Boolean.FALSE);
        assertFalse(
                "For who_id=111 action RENAME file is denied: DELETE_CHILD allowed, ADD_FILE denied.",
                checkRENAMEfile);

    }
    //////////////////////////////////////////////

    @Test
    public void testRENAMEfileUndefined() throws Exception {

        RsType rsType = RsType.FILE;

        List<ACE> aces = new ArrayList<ACE>();

        //this is ACE of the directory, that contains a file to be renamed
        //For user 222: ADD_FILE is allowed, but DELETE_CHILD is undefined
        aces.add(new ACE(AceType.ACCESS_ALLOWED_ACE_TYPE, 1,
                AccessMask.ADD_FILE.getValue(), Who.USER, 222,
                ACE.DEFAULT_ADDRESS_MSK));

        ACL newACL = new ACL(rsType, aces);

        Subject subjectNew = new Subject();
        subjectNew.getPrincipals().add(new UidPrincipal(UID_2));
        subjectNew.getPrincipals().add(new GidPrincipal(100, true));

        Origin originNew = new Origin(Origin.AuthType.ORIGIN_AUTHTYPE_WEAK,
                "127.0.0.1");

        Owner ownerNew = new Owner(0, 0);

        Permission permissionNew = AclMapper.getPermission(subjectNew,
                originNew, ownerNew, newACL);

        // Action RENAME (file). Undefined, expected NULL.
        //use Boolean isAllowed(Permission perm1, Permission perm2, Action action, Boolean isDir)
        //where perm1 - for a source , perm2 - for a destination
        Boolean checkUndef = AclNFSv4Matcher.isAllowed(permissionNew, permissionNew, Action.RENAME,
                Boolean.FALSE);
        assertNull("For who_id=222 action RENAME is undefined: ADD_FILE is allowed, but DELETE_CHILD is undefined", checkUndef);

    }

    // ///////////////////////////////////////////
    @Test
    public void testWRITEfileDeny() throws Exception {

        RsType rsType = RsType.FILE;

        List<ACE> aces = new ArrayList<ACE>();

        aces.add(new ACE(AceType.ACCESS_DENIED_ACE_TYPE, 0,
                AccessMask.WRITE_DATA.getValue(), Who.USER, 111,
                ACE.DEFAULT_ADDRESS_MSK));

        aces.add(new ACE(AceType.ACCESS_ALLOWED_ACE_TYPE, 0,
                AccessMask.APPEND_DATA.getValue(), Who.USER, 111,
                ACE.DEFAULT_ADDRESS_MSK));

        ACL newACL = new ACL(rsType, aces);

        Subject subjectNew = new Subject();
        subjectNew.getPrincipals().add(new UidPrincipal(UID_1));
        subjectNew.getPrincipals().add(new GidPrincipal(100, true));

        Origin originNew = new Origin(Origin.AuthType.ORIGIN_AUTHTYPE_WEAK,
                "127.0.0.1");

        Owner ownerNew = new Owner(0, 0);

        Permission permissionNew = AclMapper.getPermission(subjectNew,
                originNew, ownerNew, newACL);

        // Check WRITE. Bits: APPEND_DATA (not checked), WRITE_DATA
        Action actionWRITEfile = Action.WRITE;
        Boolean checkWRITEfile = AclNFSv4Matcher.isAllowed(permissionNew,
                actionWRITEfile);
        assertFalse(
                "For who_id=111 action RENAME file is denied: DELETE_CHILD , ADD_FILE denied",
                checkWRITEfile);

    }

    // ///////////////////////////////////////////
    @Test
    public void testWRITEfileAllowed() throws Exception {

        RsType rsType = RsType.FILE;

        List<ACE> aces = new ArrayList<ACE>();

        aces.add(new ACE(AceType.ACCESS_ALLOWED_ACE_TYPE, 0, AccessMask.EXECUTE.getValue() | AccessMask.READ_DATA.getValue() | AccessMask.WRITE_DATA.getValue() | AccessMask.APPEND_DATA.getValue(), Who.USER, 111,
                ACE.DEFAULT_ADDRESS_MSK));

        ACL newACL = new ACL(rsType, aces);

        Subject subjectNew = new Subject();
        subjectNew.getPrincipals().add(new UidPrincipal(UID_1));
        subjectNew.getPrincipals().add(new GidPrincipal(100, true));

        Origin originNew = new Origin(Origin.AuthType.ORIGIN_AUTHTYPE_STRONG,
                "127.0.0.1");

        Owner ownerNew = new Owner(0, 0);

        Permission permissionNew = AclMapper.getPermission(subjectNew,
                originNew, ownerNew, newACL);

        // Check WRITE. Bits: APPEND_DATA (not checked), WRITE_DATA
        Action actionWRITEfile2 = Action.WRITE;
        Boolean checkWRITEfile2 = AclNFSv4Matcher.isAllowed(permissionNew,
                actionWRITEfile2);
        assertTrue(
                "For who_id=111 action WRITE file is allowed: WRITE_DATA allowed",
                checkWRITEfile2);

    }

    // ///////////////////////////////////////////
    @Test
    public void testSETATTRfileAllowed() throws Exception {

        RsType rsType = RsType.FILE;

        List<ACE> aces = new ArrayList<ACE>();

        int // masks = (AccessMask.APPEND_DATA.getValue());
                masks = (AccessMask.WRITE_ATTRIBUTES.getValue());
        masks |= (AccessMask.WRITE_ACL.getValue());
        masks |= (AccessMask.WRITE_OWNER.getValue());

        aces.add(new ACE(AceType.ACCESS_ALLOWED_ACE_TYPE, 0, masks, Who.USER,
                111, ACE.DEFAULT_ADDRESS_MSK));

        aces.add(new ACE(AceType.ACCESS_DENIED_ACE_TYPE, 0,
                AccessMask.APPEND_DATA.getValue(), Who.USER, 111,
                ACE.DEFAULT_ADDRESS_MSK));

        aces.add(new ACE(AceType.ACCESS_DENIED_ACE_TYPE, 0,
                AccessMask.WRITE_DATA.getValue(), Who.USER, 111,
                ACE.DEFAULT_ADDRESS_MSK));

        ACL newACL = new ACL(rsType, aces);

        Subject subjectNew = new Subject();
        subjectNew.getPrincipals().add(new UidPrincipal(UID_1));
        subjectNew.getPrincipals().add(new GidPrincipal(100, true));

        Origin originNew = new Origin(Origin.AuthType.ORIGIN_AUTHTYPE_STRONG,
                "127.0.0.1");

        Owner ownerNew = new Owner(0, 0);

        Permission permissionNew = AclMapper.getPermission(subjectNew,
                originNew, ownerNew, newACL);

        // Check SETATTR (Attribute ACL). Access flag: WRITE_ACL
        Action actionSETATTRfile = Action.SETATTR;
        // USE: Boolean isAllowed(Permission perm, Action action, FileAttribute
        // attribute)
        Boolean checkSETATTRfile = AclNFSv4Matcher.isAllowed(permissionNew,
                actionSETATTRfile, FileAttribute.FATTR4_ACL);
        assertTrue(
                "For who_id=111 action SETATTR (Attribute FATTR4_ACL) is allowed: WRITE_ACL allowed",
                checkSETATTRfile);

        // Check SETATTR (Attribute OWNER_GROUP). Access flag: WRITE_OWNER
        Boolean checkSETATTRfile2 = AclNFSv4Matcher.isAllowed(permissionNew,
                actionSETATTRfile, FileAttribute.FATTR4_OWNER_GROUP);
        assertTrue(
                "For who_id=111 action SETATTR (Attribute  FATTR4_OWNER_GROUP) is allowed: WRITE_OWNER allowed",
                checkSETATTRfile2);

        // Check SETATTR (Attributes OWNER_GROUP and OWNER). Access flag:
        // WRITE_OWNER
        int fileAttrTest = (FileAttribute.FATTR4_OWNER_GROUP.getValue());
        fileAttrTest |= (FileAttribute.FATTR4_OWNER.getValue());
        Boolean checkSETATTRfile3 = AclNFSv4Matcher.isAllowed(permissionNew,
                actionSETATTRfile, FileAttribute.valueOf(fileAttrTest));
        assertTrue(
                "For who_id=111 action SETATTR (Attributes  FATTR4_OWNER_GROUP and FATTR4_OWNER) is allowed: WRITE_OWNER allowed",
                checkSETATTRfile3);

        // Check SETATTR (Attribute SIZE). Access flag: WRITE_DATA
        Boolean checkSETATTRfile4 = AclNFSv4Matcher.isAllowed(permissionNew,
                actionSETATTRfile, FileAttribute.FATTR4_SIZE);
        assertFalse(
                "For who_id=111 action SETATTR (Attribute  FATTR4_SIZE) is denied: WRITE_DATA denied",
                checkSETATTRfile4);
    }

    // ///////////////////////////////////////////
    @Test
    public void testGETATTRreadACLAllowed() throws Exception {
        // Test description.
        // Bit READ_ACL allowed, bit READ_ATTRIBUTES denied.
        // Expected results:
        // 1. READ_ACL allowed => It is allowed to get complete ACL,
        // i.e., action GETATTR for the attribute FileAttribute.FATTR4_ACL is allowed.
        // 2. READ_ATTRIBUTES denied => It is NOT allowed to get a single attribute,
        // i.e., action GETATTR is denied for any attribute (FATTR4_OWNER_GROUP, FATTR4_SIZE etc.)

        RsType rsType = RsType.FILE;

        List<ACE> aces = new ArrayList<ACE>();

        int masks = (AccessMask.READ_ACL.getValue());

        aces.add(new ACE(AceType.ACCESS_ALLOWED_ACE_TYPE, 0, masks, Who.USER,
                111, ACE.DEFAULT_ADDRESS_MSK));

        aces.add(new ACE(AceType.ACCESS_DENIED_ACE_TYPE, 0,
                AccessMask.READ_ATTRIBUTES.getValue(), Who.USER, 111,
                ACE.DEFAULT_ADDRESS_MSK));

        ACL newACL = new ACL(rsType, aces);

        Subject subjectNew = new Subject();
        subjectNew.getPrincipals().add(new UidPrincipal(UID_1));
        subjectNew.getPrincipals().add(new GidPrincipal(100, true));

        Origin originNew = new Origin(Origin.AuthType.ORIGIN_AUTHTYPE_STRONG,
                "127.0.0.1");

        Owner ownerNew = new Owner(0, 0);

        Permission permissionNew = AclMapper.getPermission(subjectNew,
                originNew, ownerNew, newACL);

        // Check GETATTR (Attribute ACL). Access flag: WRITE_ACL
        Action actionGETATTRfile = Action.GETATTR;
        // USE: Boolean isAllowed(Permission perm, Action action, FileAttribute
        // attribute)
        Boolean checkGETATTRfile = AclNFSv4Matcher.isAllowed(permissionNew,
                actionGETATTRfile, FileAttribute.FATTR4_ACL);
        assertTrue(
                "For who_id=111 action GETATTR (Attribute FATTR4_ACL) is allowed: READ_ACL allowed",
                checkGETATTRfile);

        // Check GETATTR (Attribute OWNER_GROUP).
        Boolean checkGETATTRfile2 = AclNFSv4Matcher.isAllowed(permissionNew,
                actionGETATTRfile, FileAttribute.FATTR4_OWNER_GROUP);
        assertFalse(
                "For who_id=111 action GETATTR (Attribute  FATTR4_OWNER_GROUP) is denied: GET_ATTRIBUTES denied ",
                checkGETATTRfile2);

        // Check GETATTR (Attributes OWNER_GROUP and OWNER).
        int fileAttrTest = (FileAttribute.FATTR4_OWNER_GROUP.getValue());
        fileAttrTest |= (FileAttribute.FATTR4_OWNER.getValue());
        Boolean checkGETATTRfile3 = AclNFSv4Matcher.isAllowed(permissionNew,
                actionGETATTRfile, FileAttribute.valueOf(fileAttrTest));
        assertFalse(
                "For who_id=111 action GETATTR (Attributes  FATTR4_OWNER_GROUP and FATTR4_OWNER) is denied: GET_ATTRIBUTES denied ",
                checkGETATTRfile3);

        // Check GETATTR (Attribute SIZE).
        Boolean checkGETATTRfile4 = AclNFSv4Matcher.isAllowed(permissionNew,
                actionGETATTRfile, FileAttribute.FATTR4_SIZE);
        assertFalse(
                "For who_id=111 action GETATTR (Attribute  FATTR4_SIZE) is denied: GET_ATTRIBUTES denied",
                checkGETATTRfile4);
    }

    // ///////////////////////////////////////////
    @Test
    public void testGETATTRreadAttributeAllowed() throws Exception {
        // Test description.
        // Bit READ_ACL denied, bit READ_ATTRIBUTES allowed.
        // Expected results:
        // 1. READ_ACL denied => It is NOT allowed to get complete ACL,
        // i.e., action GETATTR for the attribute FileAttribute.FATTR4_ACL is DENIED.
        // 2. READ_ATTRIBUTES allowed => It is allowed to get a single attribute,
        // i.e., action GETATTR is allowed for any attribute (FATTR4_OWNER_GROUP, FATTR4_SIZE etc.)

        RsType rsType = RsType.FILE;

        List<ACE> aces = new ArrayList<ACE>();

        aces.add(new ACE(AceType.ACCESS_DENIED_ACE_TYPE, 0, AccessMask.READ_ACL.getValue(), Who.USER, 111, ACE.DEFAULT_ADDRESS_MSK));

        aces.add(new ACE(AceType.ACCESS_ALLOWED_ACE_TYPE, 0,
                AccessMask.READ_ATTRIBUTES.getValue(), Who.USER, 111,
                ACE.DEFAULT_ADDRESS_MSK));

        ACL newACL = new ACL(rsType, aces);

        Subject subjectNew = new Subject();
        subjectNew.getPrincipals().add(new UidPrincipal(UID_1));
        subjectNew.getPrincipals().add(new GidPrincipal(100, true));

        Origin originNew = new Origin(Origin.AuthType.ORIGIN_AUTHTYPE_STRONG,
                "127.0.0.1");

        Owner ownerNew = new Owner(0, 0);

        Permission permissionNew = AclMapper.getPermission(subjectNew,
                originNew, ownerNew, newACL);

        // Check GETATTR (Attribute ACL).
        Action actionGETATTRfile = Action.GETATTR;
        // USE: Boolean isAllowed(Permission perm, Action action, FileAttribute
        // attribute)
        Boolean checkGETATTRfile = AclNFSv4Matcher.isAllowed(permissionNew,
                actionGETATTRfile, FileAttribute.FATTR4_ACL);
        assertFalse(
                "For who_id=111 action GETATTR (Attribute FATTR4_ACL) is denied: READ_ACL denied",
                checkGETATTRfile);

        // Check GETATTR (Attribute OWNER_GROUP).
        Boolean checkGETATTRfile2 = AclNFSv4Matcher.isAllowed(permissionNew,
                actionGETATTRfile, FileAttribute.FATTR4_OWNER_GROUP);
        assertTrue(
                "For who_id=111 action GETATTR (Attribute  FATTR4_OWNER_GROUP) is allowed: GET_ATTRIBUTES allowed ",
                checkGETATTRfile2);

        // Check GETATTR (Attributes OWNER_GROUP and OWNER).
        int fileAttrTest = (FileAttribute.FATTR4_OWNER_GROUP.getValue());
        fileAttrTest |= (FileAttribute.FATTR4_OWNER.getValue());
        Boolean checkGETATTRfile3 = AclNFSv4Matcher.isAllowed(permissionNew,
                actionGETATTRfile, FileAttribute.valueOf(fileAttrTest));
        assertTrue(
                "For who_id=111 action GETATTR (Attributes  FATTR4_OWNER_GROUP and FATTR4_OWNER) is allowed: GET_ATTRIBUTES allowed ",
                checkGETATTRfile3);

        // Check GETATTR (Attribute SIZE).
        Boolean checkGETATTRfile4 = AclNFSv4Matcher.isAllowed(permissionNew,
                actionGETATTRfile, FileAttribute.FATTR4_SIZE);
        assertTrue(
                "For who_id=111 action GETATTR (Attribute  FATTR4_SIZE) is allowed: GET_ATTRIBUTES allowed",
                checkGETATTRfile4);

    }
}
