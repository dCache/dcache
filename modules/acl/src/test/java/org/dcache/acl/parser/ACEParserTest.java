package org.dcache.acl.parser;

import org.junit.Test;

import org.dcache.acl.ACE;
import org.dcache.acl.enums.AccessMask;
import org.dcache.acl.enums.Who;

import static org.dcache.acl.enums.AceType.*;
import static org.dcache.acl.enums.AceFlags.*;
import static org.dcache.acl.enums.AccessMask.*;
import static org.dcache.acl.parser.ACEParser.*;

import static org.junit.Assert.*;

public class ACEParserTest {

    @Test
    public void testParseAllowOnOwner() {
        int mask = toAccessMask(APPEND_DATA, READ_DATA, WRITE_DATA);
        ACE ace = new ACE(ACCESS_ALLOWED_ACE_TYPE, 0, mask, Who.OWNER, -1);
        ACE parsed = parseLinuxAce("A::OWNER@:rwa");

        assertEquals(ace, parsed);
    }

    @Test
    public void testParseDenyOnOwner() {
        int mask = toAccessMask(WRITE_DATA);
        ACE ace = new ACE(ACCESS_DENIED_ACE_TYPE, 0, mask, Who.OWNER, -1);
        ACE parsed = parseLinuxAce("D::OWNER@:w");

        assertEquals(ace, parsed);
    }

    @Test
    public void testParseAlloOnPrimaryGroup() {
        int mask = toAccessMask(APPEND_DATA, READ_DATA, WRITE_DATA);
        ACE ace = new ACE(ACCESS_ALLOWED_ACE_TYPE, 0, mask, Who.OWNER_GROUP, -1);
        ACE parsed = parseLinuxAce("A::GROUP@:rwa");

        assertEquals(ace, parsed);
    }

    @Test
    public void testParseDenyOnPrimaryGroup() {
        int mask = toAccessMask(WRITE_DATA);
        ACE ace = new ACE(ACCESS_DENIED_ACE_TYPE, 0, mask, Who.OWNER_GROUP, -1);
        ACE parsed = parseLinuxAce("D::GROUP@:w");

        assertEquals(ace, parsed);
    }

    @Test
    public void testParseAlloOnUser() {
        int mask = toAccessMask(APPEND_DATA, READ_DATA, WRITE_DATA);
        ACE ace = new ACE(ACCESS_ALLOWED_ACE_TYPE, 0, mask, Who.USER, 123);
        ACE parsed = parseLinuxAce("A::123:rwa");

        assertEquals(ace, parsed);
    }

    @Test
    public void testParseDenyOnUser() {
        int mask = toAccessMask(WRITE_DATA);
        ACE ace = new ACE(ACCESS_DENIED_ACE_TYPE, 0, mask, Who.USER, 123);
        ACE parsed = parseLinuxAce("D::123:w");

        assertEquals(ace, parsed);
    }

    @Test
    public void testParseAlloOnGroup() {
        int mask = toAccessMask(APPEND_DATA, READ_DATA, WRITE_DATA);
        ACE ace = new ACE(ACCESS_ALLOWED_ACE_TYPE, IDENTIFIER_GROUP.getValue(), mask, Who.GROUP, 123);
        ACE parsed = parseLinuxAce("A:g:123:rwa");

        assertEquals(ace, parsed);
    }

    @Test
    public void testParseDenyOnGroup() {
        int mask = toAccessMask(WRITE_DATA);
        ACE ace = new ACE(ACCESS_DENIED_ACE_TYPE, IDENTIFIER_GROUP.getValue(), mask, Who.GROUP, 123);
        ACE parsed = parseLinuxAce("D:g:123:w");

        assertEquals(ace, parsed);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWrongFormat() {
        parseLinuxAce("D:g:123");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWrongType() {
        parseLinuxAce("H:g:123:w");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWrongTypeLen() {
        parseLinuxAce("AB:g:123:w");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWrongFlag() {
        parseLinuxAce("D:x:123:w");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWrongMask() {
        parseLinuxAce("D:g:123:g");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWrongPrincipal() {
        parseLinuxAce("D:g:someone:w");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWrongSpecialPrincipal() {
        parseLinuxAce("D:g:SOMEONW@:w");
    }

    public static int toAccessMask(AccessMask...masks) {
        int mask = 0;
        for(AccessMask am: masks) {
            mask |= am.getValue();
        }
        return mask;
    }
}
