package org.dcache.tests.namespace;

import org.junit.BeforeClass;
import org.junit.Test;

import javax.security.auth.Subject;

import java.security.Principal;
import java.util.EnumSet;

import diskCacheV111.util.PnfsId;

import org.dcache.acl.ACL;
import org.dcache.acl.enums.AccessType;
import org.dcache.acl.enums.RsType;
import org.dcache.acl.parser.ACLParser;
import org.dcache.auth.GidPrincipal;
import org.dcache.auth.Origin;
import org.dcache.auth.UidPrincipal;
import org.dcache.namespace.ACLPermissionHandler;
import org.dcache.vehicles.FileAttributes;

import static org.dcache.namespace.FileAttribute.ACL;
import static org.junit.Assert.assertTrue;

public class ACLPermissionHandlerSecondTest {

    private static final int UID = 111, GID = 1000;
    private static final String PREFIX_USER = "USER:" + UID + ":";
    private static ACLPermissionHandler pdp;
    private static Origin origin;
    private static Subject subject;
    private static PnfsId pnfsID =
            new PnfsId("0000416DFB43177548A8ADE89BAB82EC529C");

    @BeforeClass
    public static void setUpClass() throws Exception {
        pdp = new ACLPermissionHandler();
        origin = new Origin("127.0.0.1");

        Principal user = new UidPrincipal(UID);
        Principal group = new GidPrincipal(GID, true);

        subject = new Subject();
        subject.getPrincipals().add(user);
        subject.getPrincipals().add(group);
        subject.getPrincipals().add(origin);
    }

    private FileAttributes getAttributes(int uid, int gid, ACL acl) {
        FileAttributes attr = new FileAttributes();
        attr.setOwner(uid);
        attr.setGroup(gid);
        attr.setAcl(acl);
        return attr;
    }

    /***********************************************************************************************************************************************************
     * Tests
     */
    @Test
    public void testReadFile() {
        FileAttributes attr = getAttributes(UID, GID, null);

        assertTrue("Read file should be undefined!",
                pdp.canReadFile(subject, attr) == AccessType.ACCESS_UNDEFINED);

        attr.setAcl(ACLParser.parseAdm(RsType.FILE, PREFIX_USER + "-r"));
        assertTrue("Read file should be denied!",
                pdp.canReadFile(subject, attr) == AccessType.ACCESS_DENIED);

        attr.setAcl(ACLParser.parseAdm(RsType.FILE, PREFIX_USER + "+r"));
        assertTrue("Read file should be allowed!",
                pdp.canReadFile(subject, attr) == AccessType.ACCESS_ALLOWED);
    }

    @Test
    public void testWriteFile() {
        FileAttributes attr = getAttributes(UID, GID, null);

        assertTrue("Write file should be undefined!",
                pdp.canWriteFile(subject, attr) == AccessType.ACCESS_UNDEFINED);

        attr.setAcl(ACLParser.parseAdm(RsType.FILE, PREFIX_USER + "-w"));
        assertTrue("Write file should be denied!", //
                pdp.canWriteFile(subject, attr) == AccessType.ACCESS_DENIED);

        attr.setAcl(ACLParser.parseAdm(RsType.FILE, PREFIX_USER + "+w"));
        assertTrue("Write file should be allowed!", //
                pdp.canWriteFile(subject, attr) == AccessType.ACCESS_ALLOWED);
    }

    @Test
    public void testCreateFile() {
        FileAttributes attr = getAttributes(UID, GID, null);

        assertTrue("Create file should be undefined!", //
                pdp.canCreateFile(subject, attr) == AccessType.ACCESS_UNDEFINED);

        attr.setAcl(ACLParser.parseAdm(RsType.DIR, PREFIX_USER + "-f"));
        assertTrue("Create file should be denied!", //
                pdp.canCreateFile(subject, attr) == AccessType.ACCESS_DENIED);

        attr.setAcl(ACLParser.parseAdm(RsType.DIR, PREFIX_USER + "+f"));
        assertTrue("Create file should be allowed!", //
                pdp.canCreateFile(subject, attr) == AccessType.ACCESS_ALLOWED);
    }

    @Test
    public void testCreateDir() {
        FileAttributes attr = getAttributes(UID, GID, null);

        assertTrue("Create directory should be undefined!", //
                pdp.canCreateSubDir(subject, attr) == AccessType.ACCESS_UNDEFINED);

        attr.setAcl(ACLParser.parseAdm(RsType.DIR, PREFIX_USER + "-s"));
        assertTrue("Create directory should be denied!", //
                pdp.canCreateSubDir(subject, attr) == AccessType.ACCESS_DENIED);

        attr.setAcl(ACLParser.parseAdm(RsType.DIR, PREFIX_USER + "+s"));
        assertTrue("Create directory should be allowed!", //
                pdp.canCreateSubDir(subject, attr) == AccessType.ACCESS_ALLOWED);
    }

    @Test
    public void testListDir() {
        FileAttributes attr = getAttributes(UID, GID, null);

        assertTrue("List directory should be undefined!", //
                pdp.canListDir(subject, attr) == AccessType.ACCESS_UNDEFINED);

        attr.setAcl(ACLParser.parseAdm(RsType.DIR, PREFIX_USER + "+l"));
        assertTrue("List directory should be allowed!", //
                pdp.canListDir(subject, attr) == AccessType.ACCESS_ALLOWED);

        attr.setAcl(ACLParser.parseAdm(RsType.DIR, PREFIX_USER + "-l"));
        assertTrue("List directory should be denied!", //
                pdp.canListDir(subject, attr) == AccessType.ACCESS_DENIED);
    }

    @Test
    public void testSetAttributes() {
        FileAttributes attr = getAttributes(UID, GID, null);

        assertTrue("Set attributes should be undefined!", //
                pdp.canSetAttributes(subject, attr, EnumSet.of(ACL)) == AccessType.ACCESS_UNDEFINED);

        attr.setAcl(ACLParser.parseAdm(RsType.DIR, PREFIX_USER + "+C"));
        assertTrue("Set attributes should be allowed!", //
                pdp.canSetAttributes(subject, attr, EnumSet.of(ACL)) == AccessType.ACCESS_ALLOWED);

        attr.setAcl(ACLParser.parseAdm(RsType.DIR, PREFIX_USER + "-C"));
        assertTrue("Set attributes should be denied!", //
                pdp.canSetAttributes(subject, attr, EnumSet.of(ACL)) == AccessType.ACCESS_DENIED);
    }

    @Test
    public void testGetAttributes() {
        FileAttributes attr = getAttributes(UID, GID, null);

        assertTrue("Get attributes (read ACL) should be undefined!", //
                pdp.canGetAttributes(subject, attr, EnumSet.of(ACL)) == AccessType.ACCESS_UNDEFINED);

        attr.setAcl(ACLParser.parseAdm(RsType.DIR, PREFIX_USER + "+c"));
        assertTrue("Get attributes (read ACL) should be allowed!", //
                pdp.canGetAttributes(subject, attr, EnumSet.of(ACL)) == AccessType.ACCESS_ALLOWED);

        attr.setAcl(ACLParser.parseAdm(RsType.DIR, PREFIX_USER + "-c"));
        assertTrue("Get attributes (read ACL) should be denied!", //
                pdp.canGetAttributes(subject, attr, EnumSet.of(ACL)) == AccessType.ACCESS_DENIED);
    }
}
