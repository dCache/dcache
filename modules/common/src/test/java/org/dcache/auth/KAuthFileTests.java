package org.dcache.auth;

import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

import static org.junit.Assert.*;

/**
 *  Testcase for the KAuthFile class
 */
public class KAuthFileTests
{
    public static final String VERSION_21_FILENAME = "sample21.kpwd";

    public static final int PASSWD_ENTRY_ID = 0;
    public static final int PASSWD_ENTRY_PRIORITY = 0;

    /*
     * Constants relating to the sample21.kpwd file's line:
     *
     *     passwd pwduser f259f081 read-write 1000 2000 /data/pwduser /root /fs-root
     */
    public static final String PWDUSER_USER = "pwduser";
    public static final String PASSWD_ENTRY_PASSWORD = "pwduser";
    public static final String PWDUSER_PASSWORD_HASH = "f259f081";
    public static final String PWDUSER_PASSWORD_PLAINTEXT = "too many secrets";
    public static final int PWDUSER_UID = 1000;
    public static final int PWDUSER_GID = 2000;
    public static final String PWDUSER_HOME = "/data/pwduser";
    public static final String PWDUSER_FSROOT = "/fs-root";
    public static final boolean PASSWD_ENTRY_READONLY = false;
    public static final String PWDUSER_ROOT = "/root";


    /*
     * Contants about an added user's passwd entry.
     */
    public static final String NEWUSER_USER = "newuser";
    public static final int NEWUSER_UID = 100;
    public static final int NEWUSER_GID = 200;
    public static final String NEWUSER_HOME = "/home/newuser";
    public static final String NEWUSER_ROOT = "/";
    public static final String NEWUSER_PASSWORD_PLAINTEXT = "something fishy";
    public static final String NEWUSER_PASSWORD_HASH = "9d9c44b";


    KAuthFile _sample21;

    @Before
    public void setUp() throws IOException
    {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        InputStream in = loader.getResourceAsStream(VERSION_21_FILENAME);
        _sample21 = new KAuthFile(in);
    }

    @Test
    public void testSimplePwdEntry()
    {
        assertHasPwduser(_sample21);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testDcuseraddWithNoArgs()
    {
        KAuthFile.Arguments args = KAuthFile.parseArgs(new String[]{""}, null);
        _sample21.dcuseradd(args);
    }

    @Test
    public void testDcuseraddAddsPasswordEntry()
    {
        KAuthFile.Arguments args = KAuthFile.parseArgs(new String[]{
            "dcuseradd", "/path/to/file", NEWUSER_USER,
            "-u", Integer.toString(NEWUSER_UID),
            "-g", Integer.toString(NEWUSER_GID),
            "-w", "read-write", "-h", NEWUSER_HOME, "-r", NEWUSER_ROOT,
            "-p", NEWUSER_PASSWORD_PLAINTEXT}, null);

        _sample21.dcuseradd(args);

        assertHasPwduser(_sample21);
        assertHasNewuser(_sample21);

        KAuthFile newFile = saveAndParse(_sample21);

        assertHasPwduser(newFile);
        assertHasNewuser(newFile);
    }

    public void assertHasPwduser(KAuthFile file)
    {
        UserPwdRecord record = file.getUserPwdRecord(PWDUSER_USER);

        assertPasswdEntry(record, PWDUSER_USER, PWDUSER_ROOT, PWDUSER_FSROOT,
                PWDUSER_HOME, PWDUSER_PASSWORD_HASH, PWDUSER_UID, PWDUSER_GID,
                false);

        assertTrue(record.passwordIsValid(PWDUSER_PASSWORD_PLAINTEXT));
        assertFalse(record.passwordIsValid(NEWUSER_PASSWORD_PLAINTEXT));
    }

    public void assertHasNewuser(KAuthFile file)
    {
        UserPwdRecord newuser = file.getUserPwdRecord(NEWUSER_USER);
        assertPasswdEntry(newuser, NEWUSER_USER, NEWUSER_ROOT, NEWUSER_ROOT,
                NEWUSER_HOME, NEWUSER_PASSWORD_HASH, NEWUSER_UID, NEWUSER_GID,
                false);

        assertTrue(newuser.passwordIsValid(NEWUSER_PASSWORD_PLAINTEXT));
        assertFalse(newuser.passwordIsValid(PWDUSER_PASSWORD_PLAINTEXT));
    }

    public void assertPasswdEntry(UserPwdRecord record, String user, String root,
            String fsroot, String home, String hash, int uid, int gid,
            boolean isReadOnly)
    {
        assertNotNull(record);
        assertNull(record.DN);
        assertFalse(record.isDisabled());
        assertFalse(record.isAnonymous());
        assertTrue(record.isValid());
        assertEquals(PASSWD_ENTRY_ID, record.id);
        assertEquals(PASSWD_ENTRY_PRIORITY, record.priority);

        assertEquals(fsroot, record.FsRoot);
        assertEquals(home, record.Home);
        assertEquals(hash, record.Password);
        assertEquals(uid, record.UID);
        assertEquals(gid, record.GIDs.get(0).intValue());
        assertEquals(isReadOnly, record.ReadOnly);
        assertEquals(root, record.Root);
        assertEquals(user, record.Username);
    }

    /**
     * Simulate writing contents of KAuthFile to a file via KAuthFile#save
     * and creating a new KAuthFile from parsing the resulting file.
     *
     * NB.  This method relies on KAuthFile#toString returning the contents
     * of the file.
     * @param in an existing KAuthFile
     * @return the result of parsing the existing KAuthFile's serialised form
     */
    private KAuthFile saveAndParse(KAuthFile in)
    {
        String contents = in.toString();
        byte[] bytes = contents.getBytes(Charset.forName("UTF-8"));
        InputStream input = new ByteArrayInputStream(bytes);
        KAuthFile parsed;

        try {
            parsed = new KAuthFile(input);
        } catch (IOException e) {
            throw new RuntimeException("This should never happen for ByteArrayInputStream", e);
        }

        return parsed;
    }
}
