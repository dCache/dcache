/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package diskCacheV111.doors;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.util.FileExistsCacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.TimeoutCacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.NotDirCacheException;
import diskCacheV111.util.NotFileCacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import org.dcache.namespace.FileType;

import java.util.EnumSet;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
/**
 *
 * @author karsten
 */
public class AbstractFtpDoorV1Test {

    private static final String NEW_DIR = "newdir";
    private static final String OLD_DIR = "olddir";
    private static final String SRC_FILE = "source";
    private static final String DST_FILE = "target";
    private static final String INVALID_FILE = "invalid";

    @Mock AbstractFtpDoorV1 door;
    @Mock PnfsHandler pnfs;

    @Before
    public void setUp()
    {
        MockitoAnnotations.initMocks(this);
        door._pathRoot = new FsPath("pathRoot");
        door._cwd = "/cwd";
        door._pnfs = pnfs;
    }

    @After
    public void tearDown() {
        door = null;
    }

    public static class ExpectedFtpCommandException implements TestRule {

        private int _code;
        private boolean checkCode;

        private ExpectedFtpCommandException() {}

        public static ExpectedFtpCommandException none() {
            return new ExpectedFtpCommandException();
        }

        @Override
        public Statement apply(final Statement stmnt, Description d) {
            return new Statement() {

                @Override
                public void evaluate() throws Throwable {
                    try {
                        stmnt.evaluate();
                    } catch (FTPCommandException commandException) {
                        if (checkCode) {
                            assertEquals(_code, commandException.getCode());
                        }
                    }
                }
            };
        }

        public void expectCode(int code) {
            _code = code;
            checkCode = true;
        }
    }

    @Rule
    public ExpectedFtpCommandException thrown = ExpectedFtpCommandException.none();

    @Test
    public void whenRnfrIsCalledWithEmptyFilenameReplyError500() throws FTPCommandException {
        doCallRealMethod().when(door).ac_rnfr(anyString());

        thrown.expectCode(500);
        door.ac_rnfr("");
    }

    @Test
    public void whenRnfrIsCalledForNonExistingFilenameReplyFileNotFound550()
            throws FTPCommandException, CacheException {
        doCallRealMethod().when(door).ac_rnfr(anyString());
        when(pnfs.getPnfsIdByPath("/pathRoot/cwd/"+INVALID_FILE)).thenThrow(CacheException.class);

        thrown.expectCode(550);
        door.ac_rnfr(INVALID_FILE);
    }

    @Test
    public void whenRntoIsCalledWithEmptyFilenameReplyError500()
            throws FTPCommandException, CacheException {
        doCallRealMethod().when(door).ac_rnfr(anyString());
        doCallRealMethod().when(door).ac_rnto(anyString());
        when(pnfs.getPnfsIdByPath("/pathRoot/cwd/"+SRC_FILE)).thenReturn(new PnfsId("1"));

        door.ac_rnfr(SRC_FILE);
        thrown.expectCode(500);
        door.ac_rnto("");
    }

    @Test
    public void whenRntoIsCalledWithoutPreviousRnfrReplyError503()
            throws FTPCommandException, CacheException {
        doCallRealMethod().when(door).ac_rnto(anyString());
        when(pnfs.getPnfsIdByPath("/pathRoot/cwd/"+DST_FILE)).thenThrow(CacheException.class);

        thrown.expectCode(503);
        door.ac_rnto(DST_FILE);
    }

    @Test
    public void whenRenamingSuccessfulReply250() throws Exception {
        doCallRealMethod().when(door).ac_rnfr(anyString());
        doCallRealMethod().when(door).ac_rnto(anyString());
        when(pnfs.getPnfsIdByPath("/pathRoot/cwd/"+SRC_FILE)).thenReturn(new PnfsId("1"));

        door.ac_rnfr(SRC_FILE);
        door.ac_rnto(DST_FILE);
        InOrder orderedReplies = inOrder(door);
        orderedReplies.verify(door).reply(startsWith("350"));
        orderedReplies.verify(door).reply(startsWith("250"));
    }

    @Test
    public void whenMkdSuccessfulReply257() throws Exception {
        doCallRealMethod().when(door).ac_mkd(anyString());
        door.ac_mkd(NEW_DIR);
        verify(door).reply(startsWith("257 \"/cwd/"+NEW_DIR.replaceAll("\"","\"\"")+"\""));
    }

    @Test
    public void whenMkdPermissionDeniedReply550() throws Exception {
        doCallRealMethod().when(door).ac_mkd(anyString());
        when(pnfs.createPnfsDirectory("/pathRoot/cwd/"+NEW_DIR)).thenThrow(PermissionDeniedCacheException.class);
        thrown.expectCode(550);
        door.ac_mkd(NEW_DIR);
    }

    @Test
    public void whenMkdFileExistReply550() throws Exception {
        doCallRealMethod().when(door).ac_mkd(anyString());
        when(pnfs.createPnfsDirectory("/pathRoot/cwd/"+NEW_DIR)).thenThrow(FileExistsCacheException.class);
        thrown.expectCode(550);
        door.ac_mkd(NEW_DIR);
    }

    @Test
    public void whenMkdTimeOutReply451() throws Exception {
        doCallRealMethod().when(door).ac_mkd(anyString());
        when(pnfs.createPnfsDirectory("/pathRoot/cwd/"+NEW_DIR)).thenThrow(TimeoutCacheException.class);
        thrown.expectCode(451);
        door.ac_mkd(NEW_DIR);
    }

    @Test
    public void whenMkdCacheException550() throws Exception {
        doCallRealMethod().when(door).ac_mkd(anyString());
        when(pnfs.createPnfsDirectory("/pathRoot/cwd/"+NEW_DIR)).thenThrow(CacheException.class);
        thrown.expectCode(550);
        door.ac_mkd(NEW_DIR);
    }

    @Test
    public void whenDelePermissionDeniedReply550() throws Exception {
        doCallRealMethod().when(door).ac_dele(anyString());
        doThrow(new PermissionDeniedCacheException("Permission Denied")).
            when(pnfs).deletePnfsEntry("/pathRoot/cwd/"+SRC_FILE,
                                       EnumSet.of(FileType.REGULAR, FileType.LINK));
        thrown.expectCode(550);
        door.ac_dele(SRC_FILE);
    }

    @Test
    public void whenDeleNotFileReply550() throws Exception {
        doCallRealMethod().when(door).ac_dele(anyString());
        doThrow(new NotFileCacheException("Not a File")).
            when(pnfs).deletePnfsEntry("/pathRoot/cwd/"+SRC_FILE,
                                       EnumSet.of(FileType.REGULAR, FileType.LINK));
        thrown.expectCode(550);
        door.ac_dele(SRC_FILE);
    }

    @Test
    public void whenDeleFileNotFoundReply550() throws Exception {
        doCallRealMethod().when(door).ac_dele(anyString());
        doThrow(new FileNotFoundCacheException("File not found")).
                when(pnfs).deletePnfsEntry("/pathRoot/cwd/"+SRC_FILE,
                                           EnumSet.of(FileType.REGULAR, FileType.LINK));
        thrown.expectCode(550);
        door.ac_dele(SRC_FILE);
    }

    @Test
    public void whenDeleTimeOutReply451() throws Exception {
        doCallRealMethod().when(door).ac_dele(anyString());
        doThrow(new TimeoutCacheException("Timeout")).
            when(pnfs).deletePnfsEntry("/pathRoot/cwd/"+SRC_FILE,
                                       EnumSet.of(FileType.REGULAR, FileType.LINK));
        thrown.expectCode(451);
        door.ac_dele(SRC_FILE);
    }

    @Test
    public void whenDeleCacheExceptionReply550() throws Exception {
        doCallRealMethod().when(door).ac_dele(anyString());
        doThrow(new CacheException("Cache Exception")).
                when(pnfs).deletePnfsEntry("/pathRoot/cwd/"+SRC_FILE,
                                           EnumSet.of(FileType.REGULAR, FileType.LINK));
        thrown.expectCode(550);
        door.ac_dele(SRC_FILE);
    }


    @Test
    public void whenRmdSuccessfulReply250() throws Exception {
        doCallRealMethod().when(door).ac_rmd(anyString());
        door.ac_rmd(OLD_DIR);
        verify(door).reply(startsWith("250"));
    }

    @Test
    public void whenRmdPermissionDeniedReply550() throws Exception {
        doCallRealMethod().when(door).ac_rmd(anyString());
        doThrow(new PermissionDeniedCacheException("Permission denied")).
            when(pnfs).deletePnfsEntry("/pathRoot/cwd/"+OLD_DIR,EnumSet.of(FileType.DIR));
        thrown.expectCode(550);
        door.ac_rmd(OLD_DIR);
    }

    @Test
    public void whenRmdNotDirReply550() throws Exception {
        doCallRealMethod().when(door).ac_rmd(anyString());
        doThrow(new NotDirCacheException("Not a directory")).
                when(pnfs).deletePnfsEntry("/pathRoot/cwd/"+OLD_DIR,
                                           EnumSet.of(FileType.DIR));
        thrown.expectCode(550);
        door.ac_rmd(OLD_DIR);
    }

    @Test
    public void whenRmdFileNotFoundReply550() throws Exception {
        doCallRealMethod().when(door).ac_rmd(anyString());
        doThrow(new FileNotFoundCacheException("No such file or directory")).
                when(pnfs).deletePnfsEntry("/pathRoot/cwd/"+OLD_DIR,
                                           EnumSet.of(FileType.DIR));
        thrown.expectCode(550);
        door.ac_rmd(OLD_DIR);
    }

    @Test
    public void whenRmdTimeOutReply451() throws Exception {
        doCallRealMethod().when(door).ac_rmd(anyString());
        doThrow(new TimeoutCacheException("Timeout")).
            when(pnfs).deletePnfsEntry("/pathRoot/cwd/"+OLD_DIR,
                                       EnumSet.of(FileType.DIR));
        thrown.expectCode(451);
        door.ac_rmd(OLD_DIR);
    }

    @Test
    public void whenRmdCacheExceptionReply550() throws Exception {
        doCallRealMethod().when(door).ac_rmd(anyString());
        doThrow(new CacheException("Cache Exception")).
            when(pnfs).deletePnfsEntry("/pathRoot/cwd/"+OLD_DIR,
                                       EnumSet.of(FileType.DIR));
        thrown.expectCode(550);
        door.ac_rmd(OLD_DIR);
}
}
