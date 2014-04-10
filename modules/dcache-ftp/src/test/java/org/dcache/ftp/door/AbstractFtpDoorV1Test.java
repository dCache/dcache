package org.dcache.ftp.door;

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;

import java.net.Inet4Address;
import java.net.InetSocketAddress;
import java.net.InterfaceAddress;
import java.net.UnknownHostException;
import java.util.EnumSet;
import java.util.List;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileExistsCacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.NotDirCacheException;
import diskCacheV111.util.NotFileCacheException;
import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.TimeoutCacheException;

import org.dcache.namespace.FileType;
import org.dcache.util.PortRange;

import static com.google.common.net.InetAddresses.forString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@SuppressWarnings("unchecked")
public class AbstractFtpDoorV1Test {

    private static final String NEW_DIR = "newdir";
    private static final String OLD_DIR = "olddir";
    private static final String SRC_FILE = "source";
    private static final String DST_FILE = "target";
    private static final String INVALID_FILE = "invalid";

    @Mock
    AbstractFtpDoorV1 door;
    @Mock
    PnfsHandler pnfs;
    @Mock
    Logger logger;

    @Before
    public void setUp()
    {
        MockitoAnnotations.initMocks(this);
        door._userRootPath = new FsPath("pathRoot");
        door._doorRootPath = new FsPath("pathRoot");
        door._cwd = "/cwd";
        door._pnfs = pnfs;
    }

    @After
    public void tearDown() {
        door = null;
    }

    private InterfaceAddress mockInterfaceAddress(String address)
    {
        InterfaceAddress mock = mock(InterfaceAddress.class);
        Mockito.when(mock.getAddress()).thenReturn(forString(address));
        return mock;
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
                        if (checkCode)
                            fail("Expected FTPCommandException '"+_code+"' not thrown.");
                    } catch (FTPCommandException commandException) {
                        if (checkCode) {
                            assertEquals("Unexpected reply '"+commandException.getCode()+" "+commandException.getReply()+"'", _code, commandException.getCode());
                        } else {
                            fail("Caught unexpected exception FTPCommandException '" + _code + "':'"+commandException.getMessage()+"'.");
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
        doCallRealMethod().when(door).ftp_rnfr(anyString());

        thrown.expectCode(500);
        door.ftp_rnfr("");
    }

    @Test
    public void whenRnfrIsCalledForNonExistingFilenameReplyFileNotFound550()
            throws FTPCommandException, CacheException {
        doCallRealMethod().when(door).ftp_rnfr(anyString());
        when(pnfs.getPnfsIdByPath("/pathRoot/cwd/"+INVALID_FILE)).thenThrow(FileNotFoundCacheException.class);

        thrown.expectCode(550);
        door.ftp_rnfr(INVALID_FILE);
    }

    @Test
    public void whenRntoIsCalledWithEmptyFilenameReplyError500()
            throws FTPCommandException, CacheException {
        doCallRealMethod().when(door).ftp_rnfr(anyString());
        doCallRealMethod().when(door).ftp_rnto(anyString());
        when(pnfs.getPnfsIdByPath("/pathRoot/cwd/"+SRC_FILE)).thenReturn(new PnfsId("1"));

        door.ftp_rnfr(SRC_FILE);
        thrown.expectCode(500);
        door.ftp_rnto("");
    }

    @Test
    public void whenRntoIsCalledWithoutPreviousRnfrReplyError503()
            throws FTPCommandException, CacheException {
        doCallRealMethod().when(door).ftp_rnto(anyString());
        when(pnfs.getPnfsIdByPath("/pathRoot/cwd/"+DST_FILE)).thenThrow(CacheException.class);

        thrown.expectCode(503);
        door.ftp_rnto(DST_FILE);
    }

    @Test
    public void whenRenamingSuccessfulReply250() throws Exception {
        doCallRealMethod().when(door).ftp_rnfr(anyString());
        doCallRealMethod().when(door).ftp_rnto(anyString());
        when(pnfs.getPnfsIdByPath("/pathRoot/cwd/"+SRC_FILE)).thenReturn(new PnfsId("1"));

        door.ftp_rnfr(SRC_FILE);
        door.ftp_rnto(DST_FILE);
        InOrder orderedReplies = inOrder(door);
        orderedReplies.verify(door).reply(startsWith("350"));
        orderedReplies.verify(door).reply(startsWith("250"));
    }

    @Test
    public void EPRTshouldReply200ForValidIP4Arg()
            throws FTPCommandException {
        doCallRealMethod().when(door).ftp_eprt(anyString());
        doCallRealMethod().when(door).getExtendedAddressOf(anyString());
        doCallRealMethod().when(door).ok(anyString());
        door._localAddress = new InetSocketAddress(forString("::1"), 21);

        door.ftp_eprt("|1|127.0.0.1|22|");

        verify(door).reply(startsWith("200"));
    }

    @Test
    public void EPRTshouldReply200ForValidIP6Arg()
            throws FTPCommandException {
        doCallRealMethod().when(door).ftp_eprt(anyString());
        doCallRealMethod().when(door).getExtendedAddressOf(anyString());
        doCallRealMethod().when(door).ok(anyString());
        door._localAddress = new InetSocketAddress(forString("::1"), 21);

        door.ftp_eprt("|2|::1|22|");

        verify(door).reply(startsWith("200"));
    }

    @Test
    public void EPRTshouldReply501ForInvalidIP4Arg()
            throws FTPCommandException {
        doCallRealMethod().when(door).ftp_eprt(anyString());
        doCallRealMethod().when(door).getExtendedAddressOf(anyString());
        door._localAddress = new InetSocketAddress(forString("::1"), 21);

        thrown.expectCode(501);
        door.ftp_eprt("|1|999.0.0.0|22|");
    }

    @Test
    public void EPRTshouldReply501ForInvalidIP6Arg()
            throws FTPCommandException {
        doCallRealMethod().when(door).ftp_eprt(anyString());
        doCallRealMethod().when(door).getExtendedAddressOf(anyString());
        door._localAddress = new InetSocketAddress(forString("::1"), 21);

        thrown.expectCode(501);
        door.ftp_eprt("|2|:999999::1|22|");
    }

    @Test
    public void EPRTshouldReply522ForMissingProtocolArg()
            throws FTPCommandException {
        doCallRealMethod().when(door).ftp_eprt(anyString());
        doCallRealMethod().when(door).getExtendedAddressOf(anyString());
        door._localAddress = new InetSocketAddress(forString("::1"), 21);

        thrown.expectCode(501);
        door.ftp_eprt("|127.0.0.1|22|");
    }

    @Test
    public void EPRTshouldReply522ForEmptyProtocolArg()
            throws FTPCommandException {
        doCallRealMethod().when(door).ftp_eprt(anyString());
        doCallRealMethod().when(door).getExtendedAddressOf(anyString());
        door._localAddress = new InetSocketAddress(forString("::1"), 21);

        thrown.expectCode(522);
        door.ftp_eprt("||127.0.0.1|22|");
    }

    @Test
    public void EPRTshouldReply501ForMissingArg()
            throws FTPCommandException {
        doCallRealMethod().when(door).ftp_eprt(anyString());
        doCallRealMethod().when(door).getExtendedAddressOf(anyString());
        door._localAddress = new InetSocketAddress(forString("::1"), 21);

        thrown.expectCode(501);
        door.ftp_eprt("");
    }

    @Test
    public void EPRTshouldReply501ForTooManyArgs()
            throws FTPCommandException {
        doCallRealMethod().when(door).ftp_eprt(anyString());
        doCallRealMethod().when(door).getExtendedAddressOf(anyString());
        door._localAddress = new InetSocketAddress(forString("::1"), 21);

        thrown.expectCode(501);
        door.ftp_eprt("||||1|||127.0.0.1||22||||");
    }

    @Test
    public void EPRTshouldReply522ForUnsupportedProtocol()
            throws FTPCommandException {
        doCallRealMethod().when(door).ftp_eprt(anyString());
        doCallRealMethod().when(door).getExtendedAddressOf(anyString());
        door._localAddress = new InetSocketAddress(forString("::1"), 21);

        thrown.expectCode(522);
        door.ftp_eprt("|3|127.0.0.1|22|");
    }

    @Test
    public void EPSVshouldReply200WhenConnectionEstablished()
            throws FTPCommandException, UnknownHostException {
        doCallRealMethod().when(door).ftp_epsv(anyString());
        door._localAddress = new InetSocketAddress(forString("::1"), 21);
        when(door.setPassive()).thenReturn(new InetSocketAddress(forString("::1"), 20));

        door.ftp_epsv("");

        verify(door).reply(startsWith("229"));
    }

    @Test
    public void EPSVshouldReply522WhenRequestingInvalidProtocol()
            throws FTPCommandException, UnknownHostException {
        doCallRealMethod().when(door).ftp_epsv(anyString());
        door._localAddress = new InetSocketAddress(forString("::1"), 21);
        when(door.setPassive()).thenReturn(new InetSocketAddress(forString("::1"), 20));

        thrown.expectCode(522);
        door.ftp_epsv("3");
    }

    @Test
    public void EPSVshouldReply200WhenRequestingAll()
            throws FTPCommandException, UnknownHostException
    {
        doCallRealMethod().when(door).ftp_epsv(anyString());
        doCallRealMethod().when(door).ok(anyString());
        door._localAddress = new InetSocketAddress(forString("::1"), 21);
        when(door.setPassive()).thenReturn(new InetSocketAddress(forString("::1"), 20));

        door.ftp_epsv("all");

        verify(door).reply(startsWith("200"));
    }

    @Test
    public void EPSVshouldReply229WhenRequestingAllFollowedByEPSVwithoutArgument()
            throws FTPCommandException, UnknownHostException
    {
        doCallRealMethod().when(door).ftp_epsv(anyString());
        doCallRealMethod().when(door).ok(anyString());
        door._localAddress = new InetSocketAddress(forString("::1"), 21);
        when(door.setPassive()).thenReturn(new InetSocketAddress(forString("::1"), 20));

        door.ftp_epsv("all");

        door.ftp_epsv("");

        verify(door).reply(startsWith("229"));
    }

    @Test
    public void PASVshouldBeRejectedAfterEPSVallCall()
            throws FTPCommandException, UnknownHostException
    {
        doCallRealMethod().when(door).ftp_epsv(anyString());
        doCallRealMethod().when(door).ftp_pasv(anyString());
        door._localAddress = new InetSocketAddress(forString("::1"), 21);
        when(door.setPassive()).thenReturn(new InetSocketAddress(forString("::1"), 20));

        door.ftp_epsv("all");
        thrown.expectCode(503);
        door.ftp_pasv("192,168,1,1,6666");
    }

    @Test
    public void PORTshouldBeRejectedAfterEPSVallCall()
            throws FTPCommandException, UnknownHostException
    {
        doCallRealMethod().when(door).ftp_epsv(anyString());
        doCallRealMethod().when(door).ftp_port(anyString());
        doCallRealMethod().when(door).setActive((InetSocketAddress)any());
        door._localAddress = new InetSocketAddress(forString("::1"), 21);
        when(door.setPassive()).thenReturn(new InetSocketAddress(forString("::1"), 20));

        door.ftp_epsv("all");
        thrown.expectCode(503);
        door.ftp_port("192,168,1,1,0,20");
    }

    @Test
    public void EPRTshouldBeRejectedAfterEPSVallCall()
            throws FTPCommandException, UnknownHostException
    {
        doCallRealMethod().when(door).ftp_epsv(anyString());
        doCallRealMethod().when(door).ftp_eprt(anyString());
        doCallRealMethod().when(door).setActive((InetSocketAddress)any());
        door._localAddress = new InetSocketAddress(forString("::1"), 21);
        when(door.setPassive()).thenReturn(new InetSocketAddress(forString("::1"), 20));

        door.ftp_epsv("all");
        thrown.expectCode(503);
        door.ftp_eprt("|3|127.0.0.1|22|");
    }

    @Test
    public void EPSVshouldRebindIpWhenRequestedIPv4Protocol()
            throws Exception {
        doCallRealMethod().when(door).ftp_epsv(anyString());
        doCallRealMethod().when(door).setPassive();
        List<InterfaceAddress> addresses = Lists.newArrayList(
                mockInterfaceAddress("::1"), mockInterfaceAddress("127.0.0.1"));
        when(door.getLocalAddressInterfaces()).thenReturn(addresses);
        door._localAddress = new InetSocketAddress(forString("::1"), 21);
        door._passiveModePortRange = new PortRange(0);

        door.ftp_epsv("1");
        door.ftp_epsv("");

        assertThat(door._preferredProtocol, is(AbstractFtpDoorV1.Protocol.IPV4));
        assertEquals(Inet4Address.class, ((InetSocketAddress)door._passiveModeServerSocket.getLocalAddress()).getAddress().getClass());
    }

    @Test
    public void EPSVshouldReply522WhenRequestedUnsupportedProtocol()
            throws FTPCommandException {
        doCallRealMethod().when(door).ftp_epsv(anyString());
        door._localAddress = new InetSocketAddress(forString("::1"), 21);

        thrown.expectCode(522);
        door.ftp_epsv("3");
    }

    @Test
    public void EPSVshouldReply500WhenRequestedOnIpV4()
            throws FTPCommandException {
        doCallRealMethod().when(door).ftp_epsv(anyString());
        door._localAddress = new InetSocketAddress(forString("127.0.0.1"), 21);
        thrown.expectCode(502);
        door.ftp_epsv("1");
    }

    @Test
    public void EPRTshouldReply500WhenRequestedOnIpV4()
            throws FTPCommandException {
        doCallRealMethod().when(door).ftp_eprt(anyString());
        door._localAddress = new InetSocketAddress(forString("127.0.0.1"), 21);
        thrown.expectCode(502);
        door.ftp_eprt("|3|127.0.0.1|22|");
    }

    @Test
    public void whenMkdSuccessfulReply257() throws Exception {
        doCallRealMethod().when(door).ftp_mkd(anyString());
        door.ftp_mkd(NEW_DIR);
        verify(door).reply(startsWith("257 \"/cwd/"+NEW_DIR.replaceAll("\"","\"\"")+"\""));
    }

    @Test
    public void whenMkdPermissionDeniedReply550() throws Exception {
        doCallRealMethod().when(door).ftp_mkd(anyString());
        when(pnfs.createPnfsDirectory("/pathRoot/cwd/"+NEW_DIR)).thenThrow(PermissionDeniedCacheException.class);
        thrown.expectCode(550);
        door.ftp_mkd(NEW_DIR);
    }

    @Test
    public void whenMkdFileExistReply550() throws Exception {
        doCallRealMethod().when(door).ftp_mkd(anyString());
        when(pnfs.createPnfsDirectory("/pathRoot/cwd/"+NEW_DIR)).thenThrow(FileExistsCacheException.class);
        thrown.expectCode(550);
        door.ftp_mkd(NEW_DIR);
    }

    @Test
    public void whenMkdTimeOutReply451() throws Exception {
        doCallRealMethod().when(door).ftp_mkd(anyString());
        when(pnfs.createPnfsDirectory("/pathRoot/cwd/"+NEW_DIR)).thenThrow(TimeoutCacheException.class);
        thrown.expectCode(451);
        door.ftp_mkd(NEW_DIR);
    }

    @Test
    public void whenMkdCacheException550() throws Exception {
        doCallRealMethod().when(door).ftp_mkd(anyString());
        when(pnfs.createPnfsDirectory("/pathRoot/cwd/"+NEW_DIR)).thenThrow(CacheException.class);
        thrown.expectCode(550);
        door.ftp_mkd(NEW_DIR);
    }

    @Test
    public void whenDelePermissionDeniedReply550() throws Exception {
        doCallRealMethod().when(door).ftp_dele(anyString());
        doThrow(new PermissionDeniedCacheException("Permission Denied")).
            when(pnfs).deletePnfsEntry("/pathRoot/cwd/"+SRC_FILE,
                                       EnumSet.of(FileType.REGULAR, FileType.LINK));
        thrown.expectCode(550);
        door.ftp_dele(SRC_FILE);
    }

    @Test
    public void whenDeleNotFileReply550() throws Exception {
        doCallRealMethod().when(door).ftp_dele(anyString());
        doThrow(new NotFileCacheException("Not a File")).
            when(pnfs).deletePnfsEntry("/pathRoot/cwd/"+SRC_FILE,
                                       EnumSet.of(FileType.REGULAR, FileType.LINK));
        thrown.expectCode(550);
        door.ftp_dele(SRC_FILE);
    }

    @Test
    public void whenDeleFileNotFoundReply550() throws Exception {
        doCallRealMethod().when(door).ftp_dele(anyString());
        doThrow(new FileNotFoundCacheException("File not found")).
                when(pnfs).deletePnfsEntry("/pathRoot/cwd/"+SRC_FILE,
                                           EnumSet.of(FileType.REGULAR, FileType.LINK));
        thrown.expectCode(550);
        door.ftp_dele(SRC_FILE);
    }

    @Test
    public void whenDeleTimeOutReply451() throws Exception {
        doCallRealMethod().when(door).ftp_dele(anyString());
        doThrow(new TimeoutCacheException("Timeout")).
            when(pnfs).deletePnfsEntry("/pathRoot/cwd/"+SRC_FILE,
                                       EnumSet.of(FileType.REGULAR, FileType.LINK));
        thrown.expectCode(451);
        door.ftp_dele(SRC_FILE);
    }

    @Test
    public void whenDeleCacheExceptionReply550() throws Exception {
        doCallRealMethod().when(door).ftp_dele(anyString());
        doThrow(new CacheException("Cache Exception")).
                when(pnfs).deletePnfsEntry("/pathRoot/cwd/" + SRC_FILE,
                EnumSet.of(FileType.REGULAR, FileType.LINK));
        thrown.expectCode(550);
        door.ftp_dele(SRC_FILE);
    }


    @Test
    public void whenRmdSuccessfulReply250() throws Exception {
        doCallRealMethod().when(door).ftp_rmd(anyString());
        door.ftp_rmd(OLD_DIR);
        verify(door).reply(startsWith("250"));
    }

    @Test
    public void whenRmdPermissionDeniedReply550() throws Exception {
        doCallRealMethod().when(door).ftp_rmd(anyString());
        doThrow(new PermissionDeniedCacheException("Permission denied")).
            when(pnfs).deletePnfsEntry("/pathRoot/cwd/"+OLD_DIR,EnumSet.of(FileType.DIR));
        thrown.expectCode(550);
        door.ftp_rmd(OLD_DIR);
    }

    @Test
    public void whenRmdNotDirReply550() throws Exception {
        doCallRealMethod().when(door).ftp_rmd(anyString());
        doThrow(new NotDirCacheException("Not a directory")).
                when(pnfs).deletePnfsEntry("/pathRoot/cwd/"+OLD_DIR,
                                           EnumSet.of(FileType.DIR));
        thrown.expectCode(550);
        door.ftp_rmd(OLD_DIR);
    }

    @Test
    public void whenRmdFileNotFoundReply550() throws Exception {
        doCallRealMethod().when(door).ftp_rmd(anyString());
        doThrow(new FileNotFoundCacheException("No such file or directory")).
                when(pnfs).deletePnfsEntry("/pathRoot/cwd/"+OLD_DIR,
                                           EnumSet.of(FileType.DIR));
        thrown.expectCode(550);
        door.ftp_rmd(OLD_DIR);
    }

    @Test
    public void whenRmdTimeOutReply451() throws Exception {
        doCallRealMethod().when(door).ftp_rmd(anyString());
        doThrow(new TimeoutCacheException("Timeout")).
            when(pnfs).deletePnfsEntry("/pathRoot/cwd/"+OLD_DIR,
                                       EnumSet.of(FileType.DIR));
        thrown.expectCode(451);
        door.ftp_rmd(OLD_DIR);
    }

    @Test
    public void whenRmdCacheExceptionReply550() throws Exception {
        doCallRealMethod().when(door).ftp_rmd(anyString());
        doThrow(new CacheException("Cache Exception")).
            when(pnfs).deletePnfsEntry("/pathRoot/cwd/" + OLD_DIR,
                EnumSet.of(FileType.DIR));
        thrown.expectCode(550);
        door.ftp_rmd(OLD_DIR);
    }
}
