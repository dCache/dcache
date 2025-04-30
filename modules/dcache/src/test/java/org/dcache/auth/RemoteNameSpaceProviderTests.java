package org.dcache.auth;

import static com.google.common.base.Preconditions.checkState;
import static diskCacheV111.util.CacheException.FILE_EXISTS;
import static diskCacheV111.util.CacheException.FILE_NOT_FOUND;
import static diskCacheV111.util.CacheException.NOT_FILE;
import static org.dcache.auth.Subjects.ROOT;
import static org.dcache.namespace.FileAttribute.SIZE;
import static org.dcache.namespace.FileAttribute.TYPE;
import static org.dcache.namespace.FileType.REGULAR;
import static org.dcache.util.FileAttributesBuilder.fileAttributes;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.BDDMockito.any;
import static org.mockito.BDDMockito.anyLong;
import static org.mockito.BDDMockito.doAnswer;
import static org.mockito.BDDMockito.mock;
import static org.mockito.BDDMockito.never;
import static org.mockito.BDDMockito.verify;
import static org.mockito.BDDMockito.willAnswer;

import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import diskCacheV111.namespace.NameSpaceProvider.Link;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileExistsCacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.NotFileCacheException;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.PnfsAddCacheLocationMessage;
import diskCacheV111.vehicles.PnfsClearCacheLocationMessage;
import diskCacheV111.vehicles.PnfsCreateEntryMessage;
import diskCacheV111.vehicles.PnfsDeleteEntryMessage;
import diskCacheV111.vehicles.PnfsFlagMessage;
import diskCacheV111.vehicles.PnfsFlagMessage.FlagOperation;
import diskCacheV111.vehicles.PnfsGetCacheLocationsMessage;
import diskCacheV111.vehicles.PnfsGetParentMessage;
import diskCacheV111.vehicles.PnfsMapPathMessage;
import diskCacheV111.vehicles.PnfsMessage;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageAnswerable;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.SerializationException;
import dmg.cells.nucleus.SerializationHandler;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import org.dcache.cells.CellStub;
import org.dcache.namespace.FileAttribute;
import org.dcache.namespace.FileType;
import org.dcache.namespace.ListHandler;
import org.dcache.util.ChecksumType;
import org.dcache.util.list.DirectoryEntry;
import org.dcache.util.list.LabelsListHandler;
import org.dcache.util.list.ListDirectoryHandler;
import org.dcache.util.list.VirtualDirectoryListHandler;
import org.dcache.vehicles.FileAttributes;
import org.dcache.vehicles.PnfsGetFileAttributes;
import org.dcache.vehicles.PnfsListDirectoryMessage;
import org.dcache.vehicles.PnfsRemoveChecksumMessage;
import org.dcache.vehicles.PnfsSetFileAttributes;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;

/**
 * Check that the RemoteNameSpaceProvider implementation sends messages of the correct type and
 * behaves as expected when receiving reply messages.
 * <p>
 * The namespace operations come in two types: those that require a reply message (and so, will wait
 * for one or more messages) and those that are single-shot (and so, do not wait for a reply
 * message).
 * <p>
 * For the single-shot operations, this class only checks that the outgoing message is correctly
 * formed.
 * <p>
 * For those operations that wait for a reply messages, there are two groups of tests: those that
 * simulate a successful outcome and those that simulate various failure modes.
 * <p>
 * For the successful outcomes, this class check both the outgoing message is correctly formed and
 * that the reply message is processed correctly.  If the tests could lead to false-success (e.g.,
 * works-by-accident) then the successful outcome tests may be repeated with different arguments.
 * <p>
 * For the failure mode tests, this class checks only that the reply message is process correctly.
 * This is because the outgoing message is checked as part of the test(s) that check this operation
 * when it is successful.
 * <p>
 * Tests will send a single message and receive zero or more messages.  The reply messages are
 * derived from the outgoing message by serialising and deserialising (similar to intra-domain
 * communication).  This allows the "given" statements in a test to describe the changes to the
 * out-going PnfsMessage rather that describing a completely new PnfsMessage.
 */
public class RemoteNameSpaceProviderTests {

    private static final Range<Integer> ALL_ENTRIES = Range.all();

    private static final CellPath CELLPATH_PNFSMANAGER =
          new CellPath("PnfsManager");

    private static final PnfsId A_PNFSID =
          new PnfsId("0123456789abcdef0123456789abcdef0123");
    private static final PnfsId ANOTHER_PNFSID =
          new PnfsId("fedcba9876543210fedcba9876543210fedc");
    private static final PnfsId PNFSID_3 =
          new PnfsId("00112233445566778899aabbccddeeff0011");
    private static final PnfsId PNFSID_4 =
          new PnfsId("ffeeddccbbaa99887766554433221100ffee");

    private static final Modifier SUCCESSFUL = new Modifier() {
        @Override
        public void modify(PnfsMessage message) {
            message.setSucceeded();
        }
    };

    RemoteNameSpaceProvider _namespace;
    CellEndpoint _endpoint;
    ListDirectoryHandler _listHandler;
    VirtualDirectoryListHandler _virtualDirectoryHandler;
    LabelsListHandler _labelsListHandler;



    @Before
    public void setup() throws NoSuchMethodException {
        _endpoint = mock(CellEndpoint.class);
        CellStub stub = new CellStub(_endpoint, CELLPATH_PNFSMANAGER);
        PnfsHandler pnfs = new PnfsHandler(stub);
        _listHandler = new ListDirectoryHandler(pnfs);
        _namespace = new RemoteNameSpaceProvider(pnfs, _listHandler, _virtualDirectoryHandler, _labelsListHandler);
    }


    @Test
    public void shouldSucceedWhenAddCacheLocationSuccessfully() throws Exception {
        givenSuccessfulResponse();

        _namespace.addCacheLocation(ROOT, A_PNFSID, "pool-1");

        PnfsAddCacheLocationMessage sent =
              getSingleSendAndWaitMessage(PnfsAddCacheLocationMessage.class);
        assertThat(sent.getReplyRequired(), is(true));
        assertThat(sent.getSubject(), is(ROOT));
        assertThat(sent.getPnfsId(), is(A_PNFSID));
        assertThat(sent.getPoolName(), is("pool-1"));
    }


    @Test(expected = FileNotFoundCacheException.class)
    public void shouldThrowExceptionWhenAddCacheLocationForMissingFile()
          throws Exception {
        givenFailureResponse(FILE_NOT_FOUND);

        _namespace.addCacheLocation(ROOT, A_PNFSID, "pool-1");
    }


    @Test
    public void shouldSucceedWhenClearCacheLocationRemoveLast() throws Exception {
        givenSuccessfulResponse();

        _namespace.clearCacheLocation(ROOT, A_PNFSID, "pool-1", true);

        PnfsClearCacheLocationMessage sent =
              getSingleSendAndWaitMessage(PnfsClearCacheLocationMessage.class);
        assertThat(sent.getReplyRequired(), is(true));
        assertThat(sent.getSubject(), is(ROOT));
        assertThat(sent.getPnfsId(), is(A_PNFSID));
        assertThat(sent.getPoolName(), is("pool-1"));
        assertThat(sent.removeIfLast(), is(true));
    }


    @Test
    public void shouldSucceedWhenClearCacheLocationWithoutRemoveLast()
          throws Exception {
        givenSuccessfulResponse();

        _namespace.clearCacheLocation(ROOT, A_PNFSID, "pool-1", false);

        PnfsClearCacheLocationMessage sent =
              getSingleSendAndWaitMessage(PnfsClearCacheLocationMessage.class);
        assertThat(sent.getReplyRequired(), is(true));
        assertThat(sent.getSubject(), is(ROOT));
        assertThat(sent.getPnfsId(), is(A_PNFSID));
        assertThat(sent.getPoolName(), is("pool-1"));
        assertThat(sent.removeIfLast(), is(false));
    }


    @Test
    public void shouldSucceedWhenCreatingFile() throws Exception {
        givenSuccessfulResponse();

        _namespace.createFile(ROOT, "/path/to/file",
              FileAttributes.of().uid(100).gid(200).mode(0644).build(),
              EnumSet.noneOf(FileAttribute.class));

        PnfsCreateEntryMessage sent =
              getSingleSendAndWaitMessage(PnfsCreateEntryMessage.class);
        assertThat(sent.getReplyRequired(), is(true));
        assertThat(sent.getSubject(), is(ROOT));
        assertThat(sent.getPnfsPath(), is("/path/to/file"));
        assertThat(sent.getFileAttributes().getOwner(), is(100));
        assertThat(sent.getFileAttributes().getGroup(), is(200));
        assertThat(sent.getFileAttributes().getMode(), is(0644));
    }


    @Test(expected = FileExistsCacheException.class)
    public void shouldFailWhenCreatingExistingFile() throws Exception {
        givenFailureResponse(FILE_EXISTS);

        _namespace.createFile(ROOT, "/path/to/file",
              FileAttributes.of().uid(100).gid(200).mode(0644).build(),
              EnumSet.noneOf(FileAttribute.class));
    }


    @Test
    public void shouldSucceedWhenCreatingDirectory() throws Exception {
        givenSuccessfulResponse();

        _namespace.createDirectory(ROOT, "/path/to/dir",
              FileAttributes.of().uid(100).gid(200).mode(0755).build());

        PnfsCreateEntryMessage sent =
              getSingleSendAndWaitMessage(PnfsCreateEntryMessage.class);
        assertThat(sent.getReplyRequired(), is(true));
        assertThat(sent.getSubject(), is(ROOT));
        assertThat(sent.getPnfsPath(), is("/path/to/dir"));
        assertThat(sent.getFileAttributes().getOwner(), is(100));
        assertThat(sent.getFileAttributes().getGroup(), is(200));
        assertThat(sent.getFileAttributes().getMode(), is(0755));
    }


    @Test
    public void shouldSucceedWhenDeletingEntryByPnfsid() throws Exception {
        givenSuccessfulResponse();

        _namespace.deleteEntry(ROOT, EnumSet.allOf(FileType.class), A_PNFSID,
              EnumSet.noneOf(FileAttribute.class));

        PnfsDeleteEntryMessage sent =
              getSingleSendAndWaitMessage(PnfsDeleteEntryMessage.class);
        assertThat(sent.getReplyRequired(), is(true));
        assertThat(sent.getSubject(), is(ROOT));
        assertThat(sent.getPnfsPath(), nullValue());
        assertThat(sent.getPnfsId(), is(A_PNFSID));
    }


    @Test
    public void shouldSucceedWhenDeletingEntryByPath() throws Exception {
        givenSuccessfulResponse();

        _namespace.deleteEntry(ROOT, EnumSet.allOf(FileType.class), "/path/to/entry",
              EnumSet.noneOf(FileAttribute.class));

        PnfsDeleteEntryMessage sent =
              getSingleSendAndWaitMessage(PnfsDeleteEntryMessage.class);
        assertThat(sent.getReplyRequired(), is(true));
        assertThat(sent.getSubject(), is(ROOT));
        assertThat(sent.getPnfsPath(), is("/path/to/entry"));
        assertThat(sent.getPnfsId(), nullValue());
    }


    @Test(expected = FileNotFoundCacheException.class)
    public void shouldFailWhenDeletingNonexistingEntryByPnfsid() throws Exception {
        givenFailureResponse(FILE_NOT_FOUND);

        _namespace.deleteEntry(ROOT, EnumSet.allOf(FileType.class), A_PNFSID,
              EnumSet.noneOf(FileAttribute.class));
    }


    @Test(expected = FileNotFoundCacheException.class)
    public void shouldFailWhenDeletingNonexistingEntryByPath() throws Exception {
        givenFailureResponse(FILE_NOT_FOUND);

        _namespace.deleteEntry(ROOT, EnumSet.allOf(FileType.class), "/path/to/file",
              EnumSet.noneOf(FileAttribute.class));
    }


    @Test
    public void shouldSucceedWhenGetCacheLocationForFileWithNoLocations()
          throws Exception {
        givenSuccessfulResponse();

        List<String> locations = _namespace.getCacheLocation(ROOT, A_PNFSID);

        PnfsGetCacheLocationsMessage sent =
              getSingleSendAndWaitMessage(PnfsGetCacheLocationsMessage.class);
        assertThat(sent.getReplyRequired(), is(true));
        assertThat(sent.getSubject(), is(ROOT));
        assertThat(sent.getPnfsId(), is(A_PNFSID));

        assertThat(locations, hasSize(0));  // The empty() Matcher is broken.
    }


    @Test(expected = FileNotFoundCacheException.class)
    public void shouldFailWhenGetCacheLocationForFileThatDoesNotExist()
          throws Exception {
        givenFailureResponse(FILE_NOT_FOUND);

        _namespace.getCacheLocation(ROOT, A_PNFSID);
    }


    @Test(expected = NotFileCacheException.class)
    public void shouldFailWhenGetCacheLocationForADirectory()
          throws Exception {
        givenFailureResponse(NOT_FILE);

        _namespace.getCacheLocation(ROOT, A_PNFSID);
    }


    @Test
    public void shouldSucceedWhenGetCacheLocationForFileWithOneLocation()
          throws Exception {
        givenSuccessfulResponse((Modifier<PnfsGetCacheLocationsMessage>)
              (r) -> r.setCacheLocations(Collections.singletonList("pool-1")));

        List<String> locations = _namespace.getCacheLocation(ROOT, A_PNFSID);

        PnfsGetCacheLocationsMessage sent =
              getSingleSendAndWaitMessage(PnfsGetCacheLocationsMessage.class);
        assertThat(sent.getReplyRequired(), is(true));
        assertThat(sent.getSubject(), is(ROOT));
        assertThat(sent.getPnfsId(), is(A_PNFSID));

        assertThat(locations, hasSize(1));
        assertThat(locations, hasItem("pool-1"));
    }


    @Test
    public void shouldSucceedWhenGetCacheLocationForFileWithTwoLocations()
          throws Exception {
        givenSuccessfulResponse((Modifier<PnfsGetCacheLocationsMessage>)
              (r) -> r.setCacheLocations(Lists.newArrayList("pool-1", "pool-2")));

        List<String> locations = _namespace.getCacheLocation(ROOT, A_PNFSID);

        PnfsGetCacheLocationsMessage sent =
              getSingleSendAndWaitMessage(PnfsGetCacheLocationsMessage.class);
        assertThat(sent.getReplyRequired(), is(true));
        assertThat(sent.getSubject(), is(ROOT));
        assertThat(sent.getPnfsId(), is(A_PNFSID));

        assertThat(locations, hasSize(2));
        assertThat(locations, hasItem("pool-1"));
        assertThat(locations, hasItem("pool-2"));
    }


    @Test
    public void shouldSucceedWhenGetFileAttributesForExistingEntry()
          throws Exception {
        givenSuccessfulResponse((Modifier<PnfsGetFileAttributes>)
              (r) -> r.setFileAttributes(
                    fileAttributes().withSize(1234L).withType(REGULAR).build()));

        FileAttributes attributes =
              _namespace.getFileAttributes(ROOT, A_PNFSID, EnumSet.of(TYPE, SIZE));

        PnfsGetFileAttributes sent =
              getSingleSendAndWaitMessage(PnfsGetFileAttributes.class);
        assertThat(sent.getReplyRequired(), is(true));
        assertThat(sent.getSubject(), is(ROOT));
        assertThat(sent.getPnfsId(), is(A_PNFSID));
        assertThat(sent.getRequestedAttributes(), hasSize(2));
        assertThat(sent.getRequestedAttributes(), hasItem(TYPE));
        assertThat(sent.getRequestedAttributes(), hasItem(SIZE));

        assertThat(attributes.getSize(), is(1234L));
        assertThat(attributes.getFileType(), is(REGULAR));
    }


    @Test(expected = FileNotFoundCacheException.class)
    public void shouldFaileWhenGetFileAttributesForMissingEntry()
          throws Exception {
        givenFailureResponse(FILE_NOT_FOUND);

        _namespace.getFileAttributes(ROOT, A_PNFSID, EnumSet.of(TYPE, SIZE));
    }


    @Test
    public void shouldSucceedForFindExistingEntry() throws Exception {
        givenSuccessfulResponse((Modifier<PnfsGetParentMessage>)
              (r) -> r.addLocation(ANOTHER_PNFSID, "file"));

        Collection<Link> locations = _namespace.find(ROOT, A_PNFSID);

        PnfsGetParentMessage sent =
              getSingleSendAndWaitMessage(PnfsGetParentMessage.class);
        assertThat(sent.getReplyRequired(), is(true));
        assertThat(sent.getSubject(), is(ROOT));
        assertThat(sent.getPnfsId(), is(A_PNFSID));

        assertThat(locations.size(), is(1));
        Link location = locations.iterator().next();
        assertThat(location.getParent(), is(ANOTHER_PNFSID));
        assertThat(location.getName(), is("file"));
    }


    @Test(expected = FileNotFoundCacheException.class)
    public void shouldFailForFindMissingEntry() throws Exception {
        givenFailureResponse(FILE_NOT_FOUND);

        _namespace.find(ROOT, A_PNFSID);
    }


    @Test(timeout = 60_000)
    public void shouldSucceedForSuccessfulListWithSingleReplyMessage() throws Exception {
        givenListResponses(
              Lists.newArrayList(
                    entry().name("file-1").id(A_PNFSID).size(1000).build(),
                    entry().name("file-2").id(ANOTHER_PNFSID).size(2000).build()));

        ListCapture capture = new ListCapture();
        _namespace.list(ROOT, "/path/to/dir", null, ALL_ENTRIES,
              EnumSet.of(SIZE), capture);

        Map<String, FileAttributes> results = capture.getNames();
        assertThat(results.keySet(), hasSize(2));
        assertThat(results.keySet(), hasItem("file-1"));
        assertThat(results.keySet(), hasItem("file-2"));
        FileAttributes file1Attr = results.get("file-1");
        assertThat(file1Attr.getSize(), is(1000L));
        assertThat(file1Attr.getPnfsId(), is(A_PNFSID));
        FileAttributes file2Attr = results.get("file-2");
        assertThat(file2Attr.getSize(), is(2000L));
        assertThat(file2Attr.getPnfsId(), is(ANOTHER_PNFSID));
    }

    @Test(timeout = 60_000)
    public void shouldSucceedForSuccessfulListWithTwoReplyMessage() throws Exception {
        givenListResponses(
              Lists.newArrayList(
                    entry().name("file-1").id(A_PNFSID).size(1000).build(),
                    entry().name("file-2").id(ANOTHER_PNFSID).size(2000).build()),
              Lists.newArrayList(
                    entry().name("file-3").id(PNFSID_3).size(3000).build(),
                    entry().name("file-4").id(PNFSID_4).size(4000).build()));

        ListCapture capture = new ListCapture();
        _namespace.list(ROOT, "/path/to/dir", null, ALL_ENTRIES,
              EnumSet.of(SIZE), capture);

        Map<String, FileAttributes> results = capture.getNames();
        assertThat(results.keySet(), hasSize(4));
        assertThat(results.keySet(), hasItem("file-1"));
        assertThat(results.keySet(), hasItem("file-2"));
        assertThat(results.keySet(), hasItem("file-3"));
        assertThat(results.keySet(), hasItem("file-4"));
        FileAttributes file1Attr = results.get("file-1");
        assertThat(file1Attr.getSize(), is(1000L));
        assertThat(file1Attr.getPnfsId(), is(A_PNFSID));
        FileAttributes file2Attr = results.get("file-2");
        assertThat(file2Attr.getSize(), is(2000L));
        assertThat(file2Attr.getPnfsId(), is(ANOTHER_PNFSID));
        FileAttributes file3Attr = results.get("file-3");
        assertThat(file3Attr.getSize(), is(3000L));
        assertThat(file3Attr.getPnfsId(), is(PNFSID_3));
        FileAttributes file4Attr = results.get("file-4");
        assertThat(file4Attr.getSize(), is(4000L));
        assertThat(file4Attr.getPnfsId(), is(PNFSID_4));
    }


    @Test
    public void shouldSucceedForPathToPnfsidWithKnownPathAndResolvingSymlinks()
          throws Exception {
        givenSuccessfulResponse((Modifier<PnfsGetFileAttributes>)
              (m) -> m.setFileAttributes(FileAttributes.ofPnfsId(A_PNFSID)));

        PnfsId id = _namespace.pathToPnfsid(ROOT, "/path/to/entry", true);

        PnfsGetFileAttributes sent =
              getSingleSendAndWaitMessage(PnfsGetFileAttributes.class);

        assertThat(sent.getReplyRequired(), is(true));
        assertThat(sent.getSubject(), is(ROOT));
        assertThat(sent.getPnfsPath(), is("/path/to/entry"));
        assertThat(sent.getPnfsId(), nullValue());
        assertThat(sent.isFollowSymlink(), is(true));

        assertThat(id, is(A_PNFSID));
    }


    @Test
    public void shouldSucceedForPathToPnfsidWithKnownPathAndNotResolvingSymlinks()
          throws Exception {
        givenSuccessfulResponse((Modifier<PnfsGetFileAttributes>)
              (m) -> m.setFileAttributes(FileAttributes.ofPnfsId(A_PNFSID)));

        PnfsId id = _namespace.pathToPnfsid(ROOT, "/path/to/entry", false);

        PnfsGetFileAttributes sent =
              getSingleSendAndWaitMessage(PnfsGetFileAttributes.class);
        assertThat(sent.getReplyRequired(), is(true));
        assertThat(sent.getSubject(), is(ROOT));
        assertThat(sent.getPnfsPath(), is("/path/to/entry"));
        assertThat(sent.getPnfsId(), nullValue());
        assertThat(sent.isFollowSymlink(), is(false));

        assertThat(id, is(A_PNFSID));
    }


    @Test(expected = FileNotFoundCacheException.class)
    public void shouldFailForPathToPnfsidWithUnknownPath()
          throws Exception {
        givenFailureResponse(FILE_NOT_FOUND);

        _namespace.pathToPnfsid(ROOT, "/path/to/entry", false);
    }


    @Test
    public void shouldSucceedForPnfsidToPathWithKnownPnfsId()
          throws Exception {
        givenSuccessfulResponse((Modifier<PnfsMapPathMessage>)
              (m) -> m.setGlobalPath("/path/to/entry"));

        String path = _namespace.pnfsidToPath(ROOT, A_PNFSID);

        PnfsMapPathMessage sent =
              getSingleSendAndWaitMessage(PnfsMapPathMessage.class);
        assertThat(sent.getReplyRequired(), is(true));
        assertThat(sent.getSubject(), is(ROOT));
        assertThat(sent.getGlobalPath(), nullValue());
        assertThat(sent.getPnfsId(), is(A_PNFSID));

        assertThat(path, is("/path/to/entry"));
    }


    @Test(expected = FileNotFoundCacheException.class)
    public void shouldFailForPnfsidToPathWithUnknownPnfsId()
          throws Exception {
        givenFailureResponse(FILE_NOT_FOUND);

        _namespace.pnfsidToPath(ROOT, A_PNFSID);
    }


    @Test
    public void shouldSucceedForRemoveChecksum() throws Exception {
        _namespace.removeChecksum(ROOT, A_PNFSID, ChecksumType.ADLER32);

        PnfsRemoveChecksumMessage sent =
              getSingleSentMessage(PnfsRemoveChecksumMessage.class);
        assertThat(sent.getSubject(), is(ROOT));
        assertThat(sent.getPnfsId(), is(A_PNFSID));
        assertThat(sent.getType(), is(ChecksumType.ADLER32));
    }


    @Test
    public void shouldSucceedForRemoveFileAttribute() throws Exception {
        _namespace.removeFileAttribute(ROOT, A_PNFSID, "flag-name");

        PnfsFlagMessage sent = getSingleSentMessage(PnfsFlagMessage.class);
        assertThat(sent.getSubject(), is(ROOT));
        assertThat(sent.getPnfsId(), is(A_PNFSID));
        assertThat(sent.getFlagName(), is("flag-name"));
        assertThat(sent.getOperation(), is(FlagOperation.REMOVE));
    }

    @Test
    public void shouldSucceedForSetFileAttributesForKnownFile() throws Exception {
        givenSuccessfulResponse();

        _namespace.setFileAttributes(ROOT, A_PNFSID,
              fileAttributes().withSize(1000).build(), EnumSet.noneOf(FileAttribute.class));

        PnfsSetFileAttributes sent =
              getSingleSendAndWaitMessage(PnfsSetFileAttributes.class);
        assertThat(sent.getSubject(), is(ROOT));
        assertThat(sent.getPnfsId(), is(A_PNFSID));
        FileAttributes attributes = sent.getFileAttributes();
        assertThat(attributes.isDefined(SIZE), is(true));
        assertThat(attributes.getSize(), is(1000L));
        assertThat(attributes.isDefined(TYPE), is(false));
    }


    @Test(expected = FileNotFoundCacheException.class)
    public void shouldFailForSetFileAttributesForUnknownFile() throws Exception {
        givenFailureResponse(FILE_NOT_FOUND);

        _namespace.setFileAttributes(ROOT, A_PNFSID,
              fileAttributes().withSize(1000).build(), EnumSet.noneOf(FileAttribute.class));
    }

    /*
     * SUPPORT METHODS AND CLASSES
     */

    /**
     * Build response profile to a PnfsListDirectoryMessage.  The method takes a vararg list of
     * collections of DirectoryEntry answers.  Each collection represents a Message sent by the
     * namespace.
     * <p>
     * This method simulates the arrival of these Messages by calling the ListDirectoryHandler's
     * messageArrived method.  The flag that indicates whether the message is the last in the train
     * of messages is set automatically.
     * <p>
     * Delivery of these messages is triggered when the implementation next calls sendMessage (a
     * sendAndWait will trigger no activity).
     * <p>
     * Note:
     * <p>
     * 1.  the unit-test support framework knows some aspects of the implementation; if the
     * implementation changes then these methods need to up updated accordingly.
     * <p>
     * 2.  this method will use a separate thread to deliver the replies to the messageArrived so
     * there is *no* happen-before relationship between sendMessage returning and the first call to
     * messageArrived.
     */
    private void givenListResponses(final Collection<DirectoryEntry>... answers) {
        try {

            doAnswer((Answer) (i) -> {
                CellMessage request = (CellMessage) i.getArguments()[0];

                List<PnfsListDirectoryMessage> replies =
                      buildMessages(request, answers);

                backgroundDeliverMessages(replies);

                return null;
            }).when(_endpoint).sendMessage(any(CellMessage.class));

        } catch (SerializationException e) {
            throw new RuntimeException(e);
        }
    }


    private List<PnfsListDirectoryMessage> buildMessages(final CellMessage request,
          final Collection<DirectoryEntry>... replies) {
        List<PnfsListDirectoryMessage> messages =
              Lists.newArrayListWithExpectedSize(replies.length);

        for (int i = 0; i < replies.length; i++) {
            Collection<DirectoryEntry> entries = replies[i];
            boolean isLast = i == (replies.length - 1);
            CellMessage reply = buildListReply(request, entries, isLast, replies.length);

            messages.add((PnfsListDirectoryMessage) reply.getMessageObject());
        }

        return messages;
    }


    private static CellMessage buildListReply(CellMessage request,
          final Collection<DirectoryEntry> entries, final boolean isLast, final int cnt) {
        return buildReply(request, (Modifier<PnfsListDirectoryMessage>) (r) -> {
            r.setEntries(entries);
            if (isLast) {
                r.setSucceeded(cnt);
            }
        }, SUCCESSFUL);
    }


    private void backgroundDeliverMessages(final Collection<PnfsListDirectoryMessage> messages) {
        new Thread() {
            @Override
            public void run() {
                messages.forEach((m) -> _listHandler.messageArrived(m));
            }
        }.start();
    }


    /**
     * Configure the mock CellEndpoint so that any call to sendAndWait will return the same object
     * but with setSuccessful method called.
     */
    private void givenSuccessfulResponse() {
        givenResponse(SUCCESSFUL);
    }

    /**
     * Configure the mock CellEndpoint so that any call to sendAndWait will return the same object
     * but with custom modification (represented by modifier) and with the setSuccessful method
     * called.
     */
    private void givenSuccessfulResponse(final Modifier modifier) {
        givenResponse(modifier, SUCCESSFUL);
    }


    /**
     * Configure the mock CellEndpoint so that any call to sendAndWait will return the same object
     * but with with setFailed called with the supplied error code.
     */
    private void givenFailureResponse(final int errorcode) {
        givenResponse((m) -> m.setFailed(errorcode, messageFor(errorcode)));
    }

    private void givenResponse(final Modifier... modifiers) {
        willAnswer((Answer) (i) -> {
            CellMessage request = (CellMessage) i.getArguments()[0];
            CellMessageAnswerable callback =
                  (CellMessageAnswerable) i.getArguments()[1];
            callback.answerArrived(request, buildReply(request, modifiers));
            return null;
        }).given(_endpoint).sendMessage(any(CellMessage.class), any(CellMessageAnswerable.class),
              any(Executor.class), anyLong());
    }


    private static CellMessage buildReply(CellMessage request,
          Modifier... modifiers) {
        CellMessage reply = request.encodeWith(SerializationHandler.Serializer.JOS).decode();

        PnfsMessage payload = (PnfsMessage) reply.getMessageObject();
        for (Modifier modifier : modifiers) {
            modifier.modify(payload);
        }

        return reply;
    }

    /**
     * Classes that implement this interface can modify a PnfsMessage (or subclass thereof) in any
     * arbitrary fashion.
     */
    private interface Modifier<T extends PnfsMessage> {

        void modify(T message);
    }


    private static String messageFor(int errorcode) {
        switch (errorcode) {
            case CacheException.FILE_NOT_FOUND:
                return "file not found";
            case CacheException.FILE_EXISTS:
                return "file already exists";
            case CacheException.NOT_FILE:
                return "not a file";
            default:
                throw new IllegalArgumentException("Unknown code " + errorcode);
        }
    }


    /**
     * Obtain the PnfsMessage that was sent via the CellEndpoint.sendAndWait method.
     */
    private <T extends PnfsMessage> T getSingleSendAndWaitMessage(Class<T> type) {
        ArgumentCaptor<CellMessage> argument =
              ArgumentCaptor.forClass(CellMessage.class);

        verify(_endpoint).sendMessage(argument.capture(), any(CellMessageAnswerable.class),
              any(Executor.class), anyLong());

        verify(_endpoint, never()).sendMessage(any(CellMessage.class));

        Object payload = argument.getValue().getMessageObject();

        return type.cast(payload);
    }

    /**
     * Obtain the PnfsMessage that was sent via the CellEndpoint.sendMessage method.
     */
    private <T extends PnfsMessage> T getSingleSentMessage(Class<T> type) {
        ArgumentCaptor<CellMessage> argument =
              ArgumentCaptor.forClass(CellMessage.class);
        verify(_endpoint).sendMessage(argument.capture());
        return type.cast(argument.getValue().getMessageObject());
    }


    /**
     * Class to capture all list results.
     */
    private static class ListCapture implements ListHandler {

        Map<String, FileAttributes> _items = new HashMap<>();

        @Override
        public void addEntry(String name, FileAttributes attrs) throws CacheException {
            _items.put(name, attrs);
        }

        public Map<String, FileAttributes> getNames() {
            return Collections.unmodifiableMap(_items);
        }
    }

    private DirectoryEntryBuilder entry() {
        return new DirectoryEntryBuilder();
    }

    /**
     * A fluent class to build a DirectoryEntry
     */
    private static class DirectoryEntryBuilder {

        private final FileAttributes _attributes = new FileAttributes();
        private String _name;

        public DirectoryEntryBuilder name(String name) {
            _name = name;
            return this;
        }

        public DirectoryEntryBuilder size(long size) {
            _attributes.setSize(size);
            return this;
        }

        public DirectoryEntryBuilder type(FileType type) {
            _attributes.setFileType(type);
            return this;
        }

        public DirectoryEntryBuilder id(PnfsId id) {
            _attributes.setPnfsId(id);
            return this;
        }

        public DirectoryEntry build() {
            checkState(_name != null, "need to specify a name");
            return new DirectoryEntry(_name, _attributes);
        }
    }
}
