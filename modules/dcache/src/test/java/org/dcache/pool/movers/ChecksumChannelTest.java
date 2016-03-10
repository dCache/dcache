package org.dcache.pool.movers;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import diskCacheV111.util.ChecksumFactory;

import org.dcache.pool.repository.FileRepositoryChannel;
import org.dcache.pool.repository.RepositoryChannel;
import org.dcache.util.Checksum;
import org.dcache.util.ChecksumType;

import static com.google.common.collect.Lists.newArrayList;
import static org.dcache.util.ByteUnit.KiB;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ChecksumChannelTest {


    private ChecksumChannel chksumChannel;

    private byte[] data = "\0Just\0A\0Short\0TestString\0To\0Verify\0\0Checksumming\0\0Works\12".getBytes(StandardCharsets.ISO_8859_1); // \12 is a octal 10, linefeed
    private Checksum expectedChecksum;
    private int blocksize = 2;
    private int blockcount = data.length/blocksize;
    private ByteBuffer[] buffers = new ByteBuffer[blockcount];

    private File testFile;

    @Before
    public void setUp() throws NoSuchAlgorithmException, IOException {
        testFile = File.createTempFile("ChecksumChannelTest", ".tmp");
        RepositoryChannel mockRepositoryChannel = new FileRepositoryChannel(testFile, "rw");
        ChecksumFactory checksumFactory = ChecksumFactory.getFactory(ChecksumType.MD5_TYPE);
        chksumChannel = new ChecksumChannel(mockRepositoryChannel, checksumFactory);
        chksumChannel._readBackBuffer = ByteBuffer.allocate(2);
        chksumChannel._zerosBuffer = ByteBuffer.allocate(1);
        expectedChecksum = new Checksum(ChecksumType.MD5_TYPE, checksumFactory.create().digest(data));
        for (int b = 0; b < blockcount; b++) {
            buffers[b] = ByteBuffer.wrap(Arrays.copyOfRange(data, b*blocksize, (b+1)*blocksize));
        }
    }

    @After
    public void tearDown() throws IOException {
        chksumChannel.close();
        if (!testFile.delete()) {
            fail("Could not delete temporary test file " + testFile.getAbsolutePath());
        }
    }

    @Test
    public void shouldSucceedIfWrittenAtOnce() throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap( data );

        chksumChannel.write(buffer, 0);

        assertThat(chksumChannel.getChecksum(), equalTo(expectedChecksum));
    }

    @Test
    public void shouldReturnSameChecksumOnSecondCall() throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap( data );

        chksumChannel.write(buffer, 0);

        assertThat(chksumChannel.getChecksum(), equalTo(expectedChecksum));
        assertThat(chksumChannel.getChecksum(), equalTo(expectedChecksum));
    }

    @Test
    public void shouldSucceedIfWrittenInOrder() throws IOException {
        for (int block = 0; block < blockcount; block++) {
            chksumChannel.write(buffers[block], block * blocksize);
        }

        assertThat(chksumChannel.getChecksum(), equalTo(expectedChecksum));
    }

    @Test
    public void shouldSucceedIfWrittenOutOfOrderWithPosition() throws IOException {
        int[] blockorder = getRandomPermutationOfBlockOrder();
        for (int i = 0; i < blockcount; i++) {
            chksumChannel.write(buffers[blockorder[i]], blockorder[i] * blocksize);
        }

        assertThat(chksumChannel.getChecksum(), equalTo(expectedChecksum));
    }

    @Test
    public void shouldSucceedIfWrittenOutOfOrderWithSingleBuffer() throws IOException {
        int[] blockorder = getRandomPermutationOfBlockOrder();
        for (int i = 0; i < blockcount; i++) {
            chksumChannel.position(blockorder[i] * blocksize);
            chksumChannel.write(buffers[blockorder[i]]);
        }

        assertThat(chksumChannel.getChecksum(), equalTo(expectedChecksum));
    }

    @Test
    public void shouldSucceedIfWrittenInOrderWithMultipleBuffers() throws IOException {
        chksumChannel.write(buffers);

        assertThat(chksumChannel.getChecksum(), equalTo(expectedChecksum));
    }

    @Test
    public void shouldSucceedIfWrittenInOrderWithMultipleBuffersAndOffset() throws IOException {
        ByteBuffer[] buffers = new ByteBuffer[blockcount+2];
        buffers[0] = this.buffers[blockcount-1];
        buffers[blockcount] = this.buffers[0];
        System.arraycopy(this.buffers, 0, buffers, 1, blockcount);

        chksumChannel.write(buffers, 1, blockcount);

        assertThat(chksumChannel.getChecksum(), equalTo(expectedChecksum));
    }

    @Test
    public void shouldReturnNullDigestOnDoubleWrites() throws IOException {
        chksumChannel.write(buffers[0], 0);
        buffers[0].rewind();
        chksumChannel.write(buffers[0], 0);

        assertThat(chksumChannel.getChecksum(), equalTo(null));
    }

    @Test
    public void shouldReturnNullDigestOnPartlyOverlappingWrites() throws IOException {
        chksumChannel.write(buffers[1], blocksize);
        chksumChannel.write(buffers[0], blocksize - 1);

        if (blocksize == 1) {
            fail("Pick a blocksize > 1 for testing correct handling of partly overlapping writes!");
        }

        assertThat(chksumChannel.getChecksum(), equalTo(null));
    }

    @Test
    public void shouldNotUpdateChecksumForIncompleteWritesWithZeroByteWritesToChannelWithSingleBuffer () throws IOException, NoSuchAlgorithmException {
        RepositoryChannel mockRepositoryChannel = mock(RepositoryChannel.class);
        when(mockRepositoryChannel.write(any(), anyInt())).thenReturn(0);
        ChecksumFactory factory = ChecksumFactory.getFactory(ChecksumType.MD5_TYPE);
        ChecksumChannel csc = new ChecksumChannel(mockRepositoryChannel, factory);

        csc.write(buffers[0]);
    }

    @Test
    public void shouldNotUpdateChecksumForIncompleteWritesWithZeroByteWritesToChannelWithSingleBufferAndPosition () throws IOException, NoSuchAlgorithmException {
        RepositoryChannel mockRepositoryChannel = mock(RepositoryChannel.class);
        when(mockRepositoryChannel.write(any(), anyInt())).thenReturn(0);
        ChecksumFactory factory = ChecksumFactory.getFactory(ChecksumType.MD5_TYPE);
        ChecksumChannel csc = new ChecksumChannel(mockRepositoryChannel, factory);

        csc.write(buffers[0], 0);
    }

    @Test
    public void shouldUpdateChecksumOnlyForWrittenBytesOnIncompleteWritesWithSingleBufferAndPosition () throws IOException, NoSuchAlgorithmException {
        RepositoryChannel mockRepositoryChannel = mock(RepositoryChannel.class);
        when(mockRepositoryChannel.write(any(), anyInt())).thenReturn(1);
        when(mockRepositoryChannel.read(any(), eq(3L))).thenReturn(1);
        ChecksumFactory factory = ChecksumFactory.getFactory(ChecksumType.MD5_TYPE);
        ChecksumChannel csc = new ChecksumChannel(mockRepositoryChannel, factory);

        csc.write(buffers[0], 0);
        csc.write(buffers[1], 1);
        csc.write(buffers[3], 3);
        csc.write(buffers[2], 2);

        assertNotNull(csc.getChecksum());
    }

    @Test
    public void shouldNotUpdateChecksumForIncompleteWritesWithZeroBytesWritesToChannelWithMultipleBuffers () throws IOException, NoSuchAlgorithmException {
        RepositoryChannel mockRepositoryChannel = mock(RepositoryChannel.class);
        when(mockRepositoryChannel.write(any(), anyInt())).thenReturn(0);
        ChecksumFactory factory = ChecksumFactory.getFactory(ChecksumType.MD5_TYPE);
        ChecksumChannel csc = new ChecksumChannel(mockRepositoryChannel, factory);

        csc.write(buffers);
    }

    @Test
    public void shouldNotUpdateChecksumForIncompleteWritesWithZeroByteWritesToChannelWithMultipleBuffersAndOffset () throws IOException, NoSuchAlgorithmException {
        RepositoryChannel mockRepositoryChannel = mock(RepositoryChannel.class);
        when(mockRepositoryChannel.write(any(), anyInt())).thenReturn(0);
        ChecksumFactory factory = ChecksumFactory.getFactory(ChecksumType.MD5_TYPE);
        ChecksumChannel csc = new ChecksumChannel(mockRepositoryChannel, factory);

        csc.write(buffers, 0, blockcount);
    }

    @Test
    public void shouldUpdateChecksumSynchronizedForMultiThreadedWrites() throws IOException, InterruptedException {

        class Writer implements Runnable {

            ChecksumChannel channel;
            ByteBuffer block;
            int position;

            public Writer(ChecksumChannel channel, ByteBuffer block, int position) {
                this.channel = channel;
                this.block = block;
                this.position = position;
            }

            @Override
            public void run() {
                try {
                    channel.write(block, position);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        int[] blockorder = getRandomPermutationOfBlockOrder();
        List<Thread> writers = newArrayList();
        for (int i = 0; i < blockcount; i++) {
            writers.add(new Thread(new Writer(chksumChannel, buffers[blockorder[i]], blockorder[i] * blocksize)));
        }

        writers.forEach(Thread::start);

        for (Thread writer: writers) {
            writer.join();
        }

        assertThat(chksumChannel.getChecksum(), equalTo(expectedChecksum));
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowIllegalStateExceptionOnWritesAfterGetChecksum() throws IOException {
        chksumChannel.getChecksum();
        chksumChannel.write(buffers[0], 0);
    }

    @Test
    public void shouldBeAbleToReadBackRangesOfSizeGreater4Gb() throws IOException {
        /*
        Because of the way the methods in this test are mocked,
        readBackCapacity has to be a divisor of writeBufferCapacity
         */
        int readBackCapacity = KiB.toBytes(256);
        int writeBufferCapacity = 4 * readBackCapacity;
        ByteBuffer writeBuffer = ByteBuffer.allocate(writeBufferCapacity);
        chksumChannel._readBackBuffer = ByteBuffer.allocate(readBackCapacity);
        chksumChannel._channel = mock(FileRepositoryChannel.class);
        when(chksumChannel._channel.write(any(), anyLong())).thenReturn(writeBufferCapacity);
        when(chksumChannel._channel.read(any(), longThat(lessThan(0L)))).thenThrow(new IllegalArgumentException("Negative Position"));
        when(chksumChannel._channel.read(any(), longThat(greaterThanOrEqualTo(0L)))).thenReturn(readBackCapacity);
        for (long i = writeBuffer.capacity(); i < 2L*Integer.MAX_VALUE; i += writeBufferCapacity) {
            chksumChannel.write(writeBuffer, i);
            writeBuffer.rewind();
        }
        chksumChannel.write(writeBuffer, 0);
        assertThat(chksumChannel.getChecksum(), notNullValue());
    }

    @Test
    public void shouldBeAbleToFillZeroRangesOfSizeGreater4Gb() throws IOException {
        chksumChannel._zerosBuffer = ByteBuffer.allocate(KiB.toBytes(256));
        chksumChannel._channel = mock(FileRepositoryChannel.class);
        when(chksumChannel._channel.write(any(), anyLong())).thenReturn(buffers[0].capacity());
        when(chksumChannel._channel.read(any(), anyLong())).thenReturn(2);
        when(chksumChannel._channel.size()).thenReturn(2L*Integer.MAX_VALUE + 2);
        chksumChannel.write(buffers[0], 2L*Integer.MAX_VALUE);
        assertThat(chksumChannel.getChecksum(), notNullValue());
    }

    @Test
    public void shouldFillUpRangeGapsWithZerosOnGetChecksum() throws IOException {
        Map<Long, ByteBuffer> nonZeroBlocksFromByteArray = getNonZeroBlocksFromByteArray(data);
        for (Long position : nonZeroBlocksFromByteArray.keySet()) {
            chksumChannel.write(nonZeroBlocksFromByteArray.get(position), position);
        }

        assertThat(chksumChannel.getChecksum(), equalTo(expectedChecksum));
    }

    private Map<Long, ByteBuffer> getNonZeroBlocksFromByteArray(byte[] bytes) {
        Map<Long, ByteBuffer> result = new TreeMap<>();
        for (int position = 0; position < bytes.length; position++) {
            if (bytes[position] > 0) {
                int blockEnd = position;
                while (blockEnd < bytes.length && bytes[blockEnd] != 0) { blockEnd++; }
                result.put((long) position, ByteBuffer.wrap(Arrays.copyOfRange(bytes, position, blockEnd)));
                position = blockEnd;
            }
        }
        return result;
    }

    private int[] getRandomPermutationOfBlockOrder() {
        Integer[] blockSequence = new Integer[blockcount];
        for (int i = 0; i < blockcount; i++) {
            blockSequence[i] = i;
        }
        List<Integer> blockNumberList = newArrayList(blockSequence);

        Collections.shuffle(blockNumberList);

        int[] result = new int[blockcount];
        for (int i = 0; i < blockcount; i++) {
            result[i] = blockNumberList.get(i);
        }

        return result;
    }

}
