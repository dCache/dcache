package org.dcache.tests.namespace;

import diskCacheV111.namespace.NameSpaceProvider;
import diskCacheV111.namespace.provider.AttributeChecksumBridge;
import diskCacheV111.util.Checksum;
import diskCacheV111.util.FileMetaData;
import diskCacheV111.util.PnfsId;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class AttributeChecksumBridgeTest {

    private AttributeChecksumBridge _mgr;

    @Before
    public void setUp() {
        _mgr = new AttributeChecksumBridge(new MyFakeNameSpaceProvider());
    }

    @Test
    public void testSetGetRemoveMD5() throws Exception {
        String sum = "CAFE";

        _mgr.setChecksum(null, sum, Checksum.MD5);

        assertEquals("check sum mismatch", sum, _mgr.getChecksum(null, Checksum.MD5));

        _mgr.removeChecksum(null,Checksum.MD5);
        assertNull("checksum not removed", _mgr.getChecksum(null, Checksum.MD5) );
    }

    @Test
    public void testSetGetRemoveAdler32() throws Exception {
        String sum = "CAFE";

        _mgr.setChecksum(null, sum, Checksum.ADLER32);

        assertEquals("check sum mismatch", sum, _mgr.getChecksum(null, Checksum.ADLER32));

        _mgr.removeChecksum(null,Checksum.ADLER32);
        assertNull("checksum not removed", _mgr.getChecksum(null, Checksum.ADLER32) );
    }

    @Test
    public void testSetGetMixed() throws Exception {
        String md5Sum = "CAFE";
        String adlerSum = "BEEF";

        _mgr.setChecksum(null, adlerSum, Checksum.ADLER32);
        _mgr.setChecksum(null, md5Sum, Checksum.MD5);

        assertEquals("Incorrect number of stored checksums", 2, _mgr.getChecksums(null).size() );
        assertEquals("check sum mismatch", adlerSum, _mgr.getChecksum(null, Checksum.ADLER32));
        assertEquals("check sum mismatch", md5Sum, _mgr.getChecksum(null, Checksum.MD5));
    }

    class MyFakeNameSpaceProvider implements NameSpaceProvider {

        private Map<String, Object> _map = new HashMap<String, Object>();

        public void setFileMetaData(PnfsId pnfsId, FileMetaData metaData) {
        }

        public FileMetaData getFileMetaData(PnfsId pnfsId) throws Exception {
            return null;
        }

        public PnfsId createEntry(String name, FileMetaData metaData, boolean checksumType) throws Exception {
            return null;
        }

        public void deleteEntry(PnfsId pnfsId) throws Exception {
        }

        public void deleteEntry(String path) throws Exception {
        }

        public void renameEntry(PnfsId pnfsId, String newName) throws Exception {
        }

        public String pnfsidToPath(PnfsId pnfsId) throws Exception {
            return null;
        }

        public PnfsId pathToPnfsid(String path, boolean followLinks) throws Exception {
            return null;
        }

        public String[] getFileAttributeList(PnfsId pnfsId) {
            return null;
        }

        public Object getFileAttribute(PnfsId pnfsId, String attribute) {
            Object result = _map.get(attribute);
            return result;
        }

        public void removeFileAttribute(PnfsId pnfsId, String attribute) {
            _map.remove(attribute);
        }

        public void setFileAttribute(PnfsId pnfsId, String attribute, Object data) {
            _map.put(attribute, data);
        }

        public void setLevelData(PnfsId pnfsId, Map<Integer, String> levelData) throws Exception {
        }

        public void addChecksum(PnfsId pnfsId, int type, String value) throws Exception {
        }

        public String getChecksum(PnfsId pnfsId, int type) throws Exception {
            return null;
        }

        public void removeChecksum(PnfsId pnfsId, int type) throws Exception {
        }

        public int[] listChecksumTypes(PnfsId pnfsId) throws Exception {
            return null;
        }

        public Set<org.dcache.util.Checksum> getChecksums(PnfsId pnfsId) throws Exception {
            return null;
        }

        public PnfsId getParentOf(PnfsId pnfsId) {
            return null;
        }
    }
}
