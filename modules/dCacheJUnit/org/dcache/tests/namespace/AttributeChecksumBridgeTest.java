package org.dcache.tests.namespace;

import diskCacheV111.namespace.AbstractNameSpaceProvider;
import diskCacheV111.namespace.provider.AttributeChecksumBridge;
import org.dcache.util.Checksum;
import org.dcache.util.ChecksumType;
import diskCacheV111.util.PnfsId;
import java.util.HashMap;
import java.util.Map;
import javax.security.auth.Subject;
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

        _mgr.setChecksum(null, null, sum, ChecksumType.MD5_TYPE.getType());

        assertEquals("check sum mismatch", sum, _mgr.getChecksum(null, null, ChecksumType.MD5_TYPE.getType()));

        _mgr.removeChecksum(null,null,ChecksumType.MD5_TYPE.getType());
        assertNull("checksum not removed", _mgr.getChecksum(null, null, ChecksumType.MD5_TYPE.getType()) );
    }

    @Test
    public void testSetGetRemoveAdler32() throws Exception {
        String sum = "CAFE";

        _mgr.setChecksum(null, null, sum, ChecksumType.ADLER32.getType());

        assertEquals("check sum mismatch", sum, _mgr.getChecksum(null,null, ChecksumType.ADLER32.getType()));

        _mgr.removeChecksum(null,null,ChecksumType.ADLER32.getType());
        assertNull("checksum not removed", _mgr.getChecksum(null,null, ChecksumType.ADLER32.getType()) );
    }

    @Test
    public void testSetGetMixed() throws Exception {
        String md5Sum = "CAFE";
        String adlerSum = "BEEF";

        _mgr.setChecksum(null, null, adlerSum, ChecksumType.ADLER32.getType());
        _mgr.setChecksum(null, null, md5Sum, ChecksumType.MD5_TYPE.getType());

        assertEquals("Incorrect number of stored checksums", 2, _mgr.getChecksums(null, null).size() );
        assertEquals("check sum mismatch", adlerSum, _mgr.getChecksum(null, null, ChecksumType.ADLER32.getType()));
        assertEquals("check sum mismatch", md5Sum, _mgr.getChecksum(null, null, ChecksumType.MD5_TYPE.getType()));
    }

    class MyFakeNameSpaceProvider extends AbstractNameSpaceProvider
    {
        private Map<String, Object> _map = new HashMap<String, Object>();

        @Override
        public Object getFileAttribute(Subject subject, PnfsId pnfsId, String attribute) {
            Object result = _map.get(attribute);
            return result;
        }

        @Override
        public void removeFileAttribute(Subject subject, PnfsId pnfsId, String attribute) {
            _map.remove(attribute);
        }

        @Override
        public void setFileAttribute(Subject subject, PnfsId pnfsId, String attribute, Object data) {
            _map.put(attribute, data);
        }
    }
}
