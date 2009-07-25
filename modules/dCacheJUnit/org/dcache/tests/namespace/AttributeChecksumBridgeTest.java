package org.dcache.tests.namespace;

import diskCacheV111.namespace.AbstractNameSpaceProvider;
import diskCacheV111.namespace.provider.AttributeChecksumBridge;
import diskCacheV111.util.Checksum;
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

        _mgr.setChecksum(null, null, sum, Checksum.MD5);

        assertEquals("check sum mismatch", sum, _mgr.getChecksum(null, null, Checksum.MD5));

        _mgr.removeChecksum(null,null,Checksum.MD5);
        assertNull("checksum not removed", _mgr.getChecksum(null, null, Checksum.MD5) );
    }

    @Test
    public void testSetGetRemoveAdler32() throws Exception {
        String sum = "CAFE";

        _mgr.setChecksum(null, null, sum, Checksum.ADLER32);

        assertEquals("check sum mismatch", sum, _mgr.getChecksum(null,null, Checksum.ADLER32));

        _mgr.removeChecksum(null,null,Checksum.ADLER32);
        assertNull("checksum not removed", _mgr.getChecksum(null,null, Checksum.ADLER32) );
    }

    @Test
    public void testSetGetMixed() throws Exception {
        String md5Sum = "CAFE";
        String adlerSum = "BEEF";

        _mgr.setChecksum(null, null, adlerSum, Checksum.ADLER32);
        _mgr.setChecksum(null, null, md5Sum, Checksum.MD5);

        assertEquals("Incorrect number of stored checksums", 2, _mgr.getChecksums(null, null).size() );
        assertEquals("check sum mismatch", adlerSum, _mgr.getChecksum(null, null, Checksum.ADLER32));
        assertEquals("check sum mismatch", md5Sum, _mgr.getChecksum(null, null, Checksum.MD5));
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
