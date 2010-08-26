package diskCacheV111.util;

import java.net.URI;
import org.junit.Test;
import static org.junit.Assert.*;

public class HsmLocationExtractorFactoryTest {

    @Test
    public void testValidateNoHsmType() throws Exception {
        URI location = new URI("aRandomString");
        try {
            HsmLocationExtractorFactory.validate(location);
            fail("invalid location not detected");
        }catch(IllegalArgumentException e) {
            // OK
        }
    }

    @Test
    public void testValidateNoHsmInstance() throws Exception {
        URI location = new URI("osm:///aRandomString");
        try {
            HsmLocationExtractorFactory.validate(location);
            fail("invalid location not detected");
        } catch (IllegalArgumentException e) {
            // OK
        }
    }

    @Test
    public void testValidateNoHsmInstance2() throws Exception {

        URI location = new URI("osm://osm?group");
        try {
            HsmLocationExtractorFactory.validate(location);
            fail("invalid location not detected");
        } catch (IllegalArgumentException e) {
            // OK
        }
    }

    @Test
    public void testValidateNoUniqueId() throws Exception {
        URI location = new URI("osm://aRandomString");
        try {
            HsmLocationExtractorFactory.validate(location);
            fail("invalid location not detected");
        } catch (IllegalArgumentException e) {
            // OK
        }
    }

    @Test
    public void testValidateOk() throws Exception {
        HsmLocationExtractorFactory.validate(new URI("osm://osm/?group=1&store=2&bfid=3"));
    }
}