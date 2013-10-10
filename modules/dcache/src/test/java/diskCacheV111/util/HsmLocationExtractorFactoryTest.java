package diskCacheV111.util;

import org.junit.Test;

import java.net.URI;

import static org.junit.Assert.fail;

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
    public void testValidateNoPath() throws Exception {

        URI location = new URI("osm://osm?group");
        HsmLocationExtractorFactory.validate(location);
    }

    @Test
    public void testValidateOk() throws Exception {
        HsmLocationExtractorFactory.validate(new URI("osm://osm/?group=1&store=2&bfid=3"));
    }

    @Test
    public void testExample1ValidateOk() throws Exception {
        HsmLocationExtractorFactory.validate(new URI("osm://desy-main/?store=sql&group=chimera&bfid=3434.0.994.1188400818542"));
    }

    @Test
    public void testExample2ValidateOk() throws Exception {
        HsmLocationExtractorFactory.validate(new URI("osm://desy-dup/?store=sql&group=chimera&bfid=3434.0.994.1188400818542"));
    }
}
