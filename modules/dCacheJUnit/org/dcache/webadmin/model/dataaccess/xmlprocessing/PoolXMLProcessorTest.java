package org.dcache.webadmin.model.dataaccess.xmlprocessing;

import org.dcache.webadmin.model.dataaccess.impl.*;
import java.util.Set;
import org.dcache.webadmin.model.businessobjects.Pool;
import org.dcache.webadmin.model.exceptions.ParsingException;
import org.junit.Before;
import org.junit.Test;
import org.w3c.dom.Document;
import static org.junit.Assert.*;

/**
 * This is a UnitTest for the XMLProcessor Class. It tests the output of
 * the parsing on pre fabricated XMLs. Also exposes them to others for testusage
 * @author jan schaefer
 */
public class PoolXMLProcessorTest {

    public PoolXMLProcessor _processor = null;

    @Before
    public void setUp() {
        _processor = new PoolXMLProcessor();
    }

    @Test
    public void testCreateEmptyXMLDocument() throws ParsingException {
        Document document = _processor.createXMLDocument(
                XMLDataGathererHelper.emptyXmlcontent);
        assertNotNull(document);
    }

    @Test
    public void testCreatePoolsXMLDocument() throws ParsingException {
        Document document = _processor.createXMLDocument(
                XMLDataGathererHelper.poolsXmlcontent);
        assertNotNull(document);
    }

    @Test
    public void testParsePoolsDocument() throws ParsingException {
        Set<Pool> parsedPools = _processor.parsePoolsDocument(
                _processor.createXMLDocument(XMLDataGathererHelper.poolsXmlcontent));
        Pool parsedPool = (Pool) parsedPools.iterator().next();
        assertTrue(XMLDataGathererHelper.getTestPool().equals(parsedPool));
    }

    @Test
    public void testParsePoolsEmptyDocument() throws ParsingException {
        Set<Pool> pools = _processor.parsePoolsDocument(
                _processor.createXMLDocument(XMLDataGathererHelper.emptyXmlcontent));
        assertEquals("more than zero elements returned", 0, pools.size());
    }
}
