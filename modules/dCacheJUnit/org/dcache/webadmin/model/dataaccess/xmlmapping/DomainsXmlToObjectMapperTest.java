package org.dcache.webadmin.model.dataaccess.xmlmapping;

import java.util.HashSet;
import java.util.Set;
import org.dcache.webadmin.model.businessobjects.CellStatus;
import org.dcache.webadmin.model.dataaccess.impl.XMLDataGathererHelper;
import org.dcache.webadmin.model.exceptions.ParsingException;
import org.junit.Before;
import org.junit.Test;
import org.w3c.dom.Document;
import static org.junit.Assert.*;

/**
 *
 * @author jans
 */
public class DomainsXmlToObjectMapperTest {

    public DomainsXmlToObjectMapper _processor;

    @Before
    public void setUp() {
        _processor = new DomainsXmlToObjectMapper();
    }

    @Test
    public void testParsePoolsList() throws ParsingException {
        Set<String> pools = _processor.parsePoolsList(
                _processor.createXMLDocument(
                XMLDataGathererHelper.poolsXmlcontent));
        assertEquals(getExpectedPoolNames(), pools);
    }

    @Test
    public void testParseDoorsList() throws ParsingException {
        Set<String> doors = _processor.parseDoorList(
                _processor.createXMLDocument(
                XMLDataGathererHelper.doorsXmlcontent));
        assertEquals(XMLDataGathererHelper.getExpectedDoorNames(), doors);
    }

    @Test
    public void testParseDomainsDocument() throws ParsingException {
        Set<CellStatus> parsedCellStatuses = _processor.parseDomainsDocument(
                XMLDataGathererHelper.getExpectedDoorNames(),
                getExpectedPoolNames(),
                _processor.createXMLDocument(XMLDataGathererHelper.domainsXmlcontent));
        assertFalse(parsedCellStatuses.isEmpty());
    }

    @Test
    public void testParseDomainsEmptyDocument() throws ParsingException {
        Set<CellStatus> parsedCellStatuses = _processor.parseDomainsDocument(
                new HashSet<String>(),
                getExpectedPoolNames(),
                _processor.createXMLDocument(XMLDataGathererHelper.emptyXmlcontent));
        assertTrue(parsedCellStatuses.isEmpty());
    }

    @Test
    public void testParseDomainsMapDocument() throws ParsingException {
        Set<CellStatus> parsedCellStatuses = _processor.parseDomainsDocument(
                new HashSet<String>(),
                getExpectedPoolNames(),
                _processor.createXMLDocument(XMLDataGathererHelper.emptyXmlcontent));
        assertTrue(parsedCellStatuses.isEmpty());
    }

    private Set<String> getExpectedPoolNames() {
        Set<String> expectedPools = new HashSet<String>();
        expectedPools.add(XMLDataGathererHelper.POOL1_NAME);
        return expectedPools;
    }
}
