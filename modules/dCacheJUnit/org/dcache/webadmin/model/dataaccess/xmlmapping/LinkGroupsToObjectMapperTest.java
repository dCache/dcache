package org.dcache.webadmin.model.dataaccess.xmlmapping;

import java.util.HashSet;
import java.util.Set;
import org.dcache.webadmin.model.businessobjects.LinkGroup;
import org.dcache.webadmin.model.businessobjects.SpaceReservation;
import org.dcache.webadmin.model.dataaccess.impl.XMLDataGathererHelper;
import org.dcache.webadmin.model.exceptions.ParsingException;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Tests the correct mapping from xml to business-objects for linkgroups and
 * spacereservations. In other words:
 * Testclass for the LinkGroupXmlToObjectMapper.
 * @author jans
 */
public class LinkGroupsToObjectMapperTest {

    private LinkGroupXmlToObjectMapper _objectMapper;

    @Before
    public void setUp() {
        _objectMapper = new LinkGroupXmlToObjectMapper();
    }

    @Test
    public void testParseLinkGroupsDocument() throws ParsingException {
        Set<LinkGroup> linkGroups = new HashSet<LinkGroup>();
        linkGroups = _objectMapper.parseLinkGroupsDocument(
                _objectMapper.createXMLDocument(
                XMLDataGathererHelper.LINKGROUPS_XML));
        LinkGroup actual = (LinkGroup) linkGroups.toArray()[0];
        assertEquals(XMLDataGathererHelper.LINKGROUP1_ID, actual.getId());
        assertEquals(XMLDataGathererHelper.LINKGROUP1_NAME, actual.getName());
        assertEquals(Long.parseLong(XMLDataGathererHelper.LINKGROUP1_TOTAL_SPACE),
                actual.getTotal());
    }

    @Test
    public void testParseSpaceReservationsDocument() throws ParsingException {

        Set<SpaceReservation> spaceReservations = new HashSet<SpaceReservation>();
        spaceReservations = _objectMapper.parseSpaceReservationsDocument(
                _objectMapper.createXMLDocument(
                XMLDataGathererHelper.RESERVATIONS_XML));
        SpaceReservation actual = (SpaceReservation) spaceReservations.toArray()[0];
        assertEquals(Long.parseLong(XMLDataGathererHelper.RESERVATION1_TOTAL), actual.getTotalSpace());
        assertEquals(XMLDataGathererHelper.RESERVATION1_FQAN, actual.getVogroup());
    }
}
