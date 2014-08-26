package org.dcache.services.info.serialisation;

import com.thaiopensource.util.PropertyMapBuilder;
import com.thaiopensource.validate.IncorrectSchemaException;
import com.thaiopensource.validate.Schema;
import com.thaiopensource.validate.SchemaReader;
import com.thaiopensource.validate.ValidateProperty;
import com.thaiopensource.validate.Validator;
import com.thaiopensource.validate.rng.CompactSchemaReader;
import org.junit.Before;
import org.junit.Test;
import org.xml.sax.ErrorHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLReaderFactory;

import java.io.IOException;
import java.io.StringReader;

import org.dcache.services.info.base.StatePath;
import org.dcache.services.info.base.TestStateExhibitor;

import static org.junit.Assert.fail;

public class XmlSerialiserTests {

    private static final String RNC_DEFAULT_NAMESPACE = "default namespace='http://www.dcache.org/2008/01/Info'\n";

    // Unfortunately, because of our XML structure, we must enumerate the possible options
    private static final String CELLS_ANY_ORDER =
        "cell-1 = element cell { attribute id {'cell-1'}} \n"
      + "cell-2 = element cell { attribute id {'cell-2'}} \n"
      + "cell-3 = element cell { attribute id {'cell-3'}} \n"
      + "cells-123 = element cells { cell-1, cell-2, cell-3} \n"
      + "cells-132 = element cells { cell-1, cell-3, cell-2} \n"
      + "cells-213 = element cells { cell-2, cell-1, cell-3} \n"
      + "cells-231 = element cells { cell-2, cell-3, cell-1} \n"
      + "cells-312 = element cells { cell-3, cell-1, cell-2} \n"
      + "cells-321 = element cells { cell-3, cell-2, cell-1} \n"
      + "cells-any-order = (cells-123 | cells-132 | cells-213 | cells-231 | cells-312 | cells-321) \n";

    private static final String RNC_ONLY_DCACHE = RNC_DEFAULT_NAMESPACE
    + "element dCache { empty }";

    private static final String RNC_DCACHE_DOMAIN = RNC_DEFAULT_NAMESPACE
        + CELLS_ANY_ORDER
        + "dCacheDomain = element domain { attribute name { 'dCacheDomain'}, \n"
        + "                                ( cells-any-order \n"
        + "                                 &element routine-info { empty })} \n"
        + "domains = element domains { dCacheDomain } \n"
        + "start = element dCache { domains }\n";


    private static final String RNC_ALL = RNC_DEFAULT_NAMESPACE
    + CELLS_ANY_ORDER
    + "infoDomain = element domain { attribute name {'infoDomain'},  \n"
    + "                              cells-any-order} \n"
    + "dCacheDomain = element domain { attribute name {'dCacheDomain'}, \n"
    + "                               ( cells-any-order\n"
    + "                                &element routine-info { empty })}\n"
    + "awkwardDomain = element domain {attribute name {'''\"bothersome\"&annoying<Cells>'sDomain'''}, \n"
    + "                                ( element cells {\n"
    + "                                    element cell { attribute id {'''\"Foo & Bar\" <foobar@example.org>'s cell'''}} \n"
    + "                                  })}\n"
    + "domains = element domains { (infoDomain, dCacheDomain, awkwardDomain) | \n"
    + "                            (infoDomain, awkwardDomain, dCacheDomain) | \n"
    + "                            (awkwardDomain, infoDomain, dCacheDomain) | \n"
    + "                            (awkwardDomain, dCacheDomain, infoDomain) | \n"
    + "                            (dCacheDomain, infoDomain, awkwardDomain) | \n"
    + "                            (dCacheDomain, awkwardDomain, infoDomain)} \n"
    + "start = element dCache { domains }\n";


    TestStateExhibitor _exhibitor;
    StateSerialiser _serialiser;

    @Before
    public void setUp() {
        _exhibitor = new TestStateExhibitor();
        XmlSerialiser serialiser = new XmlSerialiser();
        serialiser.setStateExhibitor(_exhibitor);
        _serialiser = serialiser;

        _exhibitor.addListItem( StatePath.parsePath( "domains.dCacheDomain"),
                "domain", "name");
        _exhibitor.addListItem( StatePath
                .parsePath( "domains.dCacheDomain.cells.cell-1"), "cell", "id");
        _exhibitor.addListItem( StatePath
                .parsePath( "domains.dCacheDomain.cells.cell-2"), "cell", "id");
        _exhibitor.addListItem( StatePath
                .parsePath( "domains.dCacheDomain.cells.cell-3"), "cell", "id");
        _exhibitor.addBranch( StatePath
                .parsePath( "domains.dCacheDomain.routine-info"));

        _exhibitor.addListItem( StatePath.parsePath( "domains.infoDomain"),
                "domain", "name");
        _exhibitor.addListItem( StatePath
                .parsePath( "domains.infoDomain.cells.cell-1"), "cell", "id");
        _exhibitor.addListItem( StatePath
                .parsePath( "domains.infoDomain.cells.cell-2"), "cell", "id");
        _exhibitor.addListItem( StatePath
                .parsePath( "domains.infoDomain.cells.cell-3"), "cell", "id");

        // The "bothersome" domain contains characters that must be marked-up
        StatePath domainPath = StatePath.parsePath( "domains.\"bothersome\"&annoying<Cells>'sDomain");
        _exhibitor.addListItem(  domainPath, "domain", "name");
        StatePath cellsPath = domainPath.newChild( "cells");
        _exhibitor.addListItem( cellsPath.newChild( "\"Foo & Bar\" <foobar@example.org>'s cell"), "cell", "id");
    }

    @Test
    public void testEmptyState() throws IOException, SAXException, IncorrectSchemaException {
        _exhibitor = new TestStateExhibitor();
        XmlSerialiser serialiser = new XmlSerialiser();
        serialiser.setStateExhibitor(_exhibitor);
        _serialiser = serialiser;
        String result = _serialiser.serialise();
        assertXmlValidates( RNC_ONLY_DCACHE, result);
    }

    @Test
    public void testFullVisit() {
        String result = _serialiser.serialise();
        assertXmlValidates( RNC_ALL, result);
    }

    @Test
    public void testDomainDCacheDomainVisit() {
        _serialiser.serialise();
        String result =
                _serialiser.serialise( StatePath
                        .parsePath( "domains.dCacheDomain"));
        assertXmlValidates( RNC_DCACHE_DOMAIN, result);
    }

    private void assertXmlValidates( String rncGrammar, String xmlData) {
        Validator validator = createValidator( rncGrammar);
        XMLReader reader = createValidatingXmlReader( validator);
        parseXmlWithReader( reader, xmlData);
    }

    private Validator createValidator( String rndData) {
        SchemaReader reader = CompactSchemaReader.getInstance();
        InputSource source = createInputSource( rndData);

        PropertyMapBuilder schemaPmb = new PropertyMapBuilder();
        Schema schema = null;
        try {
            schema = reader.createSchema( source, schemaPmb.toPropertyMap());
        } catch (IOException | IncorrectSchemaException | SAXException e) {
            fail(e.toString());
        }

        ErrorHandler eh = new AssertingErrorHandler();
        PropertyMapBuilder validatorPmb = new PropertyMapBuilder();
        validatorPmb.put( ValidateProperty.ERROR_HANDLER, eh);

        return schema.createValidator( validatorPmb.toPropertyMap());
    }

    private XMLReader createValidatingXmlReader( Validator validator) {
        XMLReader myReader = null;
        try {
            myReader = XMLReaderFactory.createXMLReader();
        } catch (SAXException e) {
            fail( e.toString());
        }
        myReader.setContentHandler( validator.getContentHandler());
        myReader.setDTDHandler( validator.getDTDHandler());
        return myReader;
    }

    private InputSource createInputSource( String data) {
        return new InputSource( new StringReader( data));
    }

    private void parseXmlWithReader( XMLReader reader, String xmlData) {
        InputSource input = createInputSource( xmlData);

        try {
            reader.parse( input);
        } catch (IOException | SAXException e) {
            fail( e.toString());
        }
    }

    private static class AssertingErrorHandler implements ErrorHandler {
        @Override
        public void error( SAXParseException exception) throws SAXException {
            fail(buildMessage(exception));
        }

        @Override
        public void fatalError( SAXParseException exception)
                throws SAXException {
            fail(buildMessage(exception));
        }

        @Override
        public void warning( SAXParseException exception) throws SAXException {
            fail(buildMessage(exception));
        }

        private String buildMessage(SAXParseException exception) {
            return "[RELAX-NG] " + exception.getLineNumber() +  "," + exception.getColumnNumber() + ": " + exception.getMessage();
        }
    }
}
