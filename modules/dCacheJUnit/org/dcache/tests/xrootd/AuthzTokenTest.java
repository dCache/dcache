package org.dcache.tests.xrootd;

import java.security.GeneralSecurityException;
import java.util.Iterator;

import junit.framework.TestCase;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.dcache.xrootd.security.plugins.tokenauthz.CorruptedEnvelopeException;
import org.dcache.xrootd.security.plugins.tokenauthz.Envelope;
import org.junit.Before;
import org.junit.Test;

public class AuthzTokenTest extends TestCase {

    static Logger logger = Logger.getLogger(AuthzTokenTest.class);

    private String token448Bit;
    private String token128Bit;

    @Before
    protected void setUp() throws Exception {
        super.setUp();

        BasicConfigurator.configure();


        token128Bit =
            "-----BEGIN SEALED CIPHER-----\n"+
            "4rnOjKXH9ATE+4A7-zzi8IuxrAwhg7uts5a-LJ8QDCiNke1Pkw4IgUXIsUu1e9acgdLcnuthwFRk\n"+
            "BC1TGf0ayQXWoS484Sc4HRZcnfmaqSsORUTk+xKXTh-X0QNdmBELSB1AKCB9Ai8de8w5YMZvN6q4\n"+
            "GoQCukGy+aPLVphHzgU=\n"+
            "-----END SEALED CIPHER-----\n"+
            "-----BEGIN SEALED ENVELOPE-----\n"+
            "AAAAgLTc3ZH2gYn2JyRVJCZBzg9ak6GtGxAPmT29OSaNwkIxJ-9+6V6KTedItM8mooS4vzyoGAM1\n"+
            "qXHxAxL5jPbyKa0jeh+lZ3yy-6+k-ZYWsfyFOE3Kgsu+YH2bATacnm1FNcNO8elhnzXJVQzNu83I\n"+
            "kFMS+6ZNtdQe4D0pF3x+CBpBG+hzGLqbyn7O8wn+BMpMHAMMj-6Bh1J+t0s-GP6U4n3IykrY85Tt\n"+
            "mXOdclWdVeLvgLSuntkes7-PGy6MESf6Tp69JOyg6asqwCzNR7AW29ptZytxWo3UfuvnjAZsbquh\n"+
            "Cdhk5geMo4L8SqwS8zZdJY4OFZ9+eiuymPGbCqa4gk5BmefKEUVV6ZUfo4y6h8gn9BIfnW9Fvbup\n"+
            "7b5rCR1gXjBtvcFIEFjPh+LhRt1-1iwUr09d6FOOTr3X2Z9GdVM1hr4MEctBlzRewLO1S5hv6J-w\n"+
            "CMi8655+qA+FGDQDjWcXvoxxPi7Z3HAz6X26WfiLnruPXKhJwF-XbRq+H-Tf+ixlwbCiJvBnELiB\n"+
            "0qOfhWdSKYUHTw2GNpqggB-2X+7KiNz5EgpDwjGt7CdI-79pUfF6YLVxTHn0NQ1Z7f0cBsv4aMHW\n"+
            "UbWg-Sv2uXHG0A-cCwgcPzAQ3Yz9CjklJqeJSJURZNAXToLDuzcg1bv+b95bPfALjUX4a2+M0FRg\n"+
            "raYxw5kdkDNIZqVdncPVhJeTDYL-iv9WZsX0aES5Q1iedxgeWpq3THz8huyIoE+Eb6Pnsj4CsWIy\n"+
            "IQ56mMuplNi26S2uOkBnOll0+41rAjTVPUxhpk93xZttJewNOo3RbESAMgjSkfRkHjWqFOETNwKJ\n"+
            "AMd1RPc8Qz8ewlUwKcB1G2+Wx1G6xWKzlY+cR0Pbri6aT+coLWtpXVCBXenoVmYartAaQIyKbBkO\n"+
            "-----END SEALED ENVELOPE-----";

        token448Bit =
            "-----BEGIN SEALED CIPHER-----\n"+
            "2exGzZu4mNlJ6WcA5kPmmV1FxOxt0Jy-zn3BivQcaLlZfQ8i47UewBI4cW4ljlkOdYnN--R+vZM7\n"+
            "pbBVocgDRiuDJKT87l7AbSQfRocki3+xlG6QvvDAMMY8HvUjmeFjvm5l+ohSrsk+CpSjBqGLCjpz\n"+
            "tUehUaKZTRI12-SKtt0=\n"+
            "-----END SEALED CIPHER-----\n"+
            "-----BEGIN SEALED ENVELOPE-----\n"+
            "AAAAgA5GT9EHzUa7VrtYqPUiI5lk7Shv6vOEvJ+DnTLGvBrS6gRo+Pcr9r5TeN2vcJtnjFlA-OO8\n"+
            "woYEVK61j0yaRur72SJdiQNigmEr07HpZ5VuPGhGXwEFUHLXN2xga+LN6tGjUSeeFaIct74Z5e32\n"+
            "S2Lw7XP3jBKyks826ag3AaMm7JDiE8LAkkujnjs6M8ov3n9G8IPcFQHv+iFNCMG6UmaoRIVyUFKa\n"+
            "UoO-QjTkkWfMmKxOlR8LZKkW2Cf76zfL3Tg2TxeoA7tktY9dpMkQgLu-xfYzpdRBgei9LQJYQPo2\n"+
            "CTpzrjN8O2r2E1qjozXO5IpOyyGYB1dab3Okp-Ot0ThAOEskbLN3uDDr4K7qH3YuxodoTThlTnlG\n"+
            "ZN3eRYSEcrdE9QPn2Y7scXFsajTN+3PV4adjdFMYBcthb1o7oaWERQzYOhMMkoqru93kA8wLgpBu\n"+
            "VGryVxlLS8LJX5jeMW7vJjYXM2XIxlQxhXR6TVIn732zWNLQtPAW+JcW3hB2tY-IyQMeLlG9DBND\n"+
            "bxtHprQmyi0PAtk32LNikExOpkt2esB6b5PY4+PoCXOvze2craEm3qQJHaUsATsFtge2Y2b2vImn\n"+
            "ZxOgk+YZP7V+MCql8FZo5YuP82cu0p0xgEE9jb1aCfj6aT-xvN8kgSMK68TJHm8HRpdlpb2T-SHU\n"+
            "b66RCncyG0ZTT1zlE+gVmGZY5mBP-sXEbcARpZPzS9yi778UuhM1uETaYD0VpIDhEiw9a3ZadYFv\n"+
            "hFo6Ub2TROepj5RawC690YgznuWIeBW7WT8FFhlTVq0t-R-lFWMSYSiwRYyAaEyKmLyeshhUgn+T\n"+
            "TDcCizmAOp237+Gbv62CS1uw2SZ0xbD2byW2t2gSmA4zWoIW-NgIREmXr7Qhhc3ZhqOEEoNAmBTp\n"+
            "-----END SEALED ENVELOPE-----";
    }

    //	@Test
    //	public void testDecrypt() {
    //
    //		try {
    //
    //			EncryptedAuthzToken encToken;
    //
    //
    //			encToken = new EncryptedAuthzToken(token128Bit);
    //			assertTrue(encToken.decrypt() != null);
    //			encToken.getEnvelope();
    //
    //			encToken = new EncryptedAuthzToken(token448Bit);
    //			assertTrue(encToken.decrypt() != null);
    //			encToken.getEnvelope();
    //
    //
    //		} catch (GeneralSecurityException e) {
    //			logger.error(e.getMessage());
    //			e.printStackTrace();
    //			assertTrue(false);
    //		} catch (CorruptedEnvelopeException e) {
    //			logger.error(e.getMessage());
    //			e.printStackTrace();
    //			assertTrue(false);
    //		}
    //
    //	}

    @Test
    public void testEnvelope() {

        try {
            String envString =
                "-----BEGIN ENVELOPE-----\n"+
                "CREATOR:     Martin.Radicke\n"+
                "UNIXTIME:    1156155563\n"+
                "DATE:        Mon Aug 21 12:19:23 2006\n"+
                "EXPIRES:     0\n"+
                "EXPDATE:     never\n"+
                "CERTIFICATE:\n"+
                "-----BEGIN ENVELOPE BODY-----\n"+
                "<authz>\n"+
                "	<file>\n"+
                "		<lfn>test_1</lfn>\n"+
                "		<access>read</access>\n"+
                "		<turl>root://localhost:1234/pnfs/desy.de/data/test1.root</turl>\n"+
                "	</file>\n"+
                "	<file>\n"+
                "		<lfn>test2</lfn>\n"+
                "		<access>write-once</access>\n"+
                "		<turl>root://localhost:1234/pnfs/desy.de/data/test2.root</turl>\n"+
                "	</file>\n"+
                "</authz>\n"+
                "\n"+
                "-----END ENVELOPE BODY-----\n"+
                "-----END ENVELOPE-----\n";

            Envelope env = new Envelope(envString);
            assertTrue(env.getExpirationTime() == 0);
            assertTrue(env.getExpirationDate() == null);
            assertTrue(env.isValid());

            int fileNumber = 0;
            for (Iterator it = env.getFiles(); it.hasNext();fileNumber++) {
                it.next();
            }
            assertTrue(fileNumber == 2);



        } catch (CorruptedEnvelopeException e) {
            logger.error(e.getMessage());
            e.printStackTrace();
            assertTrue(false);
        } catch (GeneralSecurityException e) {
            logger.error(e.getMessage());
            e.printStackTrace();
            assertTrue(false);
        }


        String	envString =
            "-----BEGIN ENVELOPE-----\n"+
            "CREATOR:     Martin.Radicke\n"+
            "UNIXTIME:    1156176175\n"+
            "DATE:        Mon 21 Aug 2006 16:02:55 GMT\n"+
            "EXPIRES:     1156176311\n"+
            "EXPDATE:     Mon 21 Aug 2006 16:05:11 GMT\n"+
            "CERTIFICATE:\n"+
            "-----BEGIN ENVELOPE BODY-----\n"+
            "<authz>\n"+
            "	<file>\n"+
            "		<lfn>test_1</lfn>\n"+
            "		<access>read</access>\n"+
            "		<turl>root://localhost:1234/pnfs/desy.de/data/test1.root</turl>\n"+
            "	</file>\n"+
            "	<file>\n"+
            "		<lfn>test2</lfn>\n"+
            "		<access>write-once</access>\n"+
            "		<turl>root://localhost:1234/pnfs/desy.de/data/test2.root</turl>\n"+
            "	</file>\n"+
            "</authz>\n"+
            "\n"+
            "-----END ENVELOPE BODY-----\n"+
            "-----END ENVELOPE-----\n";

        try {

            new Envelope(envString);

        } catch (CorruptedEnvelopeException e) {
            logger.error(e.getMessage());
            e.printStackTrace();
            assertTrue(false);
        } catch (GeneralSecurityException e) {
            logger.debug(e.getMessage() +" - test ok!");
            assertFalse(false);
        }
    }

}
