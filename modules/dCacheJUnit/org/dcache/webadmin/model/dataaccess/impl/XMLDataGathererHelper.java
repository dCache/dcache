package org.dcache.webadmin.model.dataaccess.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.List;
import org.dcache.webadmin.model.businessobjects.MoverQueue;
import org.dcache.webadmin.model.businessobjects.Pool;
import org.dcache.webadmin.view.beans.PoolSpaceBean;

/**
 * XML-Data Source for Unit Tests with standardized XML-Output.
 * @author jans
 */
public class XMLDataGathererHelper {

    public static final String POOL1_NAME = "myFirstPool";
    public static final String POOL1_DOMAIN = "patrickxenvm02Domain";
    public static final boolean IS_POOL1_ENABLED = true;
    public static final long POOL1_FREE_SPACE = 2147367538;
    public static final long POOL1_PRECIOUS_SPACE = 16110;
    public static final long POOL1_TOTAL_SPACE = 2147483648L;
    public static final long POOL1_USED_SPACE = 116110;
    public static final long POOL1_REMOVABLE_SPACE = 100000;
    public static final MoverQueue POOL1_MOVERS = new MoverQueue("mover", 0, 100, 0);
    public static final MoverQueue POOL1_RESTORES = new MoverQueue("restore", 0, 100, 0);
    public static final MoverQueue POOL1_STORES = new MoverQueue("store", 0, 100, 0);
    public static final MoverQueue POOL1_P2PSERVER = new MoverQueue("p2p-queue", 0, 100, 0);
    public static final MoverQueue POOL1_P2PCLIENT = new MoverQueue("p2p-clientqueue", 10, 10, 10);
    public static final String POOL1_POOLGROUP1 = "default";
    public static final List<String> POOL1_POOLGROUPS = Arrays.asList(POOL1_POOLGROUP1);
    public static final String POOL2_NAME = "mySecondPool";
    public static final String POOL2_DOMAIN = "mySecondPoolDomain";
    public static final boolean IS_POOL2_ENABLED = false;
    public static final long POOL2_FREE_SPACE = 0;
    public static final long POOL2_PRECIOUS_SPACE = 0;
    public static final long POOL2_TOTAL_SPACE = 0;
    public static final long POOL2_USED_SPACE = 0;
    public static final String DOOR1_NAME = "DCap-patrickxenvm02@dcap-patrickxenvm02Domain";
    public static final String DOOR2_NAME = "DCap-gsi-patrickxenvm02@gsidcap-patrickxenvm02Domain";
    public static final String DOOR3_NAME = "GFTP-patrickxenvm02@gridftp-patrickxenvm02Domain";
    public static final String TESTCELL_NAME = POOL1_NAME;
    public static final String TESTCELL_DOMAIN = POOL1_DOMAIN;
    public static final String TESTCELL2_NAME = POOL2_NAME;
    public static final String TESTCELL2_DOMAIN = POOL2_DOMAIN;
    public static final String EMPTY_TESTCELL_NAME = "";
    public static final String EMPTY_TESTCELL_DOMAIN = "";
    public static final String emptyXmlcontent = "";
    public static final String poolsXmlcontent = "<?xml version=\"1.0\"?>" +
            "<dCache xmlns=\"http://www.dcache.org/2008/01/Info\">" +
            "<pools><pool name=\"" + POOL1_NAME + "\">" +
            "<metric name=\"enabled\" type=\"boolean\">" + IS_POOL1_ENABLED + "</metric>" +
            "      <metric name=\"read-only\" type=\"boolean\">false</metric>" +
            "      <metric name=\"last-heartbeat\" type=\"integer\">4396</metric>" +
            "      <queues>        <queue type=\"store\">" +
            "          <metric name=\"max-active\" type=\"integer\">" + POOL1_STORES.getMax() + "</metric>" +
            "          <metric name=\"queued\" type=\"integer\">" + POOL1_STORES.getQueued() + "</metric>" +
            "          <metric name=\"active\" type=\"integer\">" + POOL1_STORES.getActive() + "</metric>" +
            "        </queue>" +
            "        <queue type=\"mover\">" +
            "          <metric name=\"max-active\" type=\"integer\">" + POOL1_MOVERS.getMax() + "</metric>" +
            "          <metric name=\"queued\" type=\"integer\">" + POOL1_MOVERS.getQueued() + "</metric>" +
            "          <metric name=\"active\" type=\"integer\">" + POOL1_MOVERS.getActive() + "</metric>" +
            "        </queue>        <queue type=\"restore\">" +
            "          <metric name=\"max-active\" type=\"integer\">" + POOL1_RESTORES.getMax() + "</metric>" +
            "          <metric name=\"queued\" type=\"integer\">" + POOL1_RESTORES.getQueued() + "</metric>" +
            "          <metric name=\"active\" type=\"integer\">" + POOL1_RESTORES.getActive() + "</metric>" +
            "        </queue>        <queue type=\"p2p-clientqueue\">" +
            "          <metric name=\"max-active\" type=\"integer\">" + POOL1_P2PCLIENT.getMax() + "</metric>" +
            "          <metric name=\"queued\" type=\"integer\">" + POOL1_P2PCLIENT.getQueued() + "</metric>" +
            "          <metric name=\"active\" type=\"integer\">" + POOL1_P2PCLIENT.getActive() + "</metric>" +
            "        </queue>        <queue type=\"p2p-queue\">" +
            "          <metric name=\"max-active\" type=\"integer\">" + POOL1_P2PSERVER.getMax() + "</metric>" +
            "          <metric name=\"queued\" type=\"integer\">" + POOL1_P2PSERVER.getQueued() + "</metric> " +
            "      <metric name=\"active\" type=\"integer\">" + POOL1_P2PSERVER.getActive() + "</metric>" +
            "        </queue>      </queues>      <space>" +
            "        <metric name=\"total\" type=\"integer\">" + POOL1_TOTAL_SPACE + "</metric>" +
            "        <metric name=\"free\" type=\"integer\">" + POOL1_FREE_SPACE + "</metric>" +
            "        <metric name=\"break-even\" type=\"float\">0.7</metric>" +
            "        <metric name=\"LRU-seconds\" type=\"integer\">1255516612</metric>" +
            "        <metric name=\"precious\" type=\"integer\">" + POOL1_PRECIOUS_SPACE + "</metric>" +
            "        <metric name=\"removable\" type=\"integer\">" + POOL1_REMOVABLE_SPACE + "</metric>" +
            "        <metric name=\"used\" type=\"integer\">" + POOL1_USED_SPACE + "</metric>" +
            "        <metric name=\"gap\" type=\"integer\">1048576</metric>" +
            "      </space>      <poolgroups>        <poolgroupref name=\"" + POOL1_POOLGROUP1 + "\"/>" +
            "      </poolgroups>    </pool>  </pools></dCache>";
    public static final String poolGroupXmlcontent = "<?xml version=\"1.0\"?>" +
            "<dCache xmlns=\"http://www.dcache.org/2008/01/Info\">" +
            "<poolgroups>" +
            " <poolgroup name=\"" + POOL1_POOLGROUP1 + "\">" +
            "  <links>" +
            "   <linkref name=\"default-link\"/>" +
            "   </links>" +
            "   <space>" +
            "     <metric name=\"total\" type=\"integer\">3221225472</metric>" +
            "        <metric name=\"free\" type=\"integer\">3221186835</metric>" +
            "        <metric name=\"removable\" type=\"integer\">0</metric>" +
            "        <metric name=\"precious\" type=\"integer\">38637</metric>" +
            "        <metric name=\"used\" type=\"integer\">38637</metric>" +
            "      </space>" +
            "      <pools>" +
            "        <poolref name=\"mySecondPool\"/>" +
            "<poolref name=\"myFirstPool\"/>" +
            "    </pools> </poolgroup>" +
            "    <poolgroup name=\"ResilientPools\">" +
            "      <space>" +
            "        <metric name=\"total\" type=\"integer\">0</metric>" +
            "        <metric name=\"free\" type=\"integer\">0</metric>" +
            "        <metric name=\"precious\" type=\"integer\">0</metric>" +
            "        <metric name=\"removable\" type=\"integer\">0</metric>" +
            "        <metric name=\"used\" type=\"integer\">0</metric>" +
            "      </space>" +
            "    </poolgroup>" +
            "  </poolgroups>" +
            "</dCache>";
    public static final String doorsXmlcontent = "<?xml version=\"1.0\"?> " +
            "<dCache xmlns=\"http://www.dcache.org/2008/01/Info\">" +
            "  <doors>" +
            "    <door name=\"" + DOOR1_NAME + "\">" +
            "      <metric name=\"port\" type=\"integer\">22125</metric>" +
            "      <metric name=\"update-time\" type=\"integer\">30000</metric>" +
            "      <protocol>" +
            "        <metric name=\"engine\" type=\"string\">diskCacheV111.doors.DCapDoor</metric>" +
            "        <metric name=\"family\" type=\"string\">dcap</metric>" +
            "        <metric name=\"version\" type=\"string\">1.3.0</metric>" +
            "      </protocol>" +
            "      <metric name=\"cell\" type=\"string\">DCap-patrickxenvm02</metric>" +
            "      <metric name=\"domain\" type=\"string\">dcap-patrickxenvm02Domain</metric>" +
            "      <interfaces>" +
            "        <interface name=\"patrickxenvm02.desy.de\">" +
            "          <metric name=\"address-type\" type=\"string\">IPv4</metric>" +
            "          <metric name=\"order\" type=\"integer\">1</metric>" +
            "          <metric name=\"address\" type=\"string\">131.169.72.127</metric>" +
            "          <metric name=\"name\" type=\"string\">patrickxenvm02.desy.de</metric>" +
            "          <metric name=\"FQDN\" type=\"string\">patrickxenvm02.desy.de</metric>" +
            "        </interface>" +
            "      </interfaces>" +
            "      <metric name=\"load\" type=\"float\">0.0</metric>" +
            "    </door>" +
            "    <door name=\"" + DOOR2_NAME + "\">" +
            "      <metric name=\"port\" type=\"integer\">22128</metric>" +
            "      <metric name=\"update-time\" type=\"integer\">120000</metric>" +
            "      <protocol>" +
            "        <metric name=\"engine\" type=\"string\">diskCacheV111.doors.DCapDoor</metric>" +
            "        <metric name=\"family\" type=\"string\">gsidcap</metric>" +
            "        <metric name=\"version\" type=\"string\">1.3.0</metric>" +
            "      </protocol>" +
            "      <metric name=\"cell\" type=\"string\">DCap-gsi-patrickxenvm02</metric>" +
            "      <metric name=\"domain\" type=\"string\">gsidcap-patrickxenvm02Domain</metric>" +
            "     <interfaces>" +
            "        <interface name=\"patrickxenvm02.desy.de\">" +
            "         <metric name=\"address-type\" type=\"string\">IPv4</metric>" +
            "         <metric name=\"order\" type=\"integer\">1</metric>" +
            "         <metric name=\"address\" type=\"string\">131.169.72.127</metric>" +
            "         <metric name=\"name\" type=\"string\">patrickxenvm02.desy.de</metric>" +
            "         <metric name=\"FQDN\" type=\"string\">patrickxenvm02.desy.de</metric>" +
            "       </interface>" +
            "     </interfaces>" +
            "     <metric name=\"load\" type=\"float\">0.0</metric>" +
            "   </door>" +
            "   <door name=\"" + DOOR3_NAME + "\">" +
            "     <metric name=\"port\" type=\"integer\">2811</metric>" +
            "     <metric name=\"update-time\" type=\"integer\">5000</metric>" +
            "     <protocol>" +
            "      <metric name=\"engine\" type=\"string\">diskCacheV111.doors.GsiFtpDoorV1</metric>" +
            "      <metric name=\"family\" type=\"string\">gsiftp</metric>" +
            "      <metric name=\"version\" type=\"string\">1.0.0</metric>" +
            "    </protocol>" +
            "     <metric name=\"cell\" type=\"string\">GFTP-patrickxenvm02</metric>" +
            "     <metric name=\"domain\" type=\"string\">gridftp-patrickxenvm02Domain</metric>" +
            "    <interfaces>" +
            "      <interface name=\"patrickxenvm02.desy.de\">" +
            "        <metric name=\"address-type\" type=\"string\">IPv4</metric>" +
            "        <metric name=\"order\" type=\"integer\">1</metric>" +
            "        <metric name=\"address\" type=\"string\">131.169.72.127</metric>" +
            "       <metric name=\"name\" type=\"string\">patrickxenvm02.desy.de</metric>" +
            "        <metric name=\"FQDN\" type=\"string\">patrickxenvm02.desy.de</metric>" +
            "      </interface>      </interfaces>" +
            "    <metric name=\"load\" type=\"float\">0.0</metric>    </door>  </doors></dCache>";
    public static final String domainsXmlcontent = "<?xml version=\"1.0\"?><dCache xmlns=\"http://www.dcache.org/2008/01/Info\">" +
            "  <domains>    " +
            "    <domain name=\"gPlazma-patrickxenvm02Domain\">" +
            "      <cells>" +
            "        <cell name=\"c-dCacheDomain-101\">" +
            "          <metric name=\"event-queue-size\" type=\"integer\">0</metric>" +
            "          <created>" +
            "            <metric name=\"ISO-8601\" type=\"string\">2010-07-14T12:32Z</metric>" +
            "            <metric name=\"simple\" type=\"string\">Jul 14, 14:32:58 CEST</metric>" +
            "            <metric name=\"unix\" type=\"integer\">1279110778</metric>" +
            "          </created>" +
            "          <metric name=\"thread-count\" type=\"integer\">1</metric>" +
            "          <metric name=\"class\" type=\"string\">dmg.cells.network.LocationManagerConnector</metric>" +
            "          <metric name=\"type\" type=\"string\">System</metric>" +
            "          <version>" +
            "            <metric name=\"revision\" type=\"string\">CA-1.28</metric>" +
            "            <metric name=\"release\" type=\"string\">cells</metric>" +
            "          </version>" +
            "        </cell>" +
            "        <cell name=\"lm\">" +
            "          <metric name=\"event-queue-size\" type=\"integer\">0</metric>" +
            "          <created>" +
            "            <metric name=\"ISO-8601\" type=\"string\">2010-07-14T12:32Z</metric>" +
            "            <metric name=\"simple\" type=\"string\">Jul 14, 14:32:58 CEST</metric>" +
            "            <metric name=\"unix\" type=\"integer\">1279110778</metric>" +
            "          </created>" +
            "          <metric name=\"thread-count\" type=\"integer\">2</metric>" +
            "          <metric name=\"class\" type=\"string\">dmg.cells.services.LocationManager</metric>" +
            "          <metric name=\"type\" type=\"string\">Generic</metric>" +
            "          <version>" +
            "            <metric name=\"revision\" type=\"string\">CA-1.28</metric>" +
            "            <metric name=\"release\" type=\"string\">cells</metric>" +
            "          </version>" +
            "        </cell>" +
            "        <cell name=\"c-dCacheDomain-101-102\">" +
            "          <metric name=\"event-queue-size\" type=\"integer\">0</metric>" +
            "          <created>" +
            "            <metric name=\"ISO-8601\" type=\"string\">2010-07-14T12:32Z</metric>" +
            "            <metric name=\"simple\" type=\"string\">Jul 14, 14:32:58 CEST</metric>" +
            "            <metric name=\"unix\" type=\"integer\">1279110778</metric>" +
            "          </created>" +
            "          <metric name=\"thread-count\" type=\"integer\">2</metric>" +
            "          <metric name=\"class\" type=\"string\">dmg.cells.network.LocationMgrTunnel</metric>" +
            "          <metric name=\"type\" type=\"string\">System</metric>" +
            "          <version>" +
            "            <metric name=\"revision\" type=\"string\">CA-1.28</metric>" +
            "            <metric name=\"release\" type=\"string\">cells</metric>" +
            "          </version>" +
            "        </cell>" +
            "        <cell name=\"gPlazma\">" +
            "          <metric name=\"event-queue-size\" type=\"integer\">0</metric>" +
            "          <created>" +
            "            <metric name=\"ISO-8601\" type=\"string\">2010-07-14T12:32Z</metric>" +
            "            <metric name=\"simple\" type=\"string\">Jul 14, 14:32:58 CEST</metric>" +
            "            <metric name=\"unix\" type=\"integer\">1279110778</metric>" +
            "          </created>" +
            "          <metric name=\"thread-count\" type=\"integer\">32</metric>" +
            "          <metric name=\"class\" type=\"string\">org.dcache.cells.UniversalSpringCell</metric>" +
            "          <metric name=\"type\" type=\"string\">Generic</metric>" +
            "          <version>" +
            "            <metric name=\"revision\" type=\"string\">14038</metric>" +
            "            <metric name=\"release\" type=\"string\">production-1.9.10-1rc</metric>" +
            "          </version>" +
            "        </cell>" +
            "        <cell name=\"RoutingMgr\">" +
            "          <metric name=\"event-queue-size\" type=\"integer\">0</metric>" +
            "          <created>" +
            "            <metric name=\"ISO-8601\" type=\"string\">2010-07-14T12:32Z</metric>" +
            "            <metric name=\"simple\" type=\"string\">Jul 14, 14:32:58 CEST</metric>" +
            "            <metric name=\"unix\" type=\"integer\">1279110778</metric>" +
            "          </created>" +
            "          <metric name=\"thread-count\" type=\"integer\">1</metric>" +
            "          <metric name=\"class\" type=\"string\">dmg.cells.services.RoutingManager</metric>" +
            "          <metric name=\"type\" type=\"string\">System</metric>" +
            "          <version>" +
            "            <metric name=\"revision\" type=\"string\">CA-1.28</metric>" +
            "            <metric name=\"release\" type=\"string\">cells</metric>" +
            "          </version>" +
            "        </cell>" +
            "        <cell name=\"System\">" +
            "          <metric name=\"event-queue-size\" type=\"integer\">0</metric>" +
            "          <created>" +
            "            <metric name=\"ISO-8601\" type=\"string\">2010-07-14T12:32Z</metric>" +
            "            <metric name=\"simple\" type=\"string\">Jul 14, 14:32:58 CEST</metric>" +
            "            <metric name=\"unix\" type=\"integer\">1279110778</metric>" +
            "          </created>" +
            "          <metric name=\"thread-count\" type=\"integer\">1</metric>" +
            "          <metric name=\"class\" type=\"string\">dmg.cells.nucleus.SystemCell</metric>" +
            "          <metric name=\"type\" type=\"string\">System</metric>" +
            "          <version>" +
            "            <metric name=\"revision\" type=\"string\">CA-1.28</metric>" +
            "            <metric name=\"release\" type=\"string\">cells</metric>" +
            "          </version>" +
            "        </cell>" +
            "      </cells>" +
            "      <metric name=\"address\" type=\"string\">System@dCacheDomain:System@gPlazma-patrickxenvm02Domain</metric>" +
            "      <routing>" +
            "        <local>" +
            "          <cellref name=\"gPlazma\"/>" +
            "        </local>" +
            "        <named-cells>" +
            "          <cell name=\"gPlazma\">" +
            "            <domainref name=\"gPlazma-patrickxenvm02Domain\"/>" +
            "          </cell>" +
            "        </named-cells>" +
            "      </routing>" +
            "    </domain>" +
            "    <domain name=\"gsidcap-patrickxenvm02Domain\">" +
            "      <cells>" +
            "        <cell name=\"c-dCacheDomain-101\">" +
            "          <metric name=\"event-queue-size\" type=\"integer\">0</metric>" +
            "          <created>" +
            "            <metric name=\"ISO-8601\" type=\"string\">2010-07-12T08:33Z</metric>" +
            "            <metric name=\"simple\" type=\"string\">Jul 12, 10:33:02 CEST</metric>" +
            "            <metric name=\"unix\" type=\"integer\">1278923582</metric>" +
            "          </created>" +
            "          <metric name=\"thread-count\" type=\"integer\">1</metric>" +
            "          <metric name=\"class\" type=\"string\">dmg.cells.network.LocationManagerConnector</metric>" +
            "          <metric name=\"type\" type=\"string\">System</metric>" +
            "          <version>" +
            "            <metric name=\"revision\" type=\"string\">CA-1.28</metric>" +
            "            <metric name=\"release\" type=\"string\">cells</metric>" +
            "          </version>" +
            "        </cell>" +
            "        <cell name=\"lm\">" +
            "          <metric name=\"event-queue-size\" type=\"integer\">0</metric>" +
            "          <created>" +
            "            <metric name=\"ISO-8601\" type=\"string\">2010-07-12T08:33Z</metric>" +
            "            <metric name=\"simple\" type=\"string\">Jul 12, 10:33:02 CEST</metric>" +
            "            <metric name=\"unix\" type=\"integer\">1278923582</metric>" +
            "          </created>" +
            "          <metric name=\"thread-count\" type=\"integer\">2</metric>" +
            "          <metric name=\"class\" type=\"string\">dmg.cells.services.LocationManager</metric>" +
            "          <metric name=\"type\" type=\"string\">Generic</metric>" +
            "          <version>" +
            "            <metric name=\"revision\" type=\"string\">CA-1.28</metric>" +
            "            <metric name=\"release\" type=\"string\">cells</metric>" +
            "          </version>" +
            "        </cell>" +
            "        <cell name=\"DCap-gsi-patrickxenvm02\">" +
            "          <metric name=\"event-queue-size\" type=\"integer\">0</metric>" +
            "          <created>" +
            "            <metric name=\"ISO-8601\" type=\"string\">2010-07-12T08:33Z</metric>" +
            "            <metric name=\"simple\" type=\"string\">Jul 12, 10:33:02 CEST</metric>" +
            "            <metric name=\"unix\" type=\"integer\">1278923582</metric>" +
            "          </created>" +
            "          <metric name=\"thread-count\" type=\"integer\">4</metric>" +
            "          <metric name=\"class\" type=\"string\">dmg.cells.services.login.LoginManager</metric>" +
            "          <metric name=\"type\" type=\"string\">Generic</metric>" +
            "          <version>" +
            "            <metric name=\"revision\" type=\"string\">1.17</metric>" +
            "            <metric name=\"release\" type=\"string\">production-1.9.10-1rc</metric>" +
            "          </version>" +
            "        </cell>" +
            "        <cell name=\"c-dCacheDomain-101-102\">" +
            "          <metric name=\"event-queue-size\" type=\"integer\">0</metric>" +
            "          <created>" +
            "            <metric name=\"ISO-8601\" type=\"string\">2010-07-12T08:33Z</metric>" +
            "            <metric name=\"simple\" type=\"string\">Jul 12, 10:33:03 CEST</metric>" +
            "            <metric name=\"unix\" type=\"integer\">1278923583</metric>" +
            "          </created>" +
            "          <metric name=\"thread-count\" type=\"integer\">2</metric>" +
            "          <metric name=\"class\" type=\"string\">dmg.cells.network.LocationMgrTunnel</metric>" +
            "          <metric name=\"type\" type=\"string\">System</metric>" +
            "          <version>" +
            "            <metric name=\"revision\" type=\"string\">CA-1.28</metric>" +
            "            <metric name=\"release\" type=\"string\">cells</metric>" +
            "          </version>" +
            "        </cell>" +
            "        <cell name=\"RoutingMgr\">" +
            "          <metric name=\"event-queue-size\" type=\"integer\">0</metric>" +
            "          <created>" +
            "            <metric name=\"ISO-8601\" type=\"string\">2010-07-12T08:33Z</metric>" +
            "            <metric name=\"simple\" type=\"string\">Jul 12, 10:33:02 CEST</metric>" +
            "            <metric name=\"unix\" type=\"integer\">1278923582</metric>" +
            "          </created>" +
            "          <metric name=\"thread-count\" type=\"integer\">1</metric>" +
            "          <metric name=\"class\" type=\"string\">dmg.cells.services.RoutingManager</metric>" +
            "          <metric name=\"type\" type=\"string\">System</metric>" +
            "          <version>" +
            "            <metric name=\"revision\" type=\"string\">CA-1.28</metric>" +
            "            <metric name=\"release\" type=\"string\">cells</metric>" +
            "          </version>" +
            "        </cell>" +
            "        <cell name=\"System\">" +
            "          <metric name=\"event-queue-size\" type=\"integer\">0</metric>" +
            "          <created>" +
            "            <metric name=\"ISO-8601\" type=\"string\">2010-07-12T08:33Z</metric>" +
            "            <metric name=\"simple\" type=\"string\">Jul 12, 10:33:02 CEST</metric>" +
            "            <metric name=\"unix\" type=\"integer\">1278923582</metric>" +
            "          </created>" +
            "          <metric name=\"thread-count\" type=\"integer\">1</metric>" +
            "          <metric name=\"class\" type=\"string\">dmg.cells.nucleus.SystemCell</metric>" +
            "          <metric name=\"type\" type=\"string\">System</metric>" +
            "          <version>" +
            "            <metric name=\"revision\" type=\"string\">CA-1.28</metric>" +
            "            <metric name=\"release\" type=\"string\">cells</metric>" +
            "          </version>" +
            "        </cell>" +
            "      </cells>" +
            "      <metric name=\"address\" type=\"string\">System@dCacheDomain:System@gsidcap-patrickxenvm02Domain</metric>" +
            "      <routing>" +
            "        <local>" +
            "          <cellref name=\"DCap-gsi-patrickxenvm02\"/>" +
            "        </local>" +
            "        <named-cells>" +
            "          <cell name=\"DCap-gsi-patrickxenvm02\">" +
            "            <domainref name=\"gsidcap-patrickxenvm02Domain\"/>" +
            "          </cell>" +
            "        </named-cells>" +
            "      </routing>" +
            "    </domain>" +
            "    <domain name=\"dCacheDomain\">" +
            "      <cells>" +
            "        <cell name=\"l-101-&lt;unknown&gt;-163\">" +
            "          <metric name=\"event-queue-size\" type=\"integer\">0</metric>" +
            "          <created>" +
            "            <metric name=\"ISO-8601\" type=\"string\">2010-07-16T08:25Z</metric>" +
            "            <metric name=\"simple\" type=\"string\">Jul 16, 10:25:56 CEST</metric>" +
            "            <metric name=\"unix\" type=\"integer\">1279268756</metric>" +
            "          </created>" +
            "          <metric name=\"thread-count\" type=\"integer\">2</metric>" +
            "          <metric name=\"class\" type=\"string\">dmg.cells.network.LocationMgrTunnel</metric>" +
            "          <metric name=\"type\" type=\"string\">System</metric>" +
            "          <version>" +
            "            <metric name=\"revision\" type=\"string\">CA-1.28</metric>" +
            "            <metric name=\"release\" type=\"string\">cells</metric>" +
            "          </version>" +
            "        </cell>" +
            "        <cell name=\"lm\">" +
            "          <metric name=\"event-queue-size\" type=\"integer\">0</metric>" +
            "          <created>" +
            "            <metric name=\"ISO-8601\" type=\"string\">2010-07-12T08:32Z</metric>" +
            "            <metric name=\"simple\" type=\"string\">Jul 12, 10:32:51 CEST</metric>" +
            "            <metric name=\"unix\" type=\"integer\">1278923571</metric>" +
            "          </created>" +
            "          <metric name=\"thread-count\" type=\"integer\">2</metric>" +
            "          <metric name=\"class\" type=\"string\">dmg.cells.services.LocationManager</metric>" +
            "          <metric name=\"type\" type=\"string\">Generic</metric>" +
            "          <version>" +
            "            <metric name=\"revision\" type=\"string\">CA-1.28</metric>" +
            "            <metric name=\"release\" type=\"string\">cells</metric>" +
            "          </version>" +
            "        </cell>" +
            "        <cell name=\"broadcast\">" +
            "          <metric name=\"event-queue-size\" type=\"integer\">0</metric>" +
            "          <created>" +
            "            <metric name=\"ISO-8601\" type=\"string\">2010-07-12T08:32Z</metric>" +
            "            <metric name=\"simple\" type=\"string\">Jul 12, 10:32:53 CEST</metric>" +
            "            <metric name=\"unix\" type=\"integer\">1278923573</metric>" +
            "          </created>" +
            "          <metric name=\"thread-count\" type=\"integer\">1</metric>" +
            "          <metric name=\"class\" type=\"string\">dmg.cells.services.multicaster.BroadcastCell</metric>" +
            "          <metric name=\"type\" type=\"string\">Generic</metric>" +
            "          <version>" +
            "            <metric name=\"revision\" type=\"string\">CA-1.28</metric>" +
            "            <metric name=\"release\" type=\"string\">cells</metric>" +
            "          </version>" +
            "        </cell>" +
            "        <cell name=\"RoutingMgr\">" +
            "          <metric name=\"event-queue-size\" type=\"integer\">0</metric>" +
            "          <created>" +
            "            <metric name=\"ISO-8601\" type=\"string\">2010-07-12T08:32Z</metric>" +
            "            <metric name=\"simple\" type=\"string\">Jul 12, 10:32:51 CEST</metric>" +
            "            <metric name=\"unix\" type=\"integer\">1278923571</metric>" +
            "          </created>" +
            "          <metric name=\"thread-count\" type=\"integer\">1</metric>" +
            "          <metric name=\"class\" type=\"string\">dmg.cells.services.RoutingManager</metric>" +
            "          <metric name=\"type\" type=\"string\">System</metric>" +
            "          <version>" +
            "            <metric name=\"revision\" type=\"string\">CA-1.28</metric>" +
            "            <metric name=\"release\" type=\"string\">cells</metric>" +
            "          </version>" +
            "        </cell>" +
            "        <cell name=\"PoolManager\">" +
            "          <metric name=\"event-queue-size\" type=\"integer\">0</metric>" +
            "          <created>" +
            "            <metric name=\"ISO-8601\" type=\"string\">2010-07-12T08:32Z</metric>" +
            "            <metric name=\"simple\" type=\"string\">Jul 12, 10:32:52 CEST</metric>" +
            "            <metric name=\"unix\" type=\"integer\">1278923572</metric>" +
            "          </created>" +
            "          <metric name=\"thread-count\" type=\"integer\">3</metric>" +
            "          <metric name=\"class\" type=\"string\">PoolManager</metric>" +
            "          <metric name=\"type\" type=\"string\">Generic</metric>" +
            "          <version>" +
            "            <metric name=\"revision\" type=\"string\">14038</metric>" +
            "            <metric name=\"release\" type=\"string\">production-1.9.10-1rc</metric>" +
            "          </version>" +
            "        </cell>" +
            "        <cell name=\"l-101-&lt;unknown&gt;-102\">" +
            "          <metric name=\"event-queue-size\" type=\"integer\">0</metric>" +
            "          <created>" +
            "            <metric name=\"ISO-8601\" type=\"string\">2010-07-12T08:32Z</metric>" +
            "            <metric name=\"simple\" type=\"string\">Jul 12, 10:32:54 CEST</metric>" +
            "            <metric name=\"unix\" type=\"integer\">1278923574</metric>" +
            "          </created>" +
            "          <metric name=\"thread-count\" type=\"integer\">2</metric>" +
            "          <metric name=\"class\" type=\"string\">dmg.cells.network.LocationMgrTunnel</metric>" +
            "          <metric name=\"type\" type=\"string\">System</metric>" +
            "          <version>" +
            "            <metric name=\"revision\" type=\"string\">CA-1.28</metric>" +
            "            <metric name=\"release\" type=\"string\">cells</metric>" +
            "          </version>" +
            "        </cell>" +
            "        <cell name=\"l-101-&lt;unknown&gt;-103\">" +
            "          <metric name=\"event-queue-size\" type=\"integer\">0</metric>" +
            "          <created>" +
            "            <metric name=\"ISO-8601\" type=\"string\">2010-07-12T08:32Z</metric>" +
            "            <metric name=\"simple\" type=\"string\">Jul 12, 10:32:54 CEST</metric>" +
            "            <metric name=\"unix\" type=\"integer\">1278923574</metric>" +
            "          </created>" +
            "          <metric name=\"thread-count\" type=\"integer\">2</metric>" +
            "          <metric name=\"class\" type=\"string\">dmg.cells.network.LocationMgrTunnel</metric>" +
            "          <metric name=\"type\" type=\"string\">System</metric>" +
            "          <version>" +
            "            <metric name=\"revision\" type=\"string\">CA-1.28</metric>" +
            "            <metric name=\"release\" type=\"string\">cells</metric>" +
            "          </version>" +
            "        </cell>" +
            "        <cell name=\"l-101-&lt;unknown&gt;-107\">" +
            "          <metric name=\"event-queue-size\" type=\"integer\">0</metric>" +
            "          <created>" +
            "            <metric name=\"ISO-8601\" type=\"string\">2010-07-12T08:32Z</metric>" +
            "            <metric name=\"simple\" type=\"string\">Jul 12, 10:32:58 CEST</metric>" +
            "            <metric name=\"unix\" type=\"integer\">1278923578</metric>" +
            "          </created>" +
            "          <metric name=\"thread-count\" type=\"integer\">2</metric>" +
            "          <metric name=\"class\" type=\"string\">dmg.cells.network.LocationMgrTunnel</metric>" +
            "          <metric name=\"type\" type=\"string\">System</metric>" +
            "          <version>" +
            "            <metric name=\"revision\" type=\"string\">CA-1.28</metric>" +
            "            <metric name=\"release\" type=\"string\">cells</metric>" +
            "          </version>" +
            "        </cell>" +
            "        <cell name=\"System\">" +
            "          <metric name=\"event-queue-size\" type=\"integer\">0</metric>" +
            "          <created>" +
            "            <metric name=\"ISO-8601\" type=\"string\">2010-07-12T08:32Z</metric>" +
            "            <metric name=\"simple\" type=\"string\">Jul 12, 10:32:51 CEST</metric>" +
            "            <metric name=\"unix\" type=\"integer\">1278923571</metric>" +
            "          </created>" +
            "          <metric name=\"thread-count\" type=\"integer\">1</metric>" +
            "          <metric name=\"class\" type=\"string\">dmg.cells.nucleus.SystemCell</metric>" +
            "          <metric name=\"type\" type=\"string\">System</metric>" +
            "          <version>" +
            "            <metric name=\"revision\" type=\"string\">CA-1.28</metric>" +
            "            <metric name=\"release\" type=\"string\">cells</metric>" +
            "          </version>" +
            "        </cell>" +
            "        <cell name=\"l-101-&lt;unknown&gt;-104\">" +
            "          <metric name=\"event-queue-size\" type=\"integer\">0</metric>" +
            "          <created>" +
            "            <metric name=\"ISO-8601\" type=\"string\">2010-07-12T08:32Z</metric>" +
            "            <metric name=\"simple\" type=\"string\">Jul 12, 10:32:55 CEST</metric>" +
            "            <metric name=\"unix\" type=\"integer\">1278923575</metric>" +
            "          </created>" +
            "          <metric name=\"thread-count\" type=\"integer\">2</metric>" +
            "          <metric name=\"class\" type=\"string\">dmg.cells.network.LocationMgrTunnel</metric>" +
            "          <metric name=\"type\" type=\"string\">System</metric>" +
            "          <version>" +
            "            <metric name=\"revision\" type=\"string\">CA-1.28</metric>" +
            "            <metric name=\"release\" type=\"string\">cells</metric>" +
            "          </version>" +
            "        </cell>" +
            "        <cell name=\"l-101-&lt;unknown&gt;-105\">" +
            "          <metric name=\"event-queue-size\" type=\"integer\">0</metric>" +
            "          <created>" +
            "            <metric name=\"ISO-8601\" type=\"string\">2010-07-12T08:32Z</metric>" +
            "            <metric name=\"simple\" type=\"string\">Jul 12, 10:32:55 CEST</metric>" +
            "            <metric name=\"unix\" type=\"integer\">1278923575</metric>" +
            "          </created>" +
            "          <metric name=\"thread-count\" type=\"integer\">2</metric>" +
            "          <metric name=\"class\" type=\"string\">dmg.cells.network.LocationMgrTunnel</metric>" +
            "          <metric name=\"type\" type=\"string\">System</metric>" +
            "          <version>" +
            "            <metric name=\"revision\" type=\"string\">CA-1.28</metric>" +
            "            <metric name=\"release\" type=\"string\">cells</metric>" +
            "          </version>" +
            "        </cell>" +
            "        <cell name=\"l-101-&lt;unknown&gt;-109\">" +
            "          <metric name=\"event-queue-size\" type=\"integer\">0</metric>" +
            "          <created>" +
            "            <metric name=\"ISO-8601\" type=\"string\">2010-07-12T08:33Z</metric>" +
            "            <metric name=\"simple\" type=\"string\">Jul 12, 10:33:02 CEST</metric>" +
            "            <metric name=\"unix\" type=\"integer\">1278923582</metric>" +
            "          </created>" +
            "          <metric name=\"thread-count\" type=\"integer\">2</metric>" +
            "          <metric name=\"class\" type=\"string\">dmg.cells.network.LocationMgrTunnel</metric>" +
            "          <metric name=\"type\" type=\"string\">System</metric>" +
            "          <version>" +
            "            <metric name=\"revision\" type=\"string\">CA-1.28</metric>" +
            "            <metric name=\"release\" type=\"string\">cells</metric>" +
            "          </version>" +
            "        </cell>" +
            "        <cell name=\"l-101-&lt;unknown&gt;-108\">" +
            "          <metric name=\"event-queue-size\" type=\"integer\">0</metric>" +
            "          <created>" +
            "            <metric name=\"ISO-8601\" type=\"string\">2010-07-12T08:33Z</metric>" +
            "            <metric name=\"simple\" type=\"string\">Jul 12, 10:33:00 CEST</metric>" +
            "            <metric name=\"unix\" type=\"integer\">1278923580</metric>" +
            "          </created>" +
            "          <metric name=\"thread-count\" type=\"integer\">2</metric>" +
            "          <metric name=\"class\" type=\"string\">dmg.cells.network.LocationMgrTunnel</metric>" +
            "          <metric name=\"type\" type=\"string\">System</metric>" +
            "          <version>" +
            "            <metric name=\"revision\" type=\"string\">CA-1.28</metric>" +
            "            <metric name=\"release\" type=\"string\">cells</metric>" +
            "          </version>" +
            "        </cell>" +
            "        <cell name=\"topo\">" +
            "          <metric name=\"event-queue-size\" type=\"integer\">0</metric>" +
            "          <created>" +
            "            <metric name=\"ISO-8601\" type=\"string\">2010-07-12T08:32Z</metric>" +
            "            <metric name=\"simple\" type=\"string\">Jul 12, 10:32:54 CEST</metric>" +
            "            <metric name=\"unix\" type=\"integer\">1278923574</metric>" +
            "          </created>" +
            "          <metric name=\"thread-count\" type=\"integer\">2</metric>" +
            "          <metric name=\"class\" type=\"string\">TopoCell</metric>" +
            "          <metric name=\"type\" type=\"string\">Generic</metric>" +
            "          <version>" +
            "            <metric name=\"revision\" type=\"string\">14038</metric>" +
            "            <metric name=\"release\" type=\"string\">production-1.9.10-1rc</metric>" +
            "          </version>" +
            "        </cell>" +
            "        <cell name=\"l-101\">" +
            "          <metric name=\"event-queue-size\" type=\"integer\">0</metric>" +
            "          <created>" +
            "            <metric name=\"ISO-8601\" type=\"string\">2010-07-12T08:32Z</metric>" +
            "            <metric name=\"simple\" type=\"string\">Jul 12, 10:32:52 CEST</metric>" +
            "            <metric name=\"unix\" type=\"integer\">1278923572</metric>" +
            "          </created>" +
            "          <metric name=\"thread-count\" type=\"integer\">2</metric>" +
            "          <metric name=\"class\" type=\"string\">dmg.cells.services.login.LoginManager</metric>" +
            "          <metric name=\"type\" type=\"string\">Generic</metric>" +
            "          <version>" +
            "            <metric name=\"revision\" type=\"string\">CA-1.28</metric>" +
            "            <metric name=\"release\" type=\"string\">cells</metric>" +
            "          </version>" +
            "        </cell>" +
            "        <cell name=\"lmd\">" +
            "          <metric name=\"event-queue-size\" type=\"integer\">0</metric>" +
            "          <created>" +
            "            <metric name=\"ISO-8601\" type=\"string\">2010-07-12T08:32Z</metric>" +
            "            <metric name=\"simple\" type=\"string\">Jul 12, 10:32:51 CEST</metric>" +
            "            <metric name=\"unix\" type=\"integer\">1278923571</metric>" +
            "          </created>" +
            "          <metric name=\"thread-count\" type=\"integer\">1</metric>" +
            "          <metric name=\"class\" type=\"string\">dmg.cells.services.LocationManager</metric>" +
            "          <metric name=\"type\" type=\"string\">Generic</metric>" +
            "          <version>" +
            "            <metric name=\"revision\" type=\"string\">CA-1.28</metric>" +
            "            <metric name=\"release\" type=\"string\">cells</metric>" +
            "          </version>" +
            "        </cell>" +
            "        <cell name=\"l-101-&lt;unknown&gt;-110\">" +
            "          <metric name=\"event-queue-size\" type=\"integer\">0</metric>" +
            "          <created>" +
            "            <metric name=\"ISO-8601\" type=\"string\">2010-07-12T08:33Z</metric>" +
            "            <metric name=\"simple\" type=\"string\">Jul 12, 10:33:02 CEST</metric>" +
            "            <metric name=\"unix\" type=\"integer\">1278923582</metric>" +
            "          </created>" +
            "          <metric name=\"thread-count\" type=\"integer\">2</metric>" +
            "          <metric name=\"class\" type=\"string\">dmg.cells.network.LocationMgrTunnel</metric>" +
            "          <metric name=\"type\" type=\"string\">System</metric>" +
            "          <version>" +
            "            <metric name=\"revision\" type=\"string\">CA-1.28</metric>" +
            "            <metric name=\"release\" type=\"string\">cells</metric>" +
            "          </version>" +
            "        </cell>" +
            "        <cell name=\"Prestager\">" +
            "          <metric name=\"event-queue-size\" type=\"integer\">0</metric>" +
            "          <created>" +
            "            <metric name=\"ISO-8601\" type=\"string\">2010-07-12T08:32Z</metric>" +
            "            <metric name=\"simple\" type=\"string\">Jul 12, 10:32:53 CEST</metric>" +
            "            <metric name=\"unix\" type=\"integer\">1278923573</metric>" +
            "          </created>" +
            "          <metric name=\"thread-count\" type=\"integer\">1</metric>" +
            "          <metric name=\"class\" type=\"string\">diskCacheV111.hsmControl.DummyStager</metric>" +
            "          <metric name=\"type\" type=\"string\">Generic</metric>" +
            "          <version>" +
            "            <metric name=\"revision\" type=\"string\">CA-1.28</metric>" +
            "            <metric name=\"release\" type=\"string\">cells</metric>" +
            "          </version>" +
            "        </cell>" +
            "        <cell name=\"l-101-&lt;unknown&gt;-111\">" +
            "          <metric name=\"event-queue-size\" type=\"integer\">0</metric>" +
            "          <created>" +
            "            <metric name=\"ISO-8601\" type=\"string\">2010-07-12T08:33Z</metric>" +
            "            <metric name=\"simple\" type=\"string\">Jul 12, 10:33:03 CEST</metric>" +
            "            <metric name=\"unix\" type=\"integer\">1278923583</metric>" +
            "          </created>" +
            "          <metric name=\"thread-count\" type=\"integer\">2</metric>" +
            "          <metric name=\"class\" type=\"string\">dmg.cells.network.LocationMgrTunnel</metric>" +
            "          <metric name=\"type\" type=\"string\">System</metric>" +
            "          <version>" +
            "            <metric name=\"revision\" type=\"string\">CA-1.28</metric>" +
            "            <metric name=\"release\" type=\"string\">cells</metric>" +
            "          </version>" +
            "        </cell>" +
            "        <cell name=\"l-101-&lt;unknown&gt;-112\">" +
            "          <metric name=\"event-queue-size\" type=\"integer\">0</metric>" +
            "          <created>" +
            "            <metric name=\"ISO-8601\" type=\"string\">2010-07-12T08:33Z</metric>" +
            "            <metric name=\"simple\" type=\"string\">Jul 12, 10:33:04 CEST</metric>" +
            "            <metric name=\"unix\" type=\"integer\">1278923584</metric>" +
            "          </created>" +
            "          <metric name=\"thread-count\" type=\"integer\">2</metric>" +
            "          <metric name=\"class\" type=\"string\">dmg.cells.network.LocationMgrTunnel</metric>" +
            "          <metric name=\"type\" type=\"string\">System</metric>" +
            "          <version>" +
            "            <metric name=\"revision\" type=\"string\">CA-1.28</metric>" +
            "            <metric name=\"release\" type=\"string\">cells</metric>" +
            "          </version>" +
            "        </cell>" +
            "        <cell name=\"l-101-&lt;unknown&gt;-150\">" +
            "          <metric name=\"event-queue-size\" type=\"integer\">0</metric>" +
            "          <created>" +
            "            <metric name=\"ISO-8601\" type=\"string\">2010-07-14T12:32Z</metric>" +
            "            <metric name=\"simple\" type=\"string\">Jul 14, 14:32:58 CEST</metric>" +
            "            <metric name=\"unix\" type=\"integer\">1279110778</metric>" +
            "          </created>" +
            "          <metric name=\"thread-count\" type=\"integer\">2</metric>" +
            "          <metric name=\"class\" type=\"string\">dmg.cells.network.LocationMgrTunnel</metric>" +
            "          <metric name=\"type\" type=\"string\">System</metric>" +
            "          <version>" +
            "            <metric name=\"revision\" type=\"string\">CA-1.28</metric>" +
            "            <metric name=\"release\" type=\"string\">cells</metric>" +
            "          </version>" +
            "        </cell>" +
            "        <cell name=\"LoginBroker\">" +
            "          <metric name=\"event-queue-size\" type=\"integer\">0</metric>" +
            "          <created>" +
            "            <metric name=\"ISO-8601\" type=\"string\">2010-07-12T08:32Z</metric>" +
            "            <metric name=\"simple\" type=\"string\">Jul 12, 10:32:54 CEST</metric>" +
            "            <metric name=\"unix\" type=\"integer\">1278923574</metric>" +
            "          </created>" +
            "          <metric name=\"thread-count\" type=\"integer\">2</metric>" +
            "          <metric name=\"class\" type=\"string\">dmg.cells.services.login.LoginBroker</metric>" +
            "          <metric name=\"type\" type=\"string\">Generic</metric>" +
            "          <version>" +
            "            <metric name=\"revision\" type=\"string\">1.6</metric>" +
            "            <metric name=\"release\" type=\"string\">cells</metric>" +
            "          </version>" +
            "        </cell>" +
            "      </cells>" +
            "      <metric name=\"address\" type=\"string\">System@dCacheDomain</metric>" +
            "    </domain>" +
            "    <domain name=\"patrickxenvm02Domain\">" +
            "      <cells>" +
            "        <cell name=\"c-dCacheDomain-101\">" +
            "          <metric name=\"event-queue-size\" type=\"integer\">0</metric>" +
            "          <created>" +
            "            <metric name=\"ISO-8601\" type=\"string\">2010-07-12T08:32Z</metric>" +
            "            <metric name=\"simple\" type=\"string\">Jul 12, 10:32:59 CEST</metric>" +
            "            <metric name=\"unix\" type=\"integer\">1278923579</metric>" +
            "          </created>" +
            "          <metric name=\"thread-count\" type=\"integer\">1</metric>" +
            "          <metric name=\"class\" type=\"string\">dmg.cells.network.LocationManagerConnector</metric>" +
            "          <metric name=\"type\" type=\"string\">System</metric>" +
            "          <version>" +
            "            <metric name=\"revision\" type=\"string\">CA-1.28</metric>" +
            "            <metric name=\"release\" type=\"string\">cells</metric>" +
            "          </version>" +
            "        </cell>" +
            "        <cell name=\"lm\">" +
            "          <metric name=\"event-queue-size\" type=\"integer\">0</metric>" +
            "          <created>" +
            "            <metric name=\"ISO-8601\" type=\"string\">2010-07-12T08:32Z</metric>" +
            "            <metric name=\"simple\" type=\"string\">Jul 12, 10:32:59 CEST</metric>" +
            "            <metric name=\"unix\" type=\"integer\">1278923579</metric>" +
            "          </created>" +
            "          <metric name=\"thread-count\" type=\"integer\">2</metric>" +
            "          <metric name=\"class\" type=\"string\">dmg.cells.services.LocationManager</metric>" +
            "          <metric name=\"type\" type=\"string\">Generic</metric>" +
            "          <version>" +
            "            <metric name=\"revision\" type=\"string\">CA-1.28</metric>" +
            "            <metric name=\"release\" type=\"string\">cells</metric>" +
            "          </version>" +
            "        </cell>" +
            "        <cell name=\"c-dCacheDomain-101-102\">" +
            "          <metric name=\"event-queue-size\" type=\"integer\">0</metric>" +
            "          <created>" +
            "            <metric name=\"ISO-8601\" type=\"string\">2010-07-12T08:33Z</metric>" +
            "            <metric name=\"simple\" type=\"string\">Jul 12, 10:33:00 CEST</metric>" +
            "            <metric name=\"unix\" type=\"integer\">1278923580</metric>" +
            "          </created>" +
            "          <metric name=\"thread-count\" type=\"integer\">2</metric>" +
            "          <metric name=\"class\" type=\"string\">dmg.cells.network.LocationMgrTunnel</metric>" +
            "          <metric name=\"type\" type=\"string\">System</metric>" +
            "          <version>" +
            "            <metric name=\"revision\" type=\"string\">CA-1.28</metric>" +
            "            <metric name=\"release\" type=\"string\">cells</metric>" +
            "          </version>" +
            "        </cell>" +
            "        <cell name=\"mySecondPool\">" +
            "          <metric name=\"event-queue-size\" type=\"integer\">0</metric>" +
            "          <created>" +
            "            <metric name=\"ISO-8601\" type=\"string\">2010-07-12T08:33Z</metric>" +
            "            <metric name=\"simple\" type=\"string\">Jul 12, 10:33:02 CEST</metric>" +
            "            <metric name=\"unix\" type=\"integer\">1278923582</metric>" +
            "          </created>" +
            "          <metric name=\"thread-count\" type=\"integer\">20</metric>" +
            "          <metric name=\"class\" type=\"string\">Pool</metric>" +
            "          <metric name=\"type\" type=\"string\">Generic</metric>" +
            "          <version>" +
            "            <metric name=\"revision\" type=\"string\">14038</metric>" +
            "            <metric name=\"release\" type=\"string\">production-1.9.10-1rc</metric>" +
            "          </version>" +
            "        </cell>" +
            "        <cell name=\"myFirstPool\">" +
            "          <metric name=\"event-queue-size\" type=\"integer\">0</metric>" +
            "          <created>" +
            "            <metric name=\"ISO-8601\" type=\"string\">2010-07-12T08:32Z</metric>" +
            "            <metric name=\"simple\" type=\"string\">Jul 12, 10:32:59 CEST</metric>" +
            "            <metric name=\"unix\" type=\"integer\">1278923579</metric>" +
            "          </created>" +
            "          <metric name=\"thread-count\" type=\"integer\">31</metric>" +
            "          <metric name=\"class\" type=\"string\">Pool</metric>" +
            "          <metric name=\"type\" type=\"string\">Generic</metric>" +
            "          <version>" +
            "            <metric name=\"revision\" type=\"string\">14038</metric>" +
            "            <metric name=\"release\" type=\"string\">production-1.9.10-1rc</metric>" +
            "          </version>" +
            "        </cell>" +
            "        <cell name=\"RoutingMgr\">" +
            "          <metric name=\"event-queue-size\" type=\"integer\">0</metric>" +
            "          <created>" +
            "            <metric name=\"ISO-8601\" type=\"string\">2010-07-12T08:32Z</metric>" +
            "            <metric name=\"simple\" type=\"string\">Jul 12, 10:32:59 CEST</metric>" +
            "            <metric name=\"unix\" type=\"integer\">1278923579</metric>" +
            "          </created>" +
            "          <metric name=\"thread-count\" type=\"integer\">1</metric>" +
            "          <metric name=\"class\" type=\"string\">dmg.cells.services.RoutingManager</metric>" +
            "          <metric name=\"type\" type=\"string\">System</metric>" +
            "          <version>" +
            "            <metric name=\"revision\" type=\"string\">CA-1.28</metric>" +
            "            <metric name=\"release\" type=\"string\">cells</metric>" +
            "          </version>" +
            "        </cell>" +
            "        <cell name=\"System\">" +
            "          <metric name=\"event-queue-size\" type=\"integer\">0</metric>" +
            "          <created>" +
            "            <metric name=\"ISO-8601\" type=\"string\">2010-07-12T08:32Z</metric>" +
            "            <metric name=\"simple\" type=\"string\">Jul 12, 10:32:59 CEST</metric>" +
            "            <metric name=\"unix\" type=\"integer\">1278923579</metric>" +
            "          </created>" +
            "          <metric name=\"thread-count\" type=\"integer\">1</metric>" +
            "          <metric name=\"class\" type=\"string\">dmg.cells.nucleus.SystemCell</metric>" +
            "          <metric name=\"type\" type=\"string\">System</metric>" +
            "          <version>" +
            "            <metric name=\"revision\" type=\"string\">CA-1.28</metric>" +
            "            <metric name=\"release\" type=\"string\">cells</metric>" +
            "          </version>" +
            "        </cell>" +
            "      </cells>" +
            "      <metric name=\"address\" type=\"string\">System@dCacheDomain:System@patrickxenvm02Domain</metric>" +
            "    </domain>" +
            "    <domain name=\"infoDomain\">" +
            "      <cells>" +
            "        <cell name=\"c-dCacheDomain-101\">" +
            "          <metric name=\"event-queue-size\" type=\"integer\">0</metric>" +
            "          <created>" +
            "            <metric name=\"ISO-8601\" type=\"string\">2010-07-12T08:33Z</metric>" +
            "            <metric name=\"simple\" type=\"string\">Jul 12, 10:33:04 CEST</metric>" +
            "            <metric name=\"unix\" type=\"integer\">1278923584</metric>" +
            "          </created>" +
            "          <metric name=\"thread-count\" type=\"integer\">1</metric>" +
            "          <metric name=\"class\" type=\"string\">dmg.cells.network.LocationManagerConnector</metric>" +
            "          <metric name=\"type\" type=\"string\">System</metric>" +
            "          <version>" +
            "            <metric name=\"revision\" type=\"string\">CA-1.28</metric>" +
            "            <metric name=\"release\" type=\"string\">cells</metric>" +
            "          </version>" +
            "        </cell>" +
            "        <cell name=\"lm\">" +
            "          <metric name=\"event-queue-size\" type=\"integer\">0</metric>" +
            "          <created>" +
            "            <metric name=\"ISO-8601\" type=\"string\">2010-07-12T08:33Z</metric>" +
            "            <metric name=\"simple\" type=\"string\">Jul 12, 10:33:04 CEST</metric>" +
            "            <metric name=\"unix\" type=\"integer\">1278923584</metric>" +
            "          </created>" +
            "          <metric name=\"thread-count\" type=\"integer\">2</metric>" +
            "          <metric name=\"class\" type=\"string\">dmg.cells.services.LocationManager</metric>" +
            "          <metric name=\"type\" type=\"string\">Generic</metric>" +
            "          <version>" +
            "            <metric name=\"revision\" type=\"string\">CA-1.28</metric>" +
            "            <metric name=\"release\" type=\"string\">cells</metric>" +
            "          </version>" +
            "        </cell>" +
            "        <cell name=\"c-dCacheDomain-101-102\">" +
            "          <metric name=\"event-queue-size\" type=\"integer\">0</metric>" +
            "          <created>" +
            "            <metric name=\"ISO-8601\" type=\"string\">2010-07-12T08:33Z</metric>" +
            "            <metric name=\"simple\" type=\"string\">Jul 12, 10:33:04 CEST</metric>" +
            "            <metric name=\"unix\" type=\"integer\">1278923584</metric>" +
            "          </created>" +
            "          <metric name=\"thread-count\" type=\"integer\">2</metric>" +
            "          <metric name=\"class\" type=\"string\">dmg.cells.network.LocationMgrTunnel</metric>" +
            "          <metric name=\"type\" type=\"string\">System</metric>" +
            "          <version>" +
            "            <metric name=\"revision\" type=\"string\">CA-1.28</metric>" +
            "            <metric name=\"release\" type=\"string\">cells</metric>" +
            "          </version>" +
            "        </cell>" +
            "        <cell name=\"RoutingMgr\">" +
            "          <metric name=\"event-queue-size\" type=\"integer\">0</metric>" +
            "          <created>" +
            "            <metric name=\"ISO-8601\" type=\"string\">2010-07-12T08:33Z</metric>" +
            "            <metric name=\"simple\" type=\"string\">Jul 12, 10:33:04 CEST</metric>" +
            "            <metric name=\"unix\" type=\"integer\">1278923584</metric>" +
            "          </created>" +
            "          <metric name=\"thread-count\" type=\"integer\">1</metric>" +
            "          <metric name=\"class\" type=\"string\">dmg.cells.services.RoutingManager</metric>" +
            "          <metric name=\"type\" type=\"string\">System</metric>" +
            "          <version>" +
            "            <metric name=\"revision\" type=\"string\">CA-1.28</metric>" +
            "            <metric name=\"release\" type=\"string\">cells</metric>" +
            "          </version>" +
            "        </cell>" +
            "        <cell name=\"System\">" +
            "          <metric name=\"event-queue-size\" type=\"integer\">0</metric>" +
            "          <created>" +
            "            <metric name=\"ISO-8601\" type=\"string\">2010-07-12T08:33Z</metric>" +
            "            <metric name=\"simple\" type=\"string\">Jul 12, 10:33:04 CEST</metric>" +
            "            <metric name=\"unix\" type=\"integer\">1278923584</metric>" +
            "          </created>" +
            "          <metric name=\"thread-count\" type=\"integer\">1</metric>" +
            "          <metric name=\"class\" type=\"string\">dmg.cells.nucleus.SystemCell</metric>" +
            "          <metric name=\"type\" type=\"string\">System</metric>" +
            "          <version>" +
            "            <metric name=\"revision\" type=\"string\">CA-1.28</metric>" +
            "            <metric name=\"release\" type=\"string\">cells</metric>" +
            "          </version>" +
            "        </cell>" +
            "        <cell name=\"info\">" +
            "          <metric name=\"event-queue-size\" type=\"integer\">0</metric>" +
            "          <created>" +
            "            <metric name=\"ISO-8601\" type=\"string\">2010-07-12T08:33Z</metric>" +
            "            <metric name=\"simple\" type=\"string\">Jul 12, 10:33:04 CEST</metric>" +
            "            <metric name=\"unix\" type=\"integer\">1278923584</metric>" +
            "          </created>" +
            "          <metric name=\"thread-count\" type=\"integer\">3</metric>" +
            "          <metric name=\"class\" type=\"string\">org.dcache.services.info.InfoProvider</metric>" +
            "          <metric name=\"type\" type=\"string\">Generic</metric>" +
            "          <version>" +
            "            <metric name=\"revision\" type=\"string\">9086</metric>" +
            "            <metric name=\"release\" type=\"string\">production-1.9.10-1rc</metric>" +
            "          </version>" +
            "        </cell>" +
            "      </cells>" +
            "      <metric name=\"address\" type=\"string\">System@dCacheDomain:System@infoDomain</metric>" +
            "    </domain>" +
            "    <domain name=\"webadminDomain\">" +
            "      <cells>" +
            "        <cell name=\"c-dCacheDomain-101\">" +
            "          <metric name=\"event-queue-size\" type=\"integer\">0</metric>" +
            "          <created>" +
            "            <metric name=\"ISO-8601\" type=\"string\">2010-07-16T08:25Z</metric>" +
            "            <metric name=\"simple\" type=\"string\">Jul 16, 10:25:56 CEST</metric>" +
            "            <metric name=\"unix\" type=\"integer\">1279268756</metric>" +
            "          </created>" +
            "          <metric name=\"thread-count\" type=\"integer\">1</metric>" +
            "          <metric name=\"class\" type=\"string\">dmg.cells.network.LocationManagerConnector</metric>" +
            "          <metric name=\"type\" type=\"string\">System</metric>" +
            "          <version>" +
            "            <metric name=\"revision\" type=\"string\">CA-1.28</metric>" +
            "            <metric name=\"release\" type=\"string\">cells</metric>" +
            "          </version>" +
            "        </cell>" +
            "        <cell name=\"lm\">" +
            "          <metric name=\"event-queue-size\" type=\"integer\">0</metric>" +
            "          <created>" +
            "            <metric name=\"ISO-8601\" type=\"string\">2010-07-16T08:25Z</metric>" +
            "            <metric name=\"simple\" type=\"string\">Jul 16, 10:25:56 CEST</metric>" +
            "            <metric name=\"unix\" type=\"integer\">1279268756</metric>" +
            "          </created>" +
            "          <metric name=\"thread-count\" type=\"integer\">2</metric>" +
            "          <metric name=\"class\" type=\"string\">dmg.cells.services.LocationManager</metric>" +
            "          <metric name=\"type\" type=\"string\">Generic</metric>" +
            "          <version>" +
            "            <metric name=\"revision\" type=\"string\">CA-1.28</metric>" +
            "            <metric name=\"release\" type=\"string\">cells</metric>" +
            "          </version>" +
            "        </cell>" +
            "        <cell name=\"c-dCacheDomain-101-102\">" +
            "          <metric name=\"event-queue-size\" type=\"integer\">0</metric>" +
            "          <created>" +
            "            <metric name=\"ISO-8601\" type=\"string\">2010-07-16T08:25Z</metric>" +
            "            <metric name=\"simple\" type=\"string\">Jul 16, 10:25:56 CEST</metric>" +
            "            <metric name=\"unix\" type=\"integer\">1279268756</metric>" +
            "          </created>" +
            "          <metric name=\"thread-count\" type=\"integer\">2</metric>" +
            "          <metric name=\"class\" type=\"string\">dmg.cells.network.LocationMgrTunnel</metric>" +
            "          <metric name=\"type\" type=\"string\">System</metric>" +
            "          <version>" +
            "            <metric name=\"revision\" type=\"string\">CA-1.28</metric>" +
            "            <metric name=\"release\" type=\"string\">cells</metric>" +
            "          </version>" +
            "        </cell>" +
            "        <cell name=\"webadmin\">" +
            "          <metric name=\"event-queue-size\" type=\"integer\">0</metric>" +
            "          <created>" +
            "            <metric name=\"ISO-8601\" type=\"string\">2010-07-16T08:25Z</metric>" +
            "            <metric name=\"simple\" type=\"string\">Jul 16, 10:25:56 CEST</metric>" +
            "            <metric name=\"unix\" type=\"integer\">1279268756</metric>" +
            "          </created>" +
            "          <metric name=\"thread-count\" type=\"integer\">12</metric>" +
            "          <metric name=\"class\" type=\"string\">org.dcache.admin.webadmin.jettycell.JettyCell</metric>" +
            "          <metric name=\"type\" type=\"string\">Generic</metric>" +
            "          <version>" +
            "            <metric name=\"revision\" type=\"string\">14038</metric>" +
            "            <metric name=\"release\" type=\"string\">production-1.9.10-1rc</metric>" +
            "          </version>" +
            "        </cell>" +
            "        <cell name=\"RoutingMgr\">" +
            "          <metric name=\"event-queue-size\" type=\"integer\">0</metric>" +
            "          <created>" +
            "            <metric name=\"ISO-8601\" type=\"string\">2010-07-16T08:25Z</metric>" +
            "            <metric name=\"simple\" type=\"string\">Jul 16, 10:25:56 CEST</metric>" +
            "            <metric name=\"unix\" type=\"integer\">1279268756</metric>" +
            "          </created>" +
            "          <metric name=\"thread-count\" type=\"integer\">1</metric>" +
            "          <metric name=\"class\" type=\"string\">dmg.cells.services.RoutingManager</metric>" +
            "          <metric name=\"type\" type=\"string\">System</metric>" +
            "          <version>" +
            "            <metric name=\"revision\" type=\"string\">CA-1.28</metric>" +
            "            <metric name=\"release\" type=\"string\">cells</metric>" +
            "          </version>" +
            "        </cell>" +
            "        <cell name=\"System\">" +
            "          <metric name=\"event-queue-size\" type=\"integer\">0</metric>" +
            "          <created>" +
            "            <metric name=\"ISO-8601\" type=\"string\">2010-07-16T08:25Z</metric>" +
            "            <metric name=\"simple\" type=\"string\">Jul 16, 10:25:56 CEST</metric>" +
            "            <metric name=\"unix\" type=\"integer\">1279268756</metric>" +
            "          </created>" +
            "          <metric name=\"thread-count\" type=\"integer\">1</metric>" +
            "          <metric name=\"class\" type=\"string\">dmg.cells.nucleus.SystemCell</metric>" +
            "          <metric name=\"type\" type=\"string\">System</metric>" +
            "          <version>" +
            "            <metric name=\"revision\" type=\"string\">CA-1.28</metric>" +
            "            <metric name=\"release\" type=\"string\">cells</metric>" +
            "          </version>" +
            "        </cell>" +
            "      </cells>" +
            "      <metric name=\"address\" type=\"string\">System@dCacheDomain:System@webadminDomain</metric>" +
            "    </domain>" +
            "    <domain name=\"dcap-patrickxenvm02Domain\">" +
            "      <cells>" +
            "        <cell name=\"c-dCacheDomain-101\">" +
            "          <metric name=\"event-queue-size\" type=\"integer\">0</metric>" +
            "          <created>" +
            "            <metric name=\"ISO-8601\" type=\"string\">2010-07-12T08:33Z</metric>" +
            "            <metric name=\"simple\" type=\"string\">Jul 12, 10:33:02 CEST</metric>" +
            "            <metric name=\"unix\" type=\"integer\">1278923582</metric>" +
            "          </created>" +
            "          <metric name=\"thread-count\" type=\"integer\">1</metric>" +
            "          <metric name=\"class\" type=\"string\">dmg.cells.network.LocationManagerConnector</metric>" +
            "          <metric name=\"type\" type=\"string\">System</metric>" +
            "          <version>" +
            "            <metric name=\"revision\" type=\"string\">CA-1.28</metric>" +
            "            <metric name=\"release\" type=\"string\">cells</metric>" +
            "          </version>" +
            "        </cell>" +
            "        <cell name=\"DCap-patrickxenvm02\">" +
            "          <metric name=\"event-queue-size\" type=\"integer\">0</metric>" +
            "          <created>" +
            "            <metric name=\"ISO-8601\" type=\"string\">2010-07-12T08:33Z</metric>" +
            "            <metric name=\"simple\" type=\"string\">Jul 12, 10:33:01 CEST</metric>" +
            "            <metric name=\"unix\" type=\"integer\">1278923581</metric>" +
            "          </created>" +
            "          <metric name=\"thread-count\" type=\"integer\">4</metric>" +
            "          <metric name=\"class\" type=\"string\">dmg.cells.services.login.LoginManager</metric>" +
            "          <metric name=\"type\" type=\"string\">Generic</metric>" +
            "          <version>" +
            "            <metric name=\"revision\" type=\"string\">1.17</metric>" +
            "            <metric name=\"release\" type=\"string\">production-1.9.10-1rc</metric>" +
            "          </version>" +
            "        </cell>" +
            "        <cell name=\"lm\">" +
            "          <metric name=\"event-queue-size\" type=\"integer\">0</metric>" +
            "          <created>" +
            "            <metric name=\"ISO-8601\" type=\"string\">2010-07-12T08:33Z</metric>" +
            "            <metric name=\"simple\" type=\"string\">Jul 12, 10:33:01 CEST</metric>" +
            "            <metric name=\"unix\" type=\"integer\">1278923581</metric>" +
            "          </created>" +
            "          <metric name=\"thread-count\" type=\"integer\">2</metric>" +
            "          <metric name=\"class\" type=\"string\">dmg.cells.services.LocationManager</metric>" +
            "          <metric name=\"type\" type=\"string\">Generic</metric>" +
            "          <version>" +
            "            <metric name=\"revision\" type=\"string\">CA-1.28</metric>" +
            "            <metric name=\"release\" type=\"string\">cells</metric>" +
            "          </version>" +
            "        </cell>" +
            "        <cell name=\"c-dCacheDomain-101-102\">" +
            "          <metric name=\"event-queue-size\" type=\"integer\">0</metric>" +
            "          <created>" +
            "            <metric name=\"ISO-8601\" type=\"string\">2010-07-12T08:33Z</metric>" +
            "            <metric name=\"simple\" type=\"string\">Jul 12, 10:33:02 CEST</metric>" +
            "            <metric name=\"unix\" type=\"integer\">1278923582</metric>" +
            "          </created>" +
            "          <metric name=\"thread-count\" type=\"integer\">2</metric>" +
            "          <metric name=\"class\" type=\"string\">dmg.cells.network.LocationMgrTunnel</metric>" +
            "          <metric name=\"type\" type=\"string\">System</metric>" +
            "          <version>" +
            "            <metric name=\"revision\" type=\"string\">CA-1.28</metric>" +
            "            <metric name=\"release\" type=\"string\">cells</metric>" +
            "          </version>" +
            "        </cell>" +
            "        <cell name=\"RoutingMgr\">" +
            "          <metric name=\"event-queue-size\" type=\"integer\">0</metric>" +
            "          <created>" +
            "            <metric name=\"ISO-8601\" type=\"string\">2010-07-12T08:33Z</metric>" +
            "            <metric name=\"simple\" type=\"string\">Jul 12, 10:33:01 CEST</metric>" +
            "            <metric name=\"unix\" type=\"integer\">1278923581</metric>" +
            "          </created>" +
            "          <metric name=\"thread-count\" type=\"integer\">1</metric>" +
            "          <metric name=\"class\" type=\"string\">dmg.cells.services.RoutingManager</metric>" +
            "          <metric name=\"type\" type=\"string\">System</metric>" +
            "          <version>" +
            "            <metric name=\"revision\" type=\"string\">CA-1.28</metric>" +
            "            <metric name=\"release\" type=\"string\">cells</metric>" +
            "          </version>" +
            "        </cell>" +
            "        <cell name=\"System\">" +
            "          <metric name=\"event-queue-size\" type=\"integer\">0</metric>" +
            "          <created>" +
            "            <metric name=\"ISO-8601\" type=\"string\">2010-07-12T08:33Z</metric>" +
            "            <metric name=\"simple\" type=\"string\">Jul 12, 10:33:01 CEST</metric>" +
            "            <metric name=\"unix\" type=\"integer\">1278923581</metric>" +
            "          </created>" +
            "          <metric name=\"thread-count\" type=\"integer\">1</metric>" +
            "          <metric name=\"class\" type=\"string\">dmg.cells.nucleus.SystemCell</metric>" +
            "          <metric name=\"type\" type=\"string\">System</metric>" +
            "          <version>" +
            "            <metric name=\"revision\" type=\"string\">CA-1.28</metric>" +
            "            <metric name=\"release\" type=\"string\">cells</metric>" +
            "          </version>" +
            "        </cell>" +
            "      </cells>" +
            "      <metric name=\"address\" type=\"string\">System@dCacheDomain:System@dcap-patrickxenvm02Domain</metric>" +
            "    </domain>" +
            "    <domain name=\"gridftp-patrickxenvm02Domain\">" +
            "      <cells>" +
            "        <cell name=\"c-dCacheDomain-101\">" +
            "          <metric name=\"event-queue-size\" type=\"integer\">0</metric>" +
            "          <created>" +
            "            <metric name=\"ISO-8601\" type=\"string\">2010-07-12T08:33Z</metric>" +
            "            <metric name=\"simple\" type=\"string\">Jul 12, 10:33:01 CEST</metric>" +
            "            <metric name=\"unix\" type=\"integer\">1278923581</metric>" +
            "          </created>" +
            "          <metric name=\"thread-count\" type=\"integer\">1</metric>" +
            "          <metric name=\"class\" type=\"string\">dmg.cells.network.LocationManagerConnector</metric>" +
            "          <metric name=\"type\" type=\"string\">System</metric>" +
            "          <version>" +
            "            <metric name=\"revision\" type=\"string\">CA-1.28</metric>" +
            "            <metric name=\"release\" type=\"string\">cells</metric>" +
            "          </version>" +
            "        </cell>" +
            "        <cell name=\"GFTP-patrickxenvm02\">" +
            "          <metric name=\"event-queue-size\" type=\"integer\">0</metric>" +
            "          <created>" +
            "            <metric name=\"ISO-8601\" type=\"string\">2010-07-12T08:33Z</metric>" +
            "            <metric name=\"simple\" type=\"string\">Jul 12, 10:33:01 CEST</metric>" +
            "            <metric name=\"unix\" type=\"integer\">1278923581</metric>" +
            "          </created>" +
            "          <metric name=\"thread-count\" type=\"integer\">4</metric>" +
            "          <metric name=\"class\" type=\"string\">dmg.cells.services.login.LoginManager</metric>" +
            "          <metric name=\"type\" type=\"string\">Generic</metric>" +
            "          <version>" +
            "            <metric name=\"revision\" type=\"string\">1.17</metric>" +
            "            <metric name=\"release\" type=\"string\">production-1.9.10-1rc</metric>" +
            "          </version>" +
            "        </cell>" +
            "        <cell name=\"lm\">" +
            "          <metric name=\"event-queue-size\" type=\"integer\">0</metric>" +
            "          <created>" +
            "            <metric name=\"ISO-8601\" type=\"string\">2010-07-12T08:33Z</metric>" +
            "            <metric name=\"simple\" type=\"string\">Jul 12, 10:33:01 CEST</metric>" +
            "            <metric name=\"unix\" type=\"integer\">1278923581</metric>" +
            "          </created>" +
            "          <metric name=\"thread-count\" type=\"integer\">2</metric>" +
            "          <metric name=\"class\" type=\"string\">dmg.cells.services.LocationManager</metric>" +
            "          <metric name=\"type\" type=\"string\">Generic</metric>" +
            "          <version>" +
            "            <metric name=\"revision\" type=\"string\">CA-1.28</metric>" +
            "            <metric name=\"release\" type=\"string\">cells</metric>" +
            "          </version>" +
            "        </cell>" +
            "        <cell name=\"c-dCacheDomain-101-102\">" +
            "          <metric name=\"event-queue-size\" type=\"integer\">0</metric>" +
            "          <created>" +
            "            <metric name=\"ISO-8601\" type=\"string\">2010-07-12T08:33Z</metric>" +
            "            <metric name=\"simple\" type=\"string\">Jul 12, 10:33:02 CEST</metric>" +
            "            <metric name=\"unix\" type=\"integer\">1278923582</metric>" +
            "          </created>" +
            "          <metric name=\"thread-count\" type=\"integer\">2</metric>" +
            "          <metric name=\"class\" type=\"string\">dmg.cells.network.LocationMgrTunnel</metric>" +
            "          <metric name=\"type\" type=\"string\">System</metric>" +
            "          <version>" +
            "            <metric name=\"revision\" type=\"string\">CA-1.28</metric>" +
            "            <metric name=\"release\" type=\"string\">cells</metric>" +
            "          </version>" +
            "        </cell>" +
            "        <cell name=\"RoutingMgr\">" +
            "          <metric name=\"event-queue-size\" type=\"integer\">0</metric>" +
            "          <created>" +
            "            <metric name=\"ISO-8601\" type=\"string\">2010-07-12T08:33Z</metric>" +
            "            <metric name=\"simple\" type=\"string\">Jul 12, 10:33:01 CEST</metric>" +
            "            <metric name=\"unix\" type=\"integer\">1278923581</metric>" +
            "          </created>" +
            "          <metric name=\"thread-count\" type=\"integer\">1</metric>" +
            "          <metric name=\"class\" type=\"string\">dmg.cells.services.RoutingManager</metric>" +
            "          <metric name=\"type\" type=\"string\">System</metric>" +
            "          <version>" +
            "            <metric name=\"revision\" type=\"string\">CA-1.28</metric>" +
            "            <metric name=\"release\" type=\"string\">cells</metric>" +
            "          </version>" +
            "        </cell>" +
            "        <cell name=\"System\">" +
            "          <metric name=\"event-queue-size\" type=\"integer\">0</metric>" +
            "          <created>" +
            "            <metric name=\"ISO-8601\" type=\"string\">2010-07-12T08:33Z</metric>" +
            "            <metric name=\"simple\" type=\"string\">Jul 12, 10:33:01 CEST</metric>" +
            "            <metric name=\"unix\" type=\"integer\">1278923581</metric>" +
            "          </created>" +
            "          <metric name=\"thread-count\" type=\"integer\">1</metric>" +
            "          <metric name=\"class\" type=\"string\">dmg.cells.nucleus.SystemCell</metric>" +
            "          <metric name=\"type\" type=\"string\">System</metric>" +
            "          <version>" +
            "            <metric name=\"revision\" type=\"string\">CA-1.28</metric>" +
            "            <metric name=\"release\" type=\"string\">cells</metric>" +
            "          </version>" +
            "        </cell>" +
            "      </cells>" +
            "      <metric name=\"address\" type=\"string\">System@dCacheDomain:System@gridftp-patrickxenvm02Domain</metric>" +
            "    </domain>" +
            "  </domains>" +
            "</dCache>";

    public static Set<Pool> getExpectedPools() {
        Set<Pool> pools = new HashSet<Pool>(2);

        pools.add(createTestPool());

        Pool pool2 = new Pool();
        pool2.setEnabled(IS_POOL2_ENABLED);
        pool2.setFreeSpace(POOL2_FREE_SPACE);
        pool2.setName(POOL2_NAME);
        pool2.setPreciousSpace(POOL2_PRECIOUS_SPACE);
        pool2.setTotalSpace(POOL2_TOTAL_SPACE);
        pool2.setUsedSpace(POOL2_USED_SPACE);

        pools.add(pool2);

        return pools;
    }

    public static Pool getTestPool() {
        return createTestPool();
    }

    private static Pool createTestPool() {
        Pool pool1 = new Pool();
        pool1.setEnabled(IS_POOL1_ENABLED);
        pool1.setFreeSpace(POOL1_FREE_SPACE);
        pool1.setName(POOL1_NAME);
        pool1.setPreciousSpace(POOL1_PRECIOUS_SPACE);
        pool1.setTotalSpace(POOL1_TOTAL_SPACE);
        pool1.setUsedSpace(POOL1_USED_SPACE);
        pool1.setRemovableSpace(POOL1_REMOVABLE_SPACE);
        pool1.addMoverQueue(POOL1_STORES);
        pool1.addMoverQueue(POOL1_MOVERS);
        pool1.addMoverQueue(POOL1_RESTORES);
        pool1.addMoverQueue(POOL1_P2PCLIENT);
        pool1.addMoverQueue(POOL1_P2PSERVER);
        pool1.setPoolGroups(POOL1_POOLGROUPS);
        return pool1;
    }

    public static List<PoolSpaceBean> createExpectedPoolBeans() {
        List<PoolSpaceBean> poolBeans = new ArrayList(2);
        PoolSpaceBean poolBean1 = new PoolSpaceBean();
        poolBean1.setDomainName(POOL1_DOMAIN);
        poolBean1.setEnabled(IS_POOL1_ENABLED);
        poolBean1.setFreeSpace(POOL1_FREE_SPACE);
        poolBean1.setName(POOL1_NAME);
        poolBean1.setPreciousSpace(POOL1_PRECIOUS_SPACE);
        poolBean1.setTotalSpace(POOL1_TOTAL_SPACE);
        poolBean1.setUsedSpace(POOL1_USED_SPACE);
        poolBeans.add(poolBean1);

        PoolSpaceBean poolBean2 = new PoolSpaceBean();

        poolBean2.setDomainName(POOL2_DOMAIN);
        poolBean2.setEnabled(IS_POOL2_ENABLED);
        poolBean2.setFreeSpace(POOL2_FREE_SPACE);
        poolBean2.setName(POOL2_NAME);
        poolBean2.setPreciousSpace(POOL2_PRECIOUS_SPACE);
        poolBean2.setTotalSpace(POOL2_TOTAL_SPACE);
        poolBean2.setUsedSpace(POOL2_USED_SPACE);
        poolBeans.add(poolBean2);

        Collections.sort(poolBeans);

        return poolBeans;
    }

    public static Set<String> getExpectedDoorNames() {
        Set<String> expectedDoors = new HashSet<String>();
        expectedDoors.add(DOOR1_NAME);
        expectedDoors.add(DOOR2_NAME);
        expectedDoors.add(DOOR3_NAME);
        return expectedDoors;
    }

    public static Set<String> getAvailableIds() {
        Set<String> ids = new HashSet<String>();
        ids.add(POOL1_NAME);
        ids.add(POOL2_NAME);
        return ids;
    }
}
