package org.dcache.webadmin.model.dataaccess.impl;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.pools.PoolV2Mode;

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
    private static final MoverQueue POOL1_MOVERS = new MoverQueue(0, 100, 0);
    private static final MoverQueue POOL1_RESTORES = new MoverQueue(0, 100, 0);
    private static final MoverQueue POOL1_STORES = new MoverQueue(0, 100, 0);
    private static final MoverQueue POOL1_P2PSERVER = new MoverQueue(0, 100, 0);
    private static final MoverQueue POOL1_P2PCLIENT = new MoverQueue(10, 10, 10);
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
    public static final String LINKGROUP1_ID = "0";
    public static final String LINKGROUP1_NAME = "dteam-linkGroup-Disk";
    public static final String LINKGROUP1_TOTAL_SPACE = "8085024768";
    public static final String LINKGROUP1_RESERVATION1_ID = "21";
    public static final String RESERVATION1_TOTAL = "123456";
    public static final String RESERVATION1_FQAN = "/dteam";
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
    public final static String LINKGROUPS_XML = "<?xml version=\"1.0\"?>" +
            "<dCache xmlns=\"http://www.dcache.org/2008/01/Info\">" +
            "  <linkgroups>" +
            "    <linkgroup lgid=\"" + LINKGROUP1_ID + "\">" +
            "      <metric name=\"id\" type=\"string\">" + LINKGROUP1_ID + "</metric>" +
            "      <access-latency>" +
            "        <metric name=\"onlineAllowed\" type=\"boolean\">true</metric>" +
            "        <metric name=\"nearlineAllowed\" type=\"boolean\">true</metric>" +
            "      </access-latency>" +
            "      <metric name=\"name\" type=\"string\">" + LINKGROUP1_NAME + "</metric>" +
            "      <lastUpdated>" +
            "        <metric name=\"ISO-8601\" type=\"string\">2010-10-15T09:13Z</metric>" +
            "        <metric name=\"simple\" type=\"string\">Oct 15, 11:13:39 CEST</metric>" +
            "        <metric name=\"unix\" type=\"integer\">1287134019</metric>" +
            "      </lastUpdated>" +
            "      <retention-policy>" +
            "        <metric name=\"custodialAllowed\" type=\"boolean\">true</metric>" +
            "        <metric name=\"outputAllowed\" type=\"boolean\">true</metric>" +
            "        <metric name=\"replicaAllowed\" type=\"boolean\">true</metric>" +
            "      </retention-policy>" +
            "      <space>" +
            "        <metric name=\"total\" type=\"integer\">" + LINKGROUP1_TOTAL_SPACE + "</metric>" +
            "        <metric name=\"free\" type=\"integer\">3221186835</metric>" +
            "        <metric name=\"available\" type=\"integer\">3221186835</metric>" +
            "        <metric name=\"used\" type=\"integer\">0</metric>" +
            "        <metric name=\"reserved\" type=\"integer\">0</metric>" +
            "      </space>" +
            "      <authorisation>" +
            "        <authorised name=\"/desy:*\">" +
            "          <metric name=\"FQAN\" type=\"string\">/desy</metric>" +
            "          <metric name=\"role\" type=\"string\">*</metric>" +
            "          <metric name=\"group\" type=\"string\">/desy</metric>" +
            "        </authorised>" +
            "        <authorised name=\"/dteam:*\">" +
            "          <metric name=\"FQAN\" type=\"string\">/dteam</metric>" +
            "          <metric name=\"role\" type=\"string\">*</metric>" +
            "          <metric name=\"group\" type=\"string\">/dteam</metric>" +
            "        </authorised>" +
            "        <authorised name=\"dteam001:*\">" +
            "          <metric name=\"FQAN\" type=\"string\">dteam001</metric>" +
            "          <metric name=\"role\" type=\"string\">*</metric>" +
            "          <metric name=\"group\" type=\"string\">dteam001</metric>" +
            "        </authorised>" +
            "        <authorised name=\"cms001:*\">" +
            "          <metric name=\"FQAN\" type=\"string\">cms001</metric>" +
            "          <metric name=\"role\" type=\"string\">*</metric>" +
            "          <metric name=\"group\" type=\"string\">cms001</metric>" +
            "        </authorised>" +
            "      </authorisation>" +
            "      <reservations>" +
            "      <reservationref reservation-id=\"" + LINKGROUP1_RESERVATION1_ID + "\"/>" +
            "      </reservations>" +
            "    </linkgroup>" +
            "  </linkgroups>" +
            "</dCache>";
    public static final String RESERVATIONS_XML = "<?xml version=\"1.0\"?>" +
            "<dCache xmlns=\"http://www.dcache.org/2008/01/Info\">" +
            "  <reservations>" +
            "    <reservation reservation-id=\"" + LINKGROUP1_RESERVATION1_ID + "\">" +
            "      <metric name=\"id\" type=\"string\">" + LINKGROUP1_RESERVATION1_ID + "</metric>" +
            "      <metric name=\"access-latency\" type=\"string\">ONLINE</metric>" +
            "      <created>" +
            "        <metric name=\"ISO-8601\" type=\"string\">2009-11-30T15:08Z</metric>" +
            "        <metric name=\"simple\" type=\"string\">Nov 30, 16:08:00 CET</metric>" +
            "        <metric name=\"unix\" type=\"integer\">1259593680</metric>" +
            "      </created>" +
            "      <metric name=\"description\" type=\"string\">Simple</metric>" +
            "      <metric name=\"state\" type=\"string\">RESERVED</metric>" +
            "      <metric name=\"retention-policy\" type=\"string\">REPLICA</metric>" +
            "      <space>" +
            "        <metric name=\"total\" type=\"integer\">" + RESERVATION1_TOTAL + "</metric>" +
            "        <metric name=\"free\" type=\"integer\">1024000</metric>" +
            "        <metric name=\"allocated\" type=\"integer\">0</metric>" +
            "        <metric name=\"used\" type=\"integer\">0</metric>" +
            "      </space>" +
            "      <authorisation>" +
            "        <metric name=\"FQAN\" type=\"string\">" + RESERVATION1_FQAN + "</metric>" +
            "        <metric name=\"group\" type=\"string\">/dteam</metric>" +
            "      </authorisation>" +
            "      <metric name=\"linkgroupref\" type=\"string\">" + LINKGROUP1_ID + "</metric>" +
            "    </reservation>" +
            "  </reservations>" +
            "</dCache>";

    public static Map<String, List<String>> getDomainsMap() {
        Map<String, List<String>> domainsMap = Maps.newHashMap();
        List<String> cellsOfPool1Domain = Lists.newArrayList();
        cellsOfPool1Domain.add(POOL1_NAME);
        domainsMap.put(POOL1_DOMAIN, cellsOfPool1Domain);
        List<String> cellsOfPool2Domain = Lists.newArrayList();
        cellsOfPool2Domain.add(POOL2_NAME);
        domainsMap.put(POOL2_DOMAIN, cellsOfPool2Domain);
        return domainsMap;
    }

    public static Set<Pool> getExpectedPools() {
        Set<Pool> pools = new HashSet<>(2);

        pools.add(createTestPool());
        SelectionPoolHelper sPool = new SelectionPoolHelper();
        sPool.setEnabled(IS_POOL2_ENABLED);
        sPool.setPoolMode(new PoolV2Mode(PoolV2Mode.DISABLED_STRICT));
        PoolCostInfo info = new PoolCostInfo(POOL2_NAME);
        info.setSpaceUsage(POOL2_TOTAL_SPACE, POOL2_FREE_SPACE, POOL2_PRECIOUS_SPACE,
                0, POOL2_USED_SPACE);
        Pool pool2 = new Pool(info, sPool);
        pools.add(pool2);

        return pools;
    }

    public static Pool getTestPool() {
        return createTestPool();
    }

    private static Pool createTestPool() {

        SelectionPoolHelper sPool = new SelectionPoolHelper();
        sPool.setEnabled(IS_POOL1_ENABLED);
        PoolCostInfo info = new PoolCostInfo(POOL1_NAME);
        info.setSpaceUsage(POOL1_TOTAL_SPACE, POOL1_FREE_SPACE, POOL1_PRECIOUS_SPACE,
                POOL1_REMOVABLE_SPACE, POOL1_USED_SPACE);
        info.setP2pClientQueueSizes(POOL1_P2PCLIENT._active,
                POOL1_P2PCLIENT._max, POOL1_P2PCLIENT._queued);
        info.setP2pServerQueueSizes(POOL1_P2PSERVER._active,
                POOL1_P2PSERVER._max, POOL1_P2PSERVER._queued);
        info.setQueueSizes(POOL1_MOVERS._active,
                POOL1_MOVERS._max, POOL1_MOVERS._queued,
                POOL1_RESTORES._active,
                POOL1_RESTORES._max, POOL1_RESTORES._queued,
                POOL1_STORES._active,
                POOL1_STORES._max, POOL1_STORES._queued);
        Pool pool1 = new Pool(info, sPool);
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
        Set<String> expectedDoors = new HashSet<>();
        expectedDoors.add(DOOR1_NAME);
        expectedDoors.add(DOOR2_NAME);
        expectedDoors.add(DOOR3_NAME);
        return expectedDoors;
    }

    public static Set<String> getAvailableIds() {
        Set<String> ids = new HashSet<>();
        ids.add(POOL1_NAME);
        ids.add(POOL2_NAME);
        return ids;
    }

    private static class MoverQueue {

        private int _active;
        private int _max;
        private int _queued;

        public MoverQueue(int active, int max, int queued) {

            _active = active;
            _max = max;
            _queued = queued;
        }

        public int getActive() {
            return _active;
        }

        public void setActive(int active) {
            _active = active;
        }

        public int getMax() {
            return _max;
        }

        public void setMax(int max) {
            _max = max;
        }

        public int getQueued() {
            return _queued;
        }

        public void setQueued(int queued) {
            _queued = queued;
        }
    }
}
