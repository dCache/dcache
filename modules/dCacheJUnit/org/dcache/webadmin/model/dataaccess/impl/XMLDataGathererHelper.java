package org.dcache.webadmin.model.dataaccess.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.List;
import org.dcache.webadmin.model.businessobjects.MoverQueue;
import org.dcache.webadmin.model.businessobjects.NamedCell;
import org.dcache.webadmin.model.businessobjects.Pool;
import org.dcache.webadmin.view.beans.PoolSpaceBean;

/**
 * XML-Data Source for Unit Tests with standardized XML-Output.
 * @author jans
 */
public class XMLDataGathererHelper {

    public static final String POOL1_NAME = "myFirstPool";
    public static final String POOL1_DOMAIN = "myFirstPoolDomain";
    public static final boolean IS_POOL1_ENABLED = true;
    public static final long POOL1_FREE_SPACE = 2147467538;
    public static final long POOL1_PRECIOUS_SPACE = 16110;
    public static final long POOL1_TOTAL_SPACE = 2147483648L;
    public static final long POOL1_USED_SPACE = 16110;
    public static final MoverQueue POOL1_MOVERS = new MoverQueue("mover", 0, 100, 0);
    public static final MoverQueue POOL1_RESTORES = new MoverQueue("restore", 0, 100, 0);
    public static final MoverQueue POOL1_STORES = new MoverQueue("store", 0, 100, 0);
    public static final MoverQueue POOL1_P2PSERVER = new MoverQueue("p2p-queue", 0, 100, 0);
    public static final MoverQueue POOL1_P2PCLIENT = new MoverQueue("p2p-clientqueue", 10, 10, 10);
    public static final String POOL2_NAME = "mySecondPool";
    public static final String POOL2_DOMAIN = "mySecondPoolDomain";
    public static final boolean IS_POOL2_ENABLED = false;
    public static final long POOL2_FREE_SPACE = 0;
    public static final long POOL2_PRECIOUS_SPACE = 0;
    public static final long POOL2_TOTAL_SPACE = 0;
    public static final long POOL2_USED_SPACE = 0;
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
            "        <metric name=\"removable\" type=\"integer\">0</metric>" +
            "        <metric name=\"used\" type=\"integer\">" + POOL1_USED_SPACE + "</metric>" +
            "        <metric name=\"gap\" type=\"integer\">1048576</metric>" +
            "      </space>      <poolgroups>        <poolgroupref name=\"default\"/>" +
            "      </poolgroups>    </pool>  </pools></dCache>";
    public static final String namedCellXmlcontent = "<?xml version=\"1.0\"?>" +
            "<dCache xmlns=\"http://www.dcache.org/2008/01/Info\"> " +
            " <domains> " +
            "  <domain name=\"dCacheDomain\"> " +
            "   <routing> " +
            "    <named-cells>" +
            "          <cell name=\"PinManager\">" +
            "<domainref name=\"utilityDomain\"/>" +
            "         </cell>" +
            "        <cell name=\"" + TESTCELL_NAME + "\">" +
            "         <domainref name=\"" + TESTCELL_DOMAIN + "\"/>" +
            "         </cell>" +
            "        <cell name=\"GFTP-srm-devel\">" +
            "          <domainref name=\"gridftp-srm-develDomain\"/>" +
            "       </cell>" +
            "      <cell name=\"info\">" +
            "            <domainref name=\"infoDomain\"/>" +
            "         </cell>" +
            "        <cell name=\"DCap-srm-devel\">" +
            "         <domainref name=\"dcap-srm-develDomain\"/>" +
            "      </cell>" +
            "     <cell name=\"PoolManager\">" +
            "      <domainref name=\"dCacheDomain\"/>" +
            "          </cell>" +
            "         <cell name=\"acladmin\">" +
            "          <domainref name=\"chimeraDomain\"/>" +
            "       </cell>" +
            "      <cell name=\"CopyManager\">" +
            "       <domainref name=\"srm-srm-develDomain\"/>" +
            "    </cell>" +
            "          <cell name=\"DCap-gsi-srm-devel\">" +
            "           <domainref name=\"gsidcap-srm-develDomain\"/>" +
            "        </cell>" +
            "       <cell name=\"PnfsManager\">" +
            "            <domainref name=\"chimeraDomain\"/>" +
            "         </cell>" +
            "        <cell name=\"srm-devel_1\">" +
            "         <domainref name=\"srm-develDomain\"/>" +
            "      </cell>" +
            "         <cell name=\"Xrootd-srm-devel\">" +
            "            <domainref name=\"xrootd-srm-develDomain\"/>" +
            "    </cell>" +
            "     <cell name=\"topo\">" +
            "        <domainref name=\"httpdDomain\"/>" +
            "       </cell>" +
            "        <cell name=\"SrmSpaceManager\">" +
            "           <domainref name=\"srm-srm-develDomain\"/>" +
            "          </cell>" +
            "  <cell name=\"RemoteGsiftpTransferManager\">" +
            "     <domainref name=\"srm-srm-develDomain\"/>" +
            "    </cell>" +
            "     <cell name=\"pam\">" +
            "        <domainref name=\"adminDoorDomain\"/>" +
            "       </cell>" +
            "        <cell name=\"billing\">" +
            "           <domainref name=\"httpdDomain\"/>" +
            "          </cell>" +
            "<cell name=\"gsi-pam\">" +
            " <domainref name=\"utilityDomain\"/>" +
            "</cell>" +
            "	  <cell name=\"srm-LoginBroker\">" +
            "	    <domainref name=\"httpdDomain\"/>" +
            "	  </cell>" +
            "	  <cell name=\"GFTP-srm-devel-Unknown-1345\">" +
            "	    <domainref name=\"gridftp-srm-develDomain\"/>" +
            "	  </cell>" +
            "	  <cell name=\"dirLookupPool\">" +
            "	    <domainref name=\"dirDomain\"/>" +
            "	  </cell>" +
            "<cell name=\"SRM-srm-devel\">" +
            "	    <domainref name=\"srm-srm-develDomain\"/>" +
            " </cell>" +
            "	  <cell name=\"RemoteHttpTransferManager\">" +
            "	   <domainref name=\"srm-srm-develDomain\"/>" +
            "	  </cell>" +
            "	  <cell name=\"cleaner\">" +
            "	    <domainref name=\"chimeraDomain\"/>" +
            "	  </cell>" +
            "	  <cell name=\"Prestager\">" +
            "	    <domainref name=\"dCacheDomain\"/>" +
            "	  </cell>" +
            "	  <cell name=\"srm-devel_5\"> " +
            "            <domainref name=\"srm-develDomain\"/>" +
            " </cell>" +
            " <cell name=\"srm-devel_4\">" +
            "    <domainref name=\"srm-develDomain\"/>" +
            "   </cell>" +
            "    <cell name=\"srm-devel_3\">" +
            "       <domainref name=\"srm-develDomain\"/>" +
            "      </cell>" +
            "       <cell name=\"gPlazma\">" +
            "          <domainref name=\"gPlazma-srm-develDomain\"/>" +
            "         </cell>" +
            "	 <cell name=\"LoginBroker\">" +
            " <domainref name=\"dCacheDomain\"/>" +
            "</cell>" +
            " <cell name=\"srm-devel_2\">" +
            "    <domainref name=\"srm-develDomain\"/>" +
            "  </cell>" +
            "   <cell name=\"TransferObserver\">" +
            "	   <domainref name=\"httpdDomain\"/>" +
            "	 </cell>" +
            "    </named-cells>" +
            "   </routing>" +
            "  </domain>" +
            " </domains>" +
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
        pool1.addMoverQueue(POOL1_STORES);
        pool1.addMoverQueue(POOL1_MOVERS);
        pool1.addMoverQueue(POOL1_RESTORES);
        pool1.addMoverQueue(POOL1_P2PCLIENT);
        pool1.addMoverQueue(POOL1_P2PSERVER);

        return pool1;
    }

    public static Set<NamedCell> getExpectedNamedCells() {
        Set<NamedCell> namedCells = new HashSet<NamedCell>(3);

        NamedCell testNamedCell = new NamedCell();
        testNamedCell = new NamedCell();
        testNamedCell.setCellName(TESTCELL_NAME);
        testNamedCell.setDomainName(TESTCELL_DOMAIN);

        namedCells.add(testNamedCell);

        NamedCell testNamedCell2 = new NamedCell();
        testNamedCell2.setCellName(TESTCELL2_NAME);
        testNamedCell2.setDomainName(TESTCELL2_DOMAIN);

        namedCells.add(testNamedCell2);

        NamedCell testNamedCell3 = new NamedCell();
        testNamedCell3.setCellName(EMPTY_TESTCELL_NAME);
        testNamedCell3.setDomainName(EMPTY_TESTCELL_DOMAIN);

        namedCells.add(testNamedCell3);
        return namedCells;
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

    public static Set<String> getAvailableIds() {
        Set<String> ids = new HashSet<String>();
        ids.add(POOL1_NAME);
        ids.add(POOL2_NAME);
        return ids;
    }
}
