package org.dcache.restful.srr;

import com.google.common.base.Strings;
import diskCacheV111.poolManager.CostModule;
import diskCacheV111.poolManager.PoolSelectionUnit;
import diskCacheV111.services.space.Space;
import diskCacheV111.services.space.message.GetSpaceTokensMessage;
import diskCacheV111.util.CacheException;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.cells.services.login.LoginBrokerInfo;
import dmg.cells.services.login.LoginBrokerSubscriber;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.dcache.cells.CellStub;
import org.dcache.poolmanager.PoolMonitor;
import org.dcache.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SrrBuilder {

    private final static Logger LOGGER = LoggerFactory.getLogger(SrrBuilder.class);

    private Map<String, List<String>> pgroup2vo;
    // info provider properties
    private String name;
    private String id;
    private String architecture;
    private String quality;
    private String doorTag;


    private CellStub spaceManager;

    private PoolMonitor remotePoolMonitor;

    private CellStub pnfsmanager;

    private LoginBrokerSubscriber loginBrokerSubscriber;

    private boolean spaceReservationEnabled;

    private SrrBuilder() {
    }

    public static SrrBuilder builder() {
        return new SrrBuilder();
    }

    public SrrBuilder withLoginBroker(LoginBrokerSubscriber loginBroker) {
        this.loginBrokerSubscriber = loginBroker;
        return this;
    }

    public SrrBuilder withSpaceManagerStub(CellStub stub) {
        this.spaceManager = stub;
        return this;
    }

    public SrrBuilder withNamespace(CellStub stub) {
        this.pnfsmanager = stub;
        return this;
    }

    public SrrBuilder withSpaceManagerEnaled(boolean isEnabled) {
        this.spaceReservationEnabled = isEnabled;
        return this;
    }

    public SrrBuilder withId(String id) {
        this.id = id;
        return this;
    }

    public SrrBuilder withPoolMonitor(PoolMonitor poolMonitor) {
        this.remotePoolMonitor = poolMonitor;
        return this;
    }

    public SrrBuilder withName(String name) {
        this.name = name;
        return this;
    }

    public SrrBuilder withQuality(String quality) {
        this.quality = quality;
        return this;
    }

    public SrrBuilder withArchitecture(String architecture) {
        this.architecture = architecture;
        return this;
    }

    public SrrBuilder withGroupVoMapping(Map<String, List<String>> pgroup2vo) {
        this.pgroup2vo = pgroup2vo;
        return this;
    }

    public SrrBuilder withDoorTag(String doorTag) {
        this.doorTag = doorTag;
        return this;
    }

    public SrrRecord generate()
          throws InterruptedException, CacheException, NoRouteToCellException {

        Online onlineCapacity = new Online()
              .withTotalsize(totalSpace())
              .withUsedsize(usedSpace());

        List<Storageendpoint> storageendpoints = collectEndpoint();
        List<Storageshare> storageshares = collectShares();

        if (spaceReservationEnabled) {
            List<Storageshare> spacetokens = collectSpaceTokens();
            storageshares.addAll(spacetokens);
            onlineCapacity.setReservedsize(collectSpaceReserved());
        }

        Storagecapacity storagecapacity = new Storagecapacity()
              .withOnline(onlineCapacity);

        Storageservice storageservice = new Storageservice()
              .withId(id)
              .withName(name)
              .withImplementation("dCache")
              .withServicetype(architecture)
              .withQualitylevel(Storageservice.Qualitylevel.fromValue(quality))
              .withImplementationversion(Version.of(this).getVersion())
              .withLatestupdate(Instant.now().getEpochSecond())
              .withStorageendpoints(storageendpoints)
              .withStorageshares(storageshares)
              .withStoragecapacity(storagecapacity);

        SrrRecord record = new SrrRecord();
        record.setStorageservice(storageservice);

        return record;
    }

    private long totalSpace() {
        return remotePoolMonitor.getCostModule().getPoolCostInfos().stream()
              .mapToLong(p -> p.getSpaceInfo().getTotalSpace())
              .sum();
    }

    private long usedSpace() {
        return remotePoolMonitor.getCostModule().getPoolCostInfos().stream()
              .mapToLong(p -> p.getSpaceInfo().getTotalSpace() - p.getSpaceInfo().getFreeSpace()
                    - p.getSpaceInfo().getRemovableSpace())
              .sum();
    }

    private List<Storageshare> collectSpaceTokens()
          throws CacheException, NoRouteToCellException, InterruptedException {

        long now = Instant.now().getEpochSecond();
        return spaceManager.sendAndWait(new GetSpaceTokensMessage()).getSpaceTokenSet().stream()
              .map(space -> {
                  Storageshare share = new Storageshare()
                        .withName(space.getDescription())
                        .withTotalsize(space.getSizeInBytes())
                        .withUsedsize(space.getUsedSizeInBytes())
                        .withTimestamp(now)
                        .withVos(Collections.singletonList(space.getVoGroup()))
                        .withAssignedendpoints(Collections.singletonList("all"))
                        .withAccesslatency(
                              Storageshare.Accesslatency.fromValue(space.getAccessLatency()))
                        .withRetentionpolicy(
                              Storageshare.Retentionpolicy.fromValue(space.getRetentionPolicy()));

                  return share;
              }).collect(Collectors.toList());
    }

    private long collectSpaceReserved()
          throws CacheException, NoRouteToCellException, InterruptedException {
        return spaceManager.sendAndWait(new GetSpaceTokensMessage()).getSpaceTokenSet().stream()
              .mapToLong(Space::getSizeInBytes)
              .sum();
    }

    private List<Storageshare> collectShares()
          throws CacheException, NoRouteToCellException, InterruptedException {

        Map<String, Storageshare> storageshares = new HashMap<>();
        long now = Instant.now().getEpochSecond();

        for (Map.Entry<String, List<String>> pgroup : pgroup2vo.entrySet()) {

            String shareName = pgroup.getKey();

            CostModule costModule = remotePoolMonitor.getCostModule();
            long totalSpace = 0;
            long usedSpace = 0;

            Collection<PoolSelectionUnit.SelectionPool> pools = remotePoolMonitor.getPoolSelectionUnit()
                  .getPoolsByPoolGroup(pgroup.getKey());

            totalSpace += pools.stream()
                  .map(PoolSelectionUnit.SelectionEntity::getName)
                  .map(costModule::getPoolCostInfo)
                  .mapToLong(p -> p.getSpaceInfo().getTotalSpace())
                  .sum();

            usedSpace += pools.stream()
                  .map(PoolSelectionUnit.SelectionEntity::getName)
                  .map(costModule::getPoolCostInfo)
                  .mapToLong(p -> p.getSpaceInfo().getTotalSpace() - p.getSpaceInfo().getFreeSpace()
                        - p.getSpaceInfo().getRemovableSpace())
                  .sum();

            Storageshare share = new Storageshare()
                  .withName(shareName)
                  .withTotalsize(totalSpace)
                  .withUsedsize(usedSpace)
                  .withTimestamp(now)
                  .withVos(pgroup.getValue())
                  .withAssignedendpoints(Collections.singletonList("all"));
            storageshares.put(shareName, share);
        }
        return new ArrayList<>(storageshares.values());
    }

    private List<Storageendpoint> collectEndpoint() {

        Predicate<LoginBrokerInfo> doorTagFilter = Strings.emptyToNull(doorTag) == null ?
              (d) -> true : (d) -> d.getTags().contains(doorTag);

        return loginBrokerSubscriber.doors().stream()
              .filter(doorTagFilter)
              .map(d -> {
                        Storageendpoint endpoint = new Storageendpoint()
                              .withName(id + "#" + d.getProtocolFamily() + "@" + d.getAddresses().get(0)
                                    .getCanonicalHostName() + "-" + d.getPort())
                              .withInterfacetype(d.getProtocolFamily())
                              .withInterfaceversion(d.getProtocolVersion())
                              .withEndpointurl(d.getProtocolFamily() + "://" + d.getAddresses().get(0)
                                    .getCanonicalHostName() + ":" + d.getPort() + d.getRoot())
                              .withAssignedshares(Collections.singletonList("all"));

                        return endpoint;
                    }
              ).collect(Collectors.toList());
    }

}
