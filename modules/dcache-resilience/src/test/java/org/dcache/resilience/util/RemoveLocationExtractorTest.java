package org.dcache.resilience.util;

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

import diskCacheV111.util.CacheException;
import org.dcache.resilience.TestBase;
import org.dcache.vehicles.FileAttributes;

import static org.junit.Assert.assertTrue;

public final class RemoveLocationExtractorTest extends TestBase {
    private String pool;
    private Collection<String> onlyOneCopyPer;
    private FileAttributes attributes;
    private RemoveLocationExtractor extractor;
    private Collection<String> selected;
    private int currentWeight = Integer.MAX_VALUE;

    @Before
    public void setUp() throws CacheException {
        setUpBase();
    }

    @Test
    public void selectedPoolsShouldMonotonicallyDescreaseInWeightWhenContinuouslyRemoved()
                    throws CacheException {
        givenAFileWithAReplicaOnAllResilientPools();
        givenTagConstraintsOn("hostname", "rack");
        givenThePoolInfoForThisFile();

        do {
            whenTheExtractorIsCalledForTheSetOfLocationsToRemove();
            andThenAPoolIsRandomlySelectedAndRemoved();
            assertTrue(theWeightsAreMonotonicallyDecreasing());
        } while (selected != null && !selected.isEmpty());
    }

    @Test
    public void shouldSelectAllPoolsWhenThereAreNoConstraints() throws CacheException {
        givenAFileWithAReplicaOnAllResilientPools();
        givenNoTagConstraints();
        givenThePoolInfoForThisFile();

        whenTheExtractorIsCalledForTheSetOfLocationsToRemove();

        assertTrue(theSelectedPoolsWere(allAvailablePools()));
    }

    @Test
    public void shouldSelectPools10And11WhenContraintsIncludeBoth() throws CacheException {
        givenAFileWithAReplicaOnAllResilientPools();
        givenTagConstraintsOn("hostname", "rack");
        givenThePoolInfoForThisFile();

        whenTheExtractorIsCalledForTheSetOfLocationsToRemove();

        assertTrue(theSelectedPoolsWere(pools10And11()));
    }

    @Test
    public void shouldSelectPoolsWithValueR0WhenContraintIsRack() throws CacheException {
        givenAFileWithAReplicaOnAllResilientPools();
        givenTagConstraintsOn("rack");
        givenThePoolInfoForThisFile();

        whenTheExtractorIsCalledForTheSetOfLocationsToRemove();

        assertTrue(theSelectedPoolsWere(poolsWithRackValueR0()));
    }

    @Test
    public void shouldSelectPoolsWithValuesH0H1WhenContraintIsHost() throws CacheException {
        givenAFileWithAReplicaOnAllResilientPools();
        givenTagConstraintsOn("hostname");
        givenThePoolInfoForThisFile();

        whenTheExtractorIsCalledForTheSetOfLocationsToRemove();

        assertTrue(theSelectedPoolsWere(poolsWithHostValuesH0AndH1()));
    }

    private Collection<String> allAvailablePools() {
        return memberPools().stream().map((i) -> poolInfoMap.getPool(i)).collect(
                        Collectors.toSet());
    }

    private Collection<Integer> memberPools() {
        return poolInfoMap.getPoolsOfGroup(poolInfoMap.getResilientPoolGroup(
                        poolInfoMap.getPoolIndex(pool)));
    }

    private void andThenAPoolIsRandomlySelectedAndRemoved() {
        pool = RandomSelectionStrategy.SELECTOR.apply(selected);
        attributes.getLocations().remove(pool);
    }

    private void givenAFileWithAReplicaOnAllResilientPools()
                    throws CacheException {
        loadFilesWithExcessLocations();
        attributes = aFileWithAReplicaOnAllResilientPools();
    }

    private void givenNoTagConstraints() {
    }

    private void givenTagConstraintsOn(String ... keys) {
        onlyOneCopyPer = ImmutableList.copyOf(keys);
    }

    private void givenThePoolInfoForThisFile() {
        pool = attributes.getLocations().iterator().next();
        extractor = new RemoveLocationExtractor(onlyOneCopyPer, poolInfoMap);
    }

    private Collection<String> pools10And11() {
        return ImmutableList.of("resilient_pool-10", "resilient_pool-11");
    }

    private Collection<String> poolsWithHostValuesH0AndH1() {
        return getPoolsWithTag("hostname", "h0", "h1");
    }

    private Collection<String> poolsWithRackValueR0() {
        return getPoolsWithTag("rack", "r0");
    }

    private Collection<String> getPoolsWithTag(final String tag, final String ... values) {
        return memberPools().stream().filter((p) -> {
            Map<String, String> tagMap = poolInfoMap.getTags(p);
            if (tagMap.isEmpty()) {
                return false;
            }
            String tagValue = tagMap.get(tag);
            for (String v : values) {
                if (v.equals(tagValue)) {
                    return true;
                }
            }
            return false;
        }).map((i) -> poolInfoMap.getPool(i)).collect(Collectors.toSet());
    }

    private boolean theSelectedPoolsWere(Collection<String> pools) {
        for (String pool : pools) {
            if (!selected.contains(pool)) {
                return false;
            }
        }

        for (String pool : selected) {
            if (!pools.contains(pool)) {
                return false;
            }
        }

        return true;
    }

    private boolean theWeightsAreMonotonicallyDecreasing() {
        int last = currentWeight;
        currentWeight = extractor.getLastComputedMaximum();
        return currentWeight <= last;
    }

    private void whenTheExtractorIsCalledForTheSetOfLocationsToRemove() {
        selected = extractor.getCandidateLocations(attributes.getLocations());
    }
}
