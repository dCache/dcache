package org.dcache.tests.pool.migration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.google.common.collect.ImmutableList;
import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.vehicles.PoolManagerPoolInformation;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import org.dcache.pool.classic.IoQueueManager;
import org.dcache.pool.migration.PoolListFilter;
import org.dcache.pool.migration.RefreshablePoolList;
import org.dcache.pool.migration.SymbolTable;
import org.dcache.util.Glob;
import org.dcache.util.expression.Expression;
import org.dcache.util.expression.ExpressionParser;
import org.dcache.util.expression.TypeMismatchException;
import org.dcache.util.expression.UnknownIdentifierException;
import org.junit.Before;
import org.junit.Test;
import org.parboiled.Parboiled;
import org.parboiled.parserunners.BasicParseRunner;
import org.parboiled.support.ParsingResult;

public class PoolListFilterTest {

    private final PoolManagerPoolInformation POOL1 =
          createPool("pool1");
    private final PoolManagerPoolInformation POOL2 =
          createPool("pool2");
    private final PoolManagerPoolInformation POOL3 =
          createPool("pool3");

    private final PoolManagerPoolInformation SOURCE =
          createPool("source");

    private SymbolTable symbols;

    @Before
    public void setup() {
        symbols = new SymbolTable();
        symbols.put("source", POOL1);
        symbols.put("target", POOL1);
    }

    @Test
    public void testExclude() {
        PoolList list = PoolList.newOnlineList(POOL1, POOL2, POOL3);
        PoolListFilter filter =
              createFilter(list, "*1", "false", null, "true", SOURCE);

        List<PoolManagerPoolInformation> result = filter.getPools();

        assertEquals(2, result.size());
        assertDoesNotContainPool(POOL1, result);
        assertContainsPool(POOL2, result);
        assertContainsPool(POOL3, result);
    }

    @Test
    public void testExcludeOffline() {
        PoolList list = PoolList.newOfflineList("pool1", "pool2", "pool3");
        PoolListFilter filter =
              createFilter(list, "*1", "false", null, "true", SOURCE);

        List<String> result = filter.getOfflinePools();

        assertThat(result, hasSize(2));
        assertThat(result, containsInAnyOrder("pool2", "pool3"));
    }

    @Test
    public void testExcludeWhen() {
        PoolList list = PoolList.newOnlineList(POOL1, POOL2, POOL3);
        PoolListFilter filter =
              createFilter(list,
                    null, "target.name=~'pool[12]'",
                    null, "true",
                    SOURCE);

        List<PoolManagerPoolInformation> result = filter.getPools();

        assertEquals(1, result.size());
        assertDoesNotContainPool(POOL1, result);
        assertDoesNotContainPool(POOL2, result);
        assertContainsPool(POOL3, result);
    }

    @Test
    public void testInclude() {
        PoolList list = PoolList.newOnlineList(POOL1, POOL2, POOL3);
        PoolListFilter filter =
              createFilter(list, null, "false", "*1", "true", SOURCE);

        List<PoolManagerPoolInformation> result = filter.getPools();

        assertEquals(1, result.size());
        assertContainsPool(POOL1, result);
        assertDoesNotContainPool(POOL2, result);
        assertDoesNotContainPool(POOL3, result);
    }

    @Test
    public void testIncludeOffline() {
        PoolList list = PoolList.newOfflineList("pool1", "pool2", "pool3");
        PoolListFilter filter =
              createFilter(list, null, "false", "*1", "true", SOURCE);

        List<String> result = filter.getOfflinePools();

        assertThat(result, hasSize(1));
        assertThat(result, contains("pool1"));
    }

    @Test
    public void testIncludeWhen() {
        PoolList list = PoolList.newOnlineList(POOL1, POOL2, POOL3);
        PoolListFilter filter =
              createFilter(list,
                    null, "false", null,
                    "target.name=~'pool[12]'",
                    SOURCE);

        List<PoolManagerPoolInformation> result = filter.getPools();

        assertEquals(2, result.size());
        assertContainsPool(POOL1, result);
        assertContainsPool(POOL2, result);
        assertDoesNotContainPool(POOL3, result);
    }

    @Test
    public void testBothIncludedAndExcluded() {
        PoolList list = PoolList.newOnlineList(POOL1, POOL2, POOL3);
        PoolListFilter filter =
              createFilter(list, "*1", "false", "*1", "true", SOURCE);

        List<PoolManagerPoolInformation> result = filter.getPools();

        assertEquals(0, result.size());
    }

    @Test
    public void testBothIncludedWhenAndExcludedWhen() {
        PoolList list = PoolList.newOnlineList(POOL1, POOL2, POOL3);
        PoolListFilter filter =
              createFilter(list,
                    null, "target.name=='pool1'",
                    null, "target.name=='pool1'",
                    SOURCE);

        List<PoolManagerPoolInformation> result = filter.getPools();

        assertEquals(0, result.size());
    }

    @Test
    public void testFilterRefersToSource() {
        PoolList list = PoolList.newOnlineList(POOL1, POOL2, POOL3);
        PoolListFilter filter =
              createFilter(list,
                    null, "source.name=='source'",
                    null, "true",
                    SOURCE);
        List<PoolManagerPoolInformation> result = filter.getPools();
        assertEquals(0, result.size());
    }

    @Test
    public void testCacheInvalidation() {
        PoolList list = PoolList.newOnlineList(POOL1, POOL2, POOL3);
        PoolListFilter filter =
              createFilter(list,
                    null, "false",
                    null, "target.name=~'pool[12]'",
                    SOURCE);

        List<PoolManagerPoolInformation> result = filter.getPools();

        assertEquals(2, result.size());
        assertContainsPool(POOL1, result);
        assertContainsPool(POOL2, result);
        assertDoesNotContainPool(POOL3, result);

        list.setPools(POOL2, POOL3);
        result = filter.getPools();

        assertEquals(1, result.size());
        assertDoesNotContainPool(POOL1, result);
        assertContainsPool(POOL2, result);
        assertDoesNotContainPool(POOL3, result);
    }

    private Expression createExpression(String s) {
        if (s == null) {
            return null;
        }

        ExpressionParser parser =
              Parboiled.createParser(ExpressionParser.class);
        ParsingResult<Expression> result =
              new BasicParseRunner(parser.Top()).run(s);
        try {
            result.resultValue.check(symbols);
        } catch (TypeMismatchException | UnknownIdentifierException e) {
            fail(e.toString());
        }
        return result.resultValue;
    }

    private static PoolManagerPoolInformation
    createPool(String name) {
        return new PoolManagerPoolInformation(name,
              new PoolCostInfo(name, IoQueueManager.DEFAULT_QUEUE), 0);
    }

    private static Set<Pattern> createPatterns(String glob) {
        Set<Pattern> patterns = new HashSet<>();
        if (glob != null) {
            patterns.add(Glob.parseGlobToPattern(glob));
        }
        return patterns;
    }

    private PoolListFilter
    createFilter(RefreshablePoolList list,
          String exclude, String excludeWhen,
          String include, String includeWhen,
          PoolManagerPoolInformation source) {
        return new PoolListFilter(list,
              createPatterns(exclude),
              createExpression(excludeWhen),
              createPatterns(include),
              createExpression(includeWhen),
              PoolList.newOnlineList(source));
    }

    private static boolean containsPool(String name, List<PoolManagerPoolInformation> list) {
        for (PoolManagerPoolInformation pool : list) {
            if (pool.getName().equals(name)) {
                return true;
            }
        }
        return false;
    }

    private static void assertContainsPool(PoolManagerPoolInformation pool,
          List<PoolManagerPoolInformation> list) {
        String name = pool.getName();
        if (!containsPool(name, list)) {
            fail("Expected pool " + name + " is not in list " + list);
        }
    }

    private static void assertDoesNotContainPool(PoolManagerPoolInformation pool,
          List<PoolManagerPoolInformation> list) {
        String name = pool.getName();
        if (containsPool(name, list)) {
            fail("Unexpected pool " + name + " is in list " + list);
        }
    }

    /**
     * The object we want to test is abstract, hence we need to subclass to test it.
     */
    static class PoolList implements RefreshablePoolList {

        private ImmutableList<PoolManagerPoolInformation> _list;
        private ImmutableList<String> _offlinePools;

        public PoolList(PoolManagerPoolInformation[] pools, String[] offlinePools) {
            setPools(pools);
            setOfflinePools(offlinePools);
        }

        public static PoolList newOfflineList(String... pools) {
            return new PoolList(new PoolManagerPoolInformation[0], pools);
        }

        public static PoolList newOnlineList(PoolManagerPoolInformation... pools) {
            return new PoolList(pools, new String[0]);
        }

        @Override
        public boolean isValid() {
            return true;
        }

        public void setPools(PoolManagerPoolInformation... pools) {
            _list = ImmutableList.copyOf(pools);
        }

        public void setOfflinePools(String... pools) {
            _offlinePools = ImmutableList.copyOf(pools);
        }

        @Override
        public void refresh() {
        }

        @Override
        public ImmutableList<String> getOfflinePools() {
            return _offlinePools;
        }

        @Override
        public ImmutableList<PoolManagerPoolInformation> getPools() {
            return _list;
        }
    }
}
