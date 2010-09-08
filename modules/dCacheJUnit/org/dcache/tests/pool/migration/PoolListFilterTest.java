package org.dcache.tests.pool.migration;

import org.junit.Test;
import org.junit.Before;
import static org.junit.Assert.*;

import org.dcache.pool.migration.PoolListFilter;
import org.dcache.pool.migration.RefreshablePoolList;
import org.dcache.util.Glob;
import org.dcache.util.ImmutableList;
import diskCacheV111.vehicles.PoolManagerPoolInformation;

import java.util.Collections;
import java.util.Collection;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;
import java.util.HashSet;
import java.util.regex.Pattern;

import org.parboiled.Parboiled;
import org.parboiled.BasicParseRunner;
import org.parboiled.support.ParsingResult;

import org.dcache.util.expression.Expression;
import org.dcache.util.expression.ExpressionParser;
import org.dcache.util.expression.TypeMismatchException;
import org.dcache.util.expression.UnknownIdentifierException;
import org.dcache.pool.migration.SymbolTable;

public class PoolListFilterTest
{
    private final PoolManagerPoolInformation POOL1 =
        createPool("pool1", 0.5, 0.5);
    private final PoolManagerPoolInformation POOL2 =
        createPool("pool2", 0.5, 0.5);
    private final PoolManagerPoolInformation POOL3 =
        createPool("pool3", Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY);

    private final PoolManagerPoolInformation SOURCE =
        createPool("source", 0.5, 0.5);

    private SymbolTable symbols;

    @Before
    public void setup()
    {
        symbols = new SymbolTable();
        symbols.put("source", POOL1);
        symbols.put("target", POOL1);
    }

    @Test
    public void testExclude()
    {
        PoolList list = new PoolList(POOL1, POOL2, POOL3);
        PoolListFilter filter =
            createFilter(list, "*1", "false", null, "true", SOURCE);

        List<PoolManagerPoolInformation> result = filter.getPools();

        assertEquals(2, result.size());
        assertDoesNotContainPool(POOL1, result);
        assertContainsPool(POOL2, result);
        assertContainsPool(POOL3, result);
    }

    @Test
    public void testExcludeWhen()
    {
        PoolList list = new PoolList(POOL1, POOL2, POOL3);
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
    public void testInclude()
    {
        PoolList list = new PoolList(POOL1, POOL2, POOL3);
        PoolListFilter filter =
            createFilter(list, null, "false", "*1", "true", SOURCE);

        List<PoolManagerPoolInformation> result = filter.getPools();

        assertEquals(1, result.size());
        assertContainsPool(POOL1, result);
        assertDoesNotContainPool(POOL2, result);
        assertDoesNotContainPool(POOL3, result);
    }

    @Test
    public void testIncludeWhen()
    {
        PoolList list = new PoolList(POOL1, POOL2, POOL3);
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
    public void testBothIncludedAndExcluded()
    {
        PoolList list = new PoolList(POOL1, POOL2, POOL3);
        PoolListFilter filter =
            createFilter(list, "*1", "false", "*1", "true", SOURCE);

        List<PoolManagerPoolInformation> result = filter.getPools();

        assertEquals(0, result.size());
    }

    @Test
    public void testBothIncludedWhenAndExcludedWhen()
    {
        PoolList list = new PoolList(POOL1, POOL2, POOL3);
        PoolListFilter filter =
            createFilter(list,
                         null, "target.name=='pool1'",
                         null, "target.name=='pool1'",
                         SOURCE);

        List<PoolManagerPoolInformation> result = filter.getPools();

        assertEquals(0, result.size());
    }

    @Test
    public void testFilterRefersToSource()
    {
        PoolList list = new PoolList(POOL1, POOL2, POOL3);
        PoolListFilter filter =
            createFilter(list,
                         null, "source.spaceCost<target.spaceCost",
                         null, "true",
                         SOURCE);
        List<PoolManagerPoolInformation> result = filter.getPools();
        assertEquals(2, result.size());
        assertContainsPool(POOL1, result);
        assertContainsPool(POOL2, result);
        assertDoesNotContainPool(POOL3, result);
    }

    @Test
    public void testCacheInvalidation()
    {
        PoolList list = new PoolList(POOL1, POOL2, POOL3);
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

    private Expression createExpression(String s)
    {
        if (s == null) {
            return null;
        }

        ExpressionParser parser =
            Parboiled.createParser(ExpressionParser.class);
        ParsingResult<Expression> result =
            BasicParseRunner.run(parser.Top(), s);
        try {
            result.resultValue.check(symbols);
        } catch (TypeMismatchException e) {
            fail(e.toString());
        } catch (UnknownIdentifierException e) {
            fail(e.toString());
        }
        return result.resultValue;
    }

    private static PoolManagerPoolInformation
        createPool(String name, double spaceCost, double cpuCost)
    {
        PoolManagerPoolInformation info =
            new PoolManagerPoolInformation(name);
        info.setSpaceCost(spaceCost);
        info.setCpuCost(cpuCost);
        return info;
    }

    private static Set<Pattern> createPatterns(String glob)
    {
        Set<Pattern> patterns = new HashSet<Pattern>();
        if (glob != null) {
            patterns.add(Glob.parseGlobToPattern(glob));
        }
        return patterns;
    }

    private PoolListFilter
        createFilter(RefreshablePoolList list,
                     String exclude, String excludeWhen,
                     String include, String includeWhen,
                     PoolManagerPoolInformation source)
    {
        return new PoolListFilter(list,
                                  createPatterns(exclude),
                                  createExpression(excludeWhen),
                                  createPatterns(include),
                                  createExpression(includeWhen),
                                  new PoolList(source));
    }

    private static boolean containsPool(String name, List<PoolManagerPoolInformation> list)
    {
        for (PoolManagerPoolInformation pool: list) {
            if (pool.getName().equals(name)) {
                return true;
            }
        }
        return false;
    }

    private static void assertContainsPool(PoolManagerPoolInformation pool,
                                           List<PoolManagerPoolInformation> list)
    {
        String name = pool.getName();
        if (!containsPool(name, list)) {
            fail("Expected pool " + name + " is not in list " + list);
        }
    }

    private static void assertDoesNotContainPool(PoolManagerPoolInformation pool,
                                                 List<PoolManagerPoolInformation> list)
    {
        String name = pool.getName();
        if (containsPool(name, list)) {
            fail("Unexpected pool " + name + " is in list " + list);
        }
    }

    /**
     * The object we want to test is abstract, hence we need to
     * subclass to test it.
     */
    static class PoolList implements RefreshablePoolList
    {
        private ImmutableList<PoolManagerPoolInformation> _list;

        public PoolList(PoolManagerPoolInformation... pools)
        {
            setPools(pools);
        }

        public boolean isValid()
        {
            return true;
        }

        public void setPools(PoolManagerPoolInformation... pools)
        {
            _list = new ImmutableList(new ArrayList(Arrays.asList(pools)));
        }

        @Override
        public void refresh()
        {
        }

        @Override
        public ImmutableList<PoolManagerPoolInformation> getPools()
        {
            return _list;
        }
    }
}
