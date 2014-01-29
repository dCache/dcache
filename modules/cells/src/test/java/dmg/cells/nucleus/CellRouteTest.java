package dmg.cells.nucleus;

import org.junit.Test;

import org.dcache.util.Args;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;

public class CellRouteTest
{
    @Test
    public void exactRouteShouldKnowItsName() throws Exception
    {
        CellRoute route = new CellRoute(new Args("-exact a@b gateway"));
        assertThat(route.getCellName(), is("a"));
        assertThat(route.getDomainName(), is("b"));
        assertThat(route.getRouteType(), is(CellRoute.EXACT));
        assertThat(route.getTargetName(), is("gateway"));
    }

    @Test
    public void exactRouteShouldBeLocalIfNoDomainIsGiven() throws Exception
    {
        CellRoute route = new CellRoute(new Args("-exact a gateway"));
        assertThat(route.getCellName(), is("a"));
        assertThat(route.getDomainName(), is("local"));
        assertThat(route.getRouteType(), is(CellRoute.EXACT));
        assertThat(route.getTargetName(), is("gateway"));
    }

    @Test
    public void aliasRouteShouldKnowItsName() throws Exception
    {
        CellRoute route = new CellRoute(new Args("-alias a@b gateway"));
        assertThat(route.getCellName(), is("a"));
        assertThat(route.getDomainName(), is("b"));
        assertThat(route.getRouteType(), is(CellRoute.ALIAS));
        assertThat(route.getTargetName(), is("gateway"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void wellKnownRouteShouldRejectDomain() throws Exception
    {
        CellRoute route = new CellRoute(new Args("-wellknown a@b gateway"));
    }

    @Test
    public void wellKnownRouteShouldHaveStarDomain() throws Exception
    {
        CellRoute route = new CellRoute(new Args("-wellknown a gateway"));
        assertThat(route.getCellName(), is("a"));
        assertThat(route.getDomainName(), is("*"));
        assertThat(route.getRouteType(), is(CellRoute.WELLKNOWN));
        assertThat(route.getTargetName(), is("gateway"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void domainRouteShouldRejectCell() throws Exception
    {
        CellRoute route = new CellRoute(new Args("-domain a@b gateway"));
    }

    @Test()
    public void domainRouteShouldHaveStarCell() throws Exception
    {
        CellRoute route = new CellRoute(new Args("-domain a gateway"));
        assertThat(route.getCellName(), is("*"));
        assertThat(route.getDomainName(), is("a"));
        assertThat(route.getRouteType(), is(CellRoute.DOMAIN));
        assertThat(route.getTargetName(), is("gateway"));
    }

    @Test
    public void defaultRouteShouldHaveStarCellAndDomain() throws Exception
    {
        CellRoute route = new CellRoute(new Args("-default gateway"));
        assertThat(route.getCellName(), is("*"));
        assertThat(route.getDomainName(), is("*"));
        assertThat(route.getRouteType(), is(CellRoute.DEFAULT));
        assertThat(route.getTargetName(), is("gateway"));
    }

    @Test
    public void shouldDetectDefaultRoute() throws Exception
    {
        CellRoute route = new CellRoute(new Args("*@* gateway"));
        assertThat(route.getCellName(), is("*"));
        assertThat(route.getDomainName(), is("*"));
        assertThat(route.getRouteType(), is(CellRoute.DEFAULT));
        assertThat(route.getTargetName(), is("gateway"));
    }

    @Test
    public void shouldDetectDomainRoute() throws Exception
    {
        CellRoute route = new CellRoute(new Args("*@a gateway"));
        assertThat(route.getCellName(), is("*"));
        assertThat(route.getDomainName(), is("a"));
        assertThat(route.getRouteType(), is(CellRoute.DOMAIN));
        assertThat(route.getTargetName(), is("gateway"));
    }

    @Test
    public void shouldDetectWellKnownRoute() throws Exception
    {
        CellRoute route = new CellRoute(new Args("a@* gateway"));
        assertThat(route.getCellName(), is("a"));
        assertThat(route.getDomainName(), is("*"));
        assertThat(route.getRouteType(), is(CellRoute.WELLKNOWN));
        assertThat(route.getTargetName(), is("gateway"));
    }

    @Test
    public void shouldDetectWellKnownRouteForAbsentDomain() throws Exception
    {
        CellRoute route = new CellRoute(new Args("a gateway"));
        assertThat(route.getCellName(), is("a"));
        assertThat(route.getDomainName(), is("*"));
        assertThat(route.getRouteType(), is(CellRoute.WELLKNOWN));
        assertThat(route.getTargetName(), is("gateway"));
    }

    @Test
    public void shouldDetectExactRoute() throws Exception
    {
        CellRoute route = new CellRoute(new Args("a@b gateway"));
        assertThat(route.getCellName(), is("a"));
        assertThat(route.getDomainName(), is("b"));
        assertThat(route.getRouteType(), is(CellRoute.EXACT));
        assertThat(route.getTargetName(), is("gateway"));
    }

    @Test
    public void shouldKnowItsTarget() throws Exception
    {
        CellRoute route = new CellRoute(new Args("a@b gateway"));
        assertThat(route.getTarget(), is(new CellAddressCore("gateway")));
    }

    @Test
    public void shouldOnlyHashCellAndDomain() throws Exception
    {
        CellRoute route1 = new CellRoute("a@b", "gateway1", CellRoute.EXACT);
        CellRoute route2 = new CellRoute("a@b", "gateway2", CellRoute.EXACT);

        assertThat(route1.hashCode(), is(route2.hashCode()));

        CellRoute route3 = new CellRoute("a@c", "gateway1", CellRoute.EXACT);
        assertThat(route1.hashCode(), not(is(route3.hashCode())));

        CellRoute route4 = new CellRoute("c@b", "gateway1", CellRoute.EXACT);
        assertThat(route1.hashCode(), not(is(route4.hashCode())));

        CellRoute route5 = new CellRoute("", "gateway1", CellRoute.DUMPSTER);
        CellRoute route6 = new CellRoute("", "gateway1", CellRoute.DEFAULT);
        assertThat(route5.hashCode(), is(route6.hashCode()));
    }

    @Test
    public void shouldOnlyConsiderCellAndDomainInEquals() throws Exception
    {
        CellRoute route1 = new CellRoute("a@b", "gateway1", CellRoute.EXACT);
        CellRoute route2 = new CellRoute("a@b", "gateway2", CellRoute.EXACT);

        assertThat(route1, is(route2));

        CellRoute route3 = new CellRoute("a@c", "gateway1", CellRoute.EXACT);
        assertThat(route1, not(is(route3)));

        CellRoute route4 = new CellRoute("c@b", "gateway1", CellRoute.EXACT);
        assertThat(route1, not(is(route4)));

        CellRoute route5 = new CellRoute("", "gateway1", CellRoute.DUMPSTER);
        CellRoute route6 = new CellRoute("", "gateway1", CellRoute.DEFAULT);
        assertThat(route5, is(route6));
    }
}
