package dmg.cells.nucleus;

import java.util.Optional;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;

public class CellRouteTest
{
    @Test
    public void shouldOnlyHashCellAndDomain() throws Exception
    {
        CellRoute route1 = new CellRoute("a@b", new CellAddressCore("gateway1"), Optional.empty(), CellRoute.EXACT);
        CellRoute route2 = new CellRoute("a@b", new CellAddressCore("gateway2"), Optional.empty(), CellRoute.EXACT);

        assertThat(route1.hashCode(), not(is(route2.hashCode())));

        CellRoute route3 = new CellRoute("a@c", new CellAddressCore("gateway1"), Optional.empty(), CellRoute.EXACT);
        assertThat(route1.hashCode(), not(is(route3.hashCode())));

        CellRoute route4 = new CellRoute("c@b", new CellAddressCore("gateway1"), Optional.empty(), CellRoute.EXACT);
        assertThat(route1.hashCode(), not(is(route4.hashCode())));

        CellRoute route5 = new CellRoute("", new CellAddressCore("gateway1"), Optional.empty(), CellRoute.DUMPSTER);
        CellRoute route6 = new CellRoute("", new CellAddressCore("gateway1"), Optional.empty(), CellRoute.DEFAULT);
        assertThat(route5.hashCode(), not(is(route6.hashCode())));
    }

    @Test
    public void shouldOnlyConsiderCellAndDomainInEquals() throws Exception
    {
        CellRoute route1 = new CellRoute("a@b", new CellAddressCore("gateway1"), Optional.empty(), CellRoute.EXACT);
        CellRoute route2 = new CellRoute("a@b", new CellAddressCore("gateway2"), Optional.empty(), CellRoute.EXACT);

        assertThat(route1, not(is(route2)));

        CellRoute route3 = new CellRoute("a@c", new CellAddressCore("gateway1"), Optional.empty(), CellRoute.EXACT);
        assertThat(route1, not(is(route3)));

        CellRoute route4 = new CellRoute("c@b", new CellAddressCore("gateway1"), Optional.empty(), CellRoute.EXACT);
        assertThat(route1, not(is(route4)));

        CellRoute route5 = new CellRoute("", new CellAddressCore("gateway1"), Optional.empty(), CellRoute.DUMPSTER);
        CellRoute route6 = new CellRoute("", new CellAddressCore("gateway1"), Optional.empty(), CellRoute.DEFAULT);
        assertThat(route5, not(is(route6)));
    }
}
