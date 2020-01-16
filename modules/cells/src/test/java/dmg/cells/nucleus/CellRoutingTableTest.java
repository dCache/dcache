package dmg.cells.nucleus;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

public class CellRoutingTableTest {


    private CellRoutingTable routingTable;

    @Before
    public void setUp() {
        routingTable = new CellRoutingTable();
    }

    @Test
    public void testAddRoute() {

        CellAddressCore gateway = new CellAddressCore("gw", "core");
        CellRoute route = new CellRoute("cell-A", gateway, Optional.empty(), CellRoute.QUEUE);

        routingTable.add(route);

        assertEquals(1, routingTable.getRoutingList().length);
    }

    @Test
    public void testDeleteRoute() {

        CellAddressCore gateway = new CellAddressCore("gw", "core");
        CellRoute route = new CellRoute("cell-A", gateway, Optional.empty(), CellRoute.QUEUE);

        routingTable.add(route);
        routingTable.delete(route);
        assertEquals(0, routingTable.getRoutingList().length);
    }

    @Test
    public void testDeleteGateway() {

        CellAddressCore gateway = new CellAddressCore("gw", "core");
        CellRoute route = new CellRoute("cell-A", gateway, Optional.empty(), CellRoute.QUEUE);

        routingTable.add(route);
        routingTable.delete(gateway);
       assertEquals(0, routingTable.getRoutingList().length);
    }

    @Test
    public void testFindRoute() {

        CellAddressCore gateway = new CellAddressCore("gw", "core");
        CellRoute route = new CellRoute("cell-A", gateway, Optional.empty(), CellRoute.QUEUE);

        routingTable.add(route);
        route = routingTable.find(new CellAddressCore("cell-A"), Optional.empty(), true);
        assertNotNull(route);
    }

    @Test
    public void testNonExistingRoute() {

        CellAddressCore gateway = new CellAddressCore("gw", "core");
        CellRoute route = new CellRoute("cell-A", gateway, Optional.empty(), CellRoute.QUEUE);

        routingTable.add(route);
        route = routingTable.find(new CellAddressCore("cell-B"), Optional.empty(), true);
        assertNull(route);
    }

    @Test
    public void testMultipleRoutesForQueue() {

        CellAddressCore gateway1 = new CellAddressCore("gw-1", "core-1");
        CellAddressCore gateway2 = new CellAddressCore("gw-2", "core-2");
        CellRoute route1 = new CellRoute("cell-A", gateway1, Optional.empty(), CellRoute.QUEUE);
        CellRoute route2 = new CellRoute("cell-A", gateway2, Optional.empty(), CellRoute.QUEUE);

        routingTable.add(route1);
        routingTable.add(route2);

        Map<String, Long> alternativeRoutes = IntStream
                .generate(() -> 1)
                .limit(10)
                .mapToObj( i -> routingTable.find(new CellAddressCore("cell-A"), Optional.empty(), true))
                .map(r -> r.getTarget())
                .map(t -> t.toString())
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

        assertEquals(2, alternativeRoutes.size());
    }

    @Test
    public void testMultipleRoutesForDefault() {

        CellAddressCore gateway1 = new CellAddressCore("gw-1", "core-1");
        CellAddressCore gateway2 = new CellAddressCore("gw-2", "core-2");
        CellRoute route1 = new CellRoute(null, gateway1, Optional.empty(), CellRoute.DEFAULT);
        CellRoute route2 = new CellRoute(null, gateway2, Optional.empty(), CellRoute.DEFAULT);

        routingTable.add(route1);
        routingTable.add(route2);

        Map<String, Long> alternativeRoutes = IntStream
                .generate(() -> 1)
                .limit(10)
                .mapToObj(i -> routingTable.find(new CellAddressCore("cell-A"), Optional.empty(), true))
                .map(r -> r.getTarget())
                .map(t -> t.toString())
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

        assertEquals(1, alternativeRoutes.size());
    }

    @Test
    public void testNoDefaultRoute() {

        CellAddressCore gateway = new CellAddressCore("gw", "core");
        CellRoute route = new CellRoute("cell-A", gateway, Optional.empty(), CellRoute.QUEUE);

        routingTable.add(route);
        assertFalse(routingTable.hasDefaultRoute());
    }

    @Test
    public void testAddDefaultRoute() {

        CellAddressCore gateway = new CellAddressCore("gw", "core");
        CellRoute route = new CellRoute(null, gateway, Optional.empty(), CellRoute.DEFAULT);

        routingTable.add(route);
        assertTrue(routingTable.hasDefaultRoute());
    }


    @Test
    public void testMultipleRoutesDefaultWithZone() {

        CellAddressCore gateway1 = new CellAddressCore("gw-1", "core-1");
        CellAddressCore gateway2 = new CellAddressCore("gw-2", "core-2");
        CellRoute route1 = new CellRoute(null, gateway1, Optional.of("zone-A"), CellRoute.DEFAULT);
        CellRoute route2 = new CellRoute(null, gateway2, Optional.of("zone-B"), CellRoute.DEFAULT);

        routingTable.add(route1);
        routingTable.add(route2);

        route1 = routingTable.find(new CellAddressCore("cell-B"), Optional.of("zone-A"), true);
        assertEquals("zone-A", route1.getZone().get());

        route2 = routingTable.find(new CellAddressCore("cell-B"), Optional.of("zone-B"), true);
        assertEquals("zone-B", route2.getZone().get());
    }

    @Test
    public void testMultipleRoutesDefaultNoZone() {

        CellAddressCore gateway1 = new CellAddressCore("gw-1", "core-1");
        CellAddressCore gateway2 = new CellAddressCore("gw-2", "core-2");
        CellRoute route1 = new CellRoute(null, gateway1, Optional.empty(), CellRoute.DEFAULT);
        CellRoute route2 = new CellRoute(null, gateway2, Optional.empty(), CellRoute.DEFAULT);

        routingTable.add(route1);
        routingTable.add(route2);

        Map<String, Long> alternativeRoutes = IntStream
                .generate(() -> 1)
                .limit(10)
                .mapToObj(i -> routingTable.find(new CellAddressCore("cell-A"), Optional.of("zone-A"), true))
                .map(r -> r.getTarget())
                .map(t -> t.toString())
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

        assertEquals(1, alternativeRoutes.size());
    }

    @Test
    public void testMultipleRoutesDefaultWithZone2() {

        CellAddressCore gateway1 = new CellAddressCore("gw-1", "core-1");
        CellAddressCore gateway2 = new CellAddressCore("gw-2", "core-2");
        CellRoute route1 = new CellRoute(null, gateway1, Optional.of("zone-A"), CellRoute.DEFAULT);
        CellRoute route2 = new CellRoute(null, gateway2, Optional.of("zone-B"), CellRoute.DEFAULT);

        routingTable.add(route1);
        routingTable.add(route2);

        Map<String, Long> alternativeRoutes = IntStream
                .generate(() -> 1)
                .limit(10)
                .mapToObj(i -> routingTable.find(new CellAddressCore("cell-A"), Optional.empty(), true))
                .map(r -> r.getTarget())
                .map(t -> t.toString())
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

        assertEquals(1, alternativeRoutes.size());
    }

    @Test
    public void testMultipleRoutesQueueWithZone() {

        CellAddressCore gateway1 = new CellAddressCore("gw-1", "core-1");
        CellAddressCore gateway2 = new CellAddressCore("gw-2", "core-2");
        CellRoute route1 = new CellRoute("cell-A", gateway1, Optional.of("zone-A"), CellRoute.QUEUE);
        CellRoute route2 = new CellRoute("cell-A", gateway2, Optional.of("zone-B"), CellRoute.QUEUE);

        routingTable.add(route1);
        routingTable.add(route2);

        Map<String, Long> alternativeRoutes = IntStream
                .generate(() -> 1)
                .limit(10)
                .mapToObj(i -> routingTable.find(new CellAddressCore("cell-A"), Optional.of("zone-A"), true))
                .map(r -> r.getZone().get())
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

        assertEquals(1, alternativeRoutes.size());
        assertNotNull(alternativeRoutes.get("zone-A"));
    }

    @Test
    public void testMultipleRoutesQueueWithZone2() {

        CellAddressCore gateway1 = new CellAddressCore("gw-1", "core-1");
        CellAddressCore gateway2 = new CellAddressCore("gw-2", "core-2");
        CellRoute route1 = new CellRoute("cell-A", gateway1, Optional.of("zone-A"), CellRoute.QUEUE);
        CellRoute route2 = new CellRoute("cell-A", gateway2, Optional.of("zone-B"), CellRoute.QUEUE);

        routingTable.add(route1);
        routingTable.add(route2);

        Map<String, Long> alternativeRoutes = IntStream
                .generate(() -> 1)
                .limit(10)
                .mapToObj(i -> routingTable.find(new CellAddressCore("cell-A"), Optional.empty(), true))
                .map(r -> r.getZone().get())
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

        assertEquals(2, alternativeRoutes.size());
    }

    @Test
    public void testMultipleRoutesQueueWithNoZone() {

        CellAddressCore gateway1 = new CellAddressCore("gw-1", "core-1");
        CellAddressCore gateway2 = new CellAddressCore("gw-2", "core-2");
        CellRoute route1 = new CellRoute("cell-A", gateway1, Optional.empty(), CellRoute.QUEUE);
        CellRoute route2 = new CellRoute("cell-A", gateway2, Optional.empty(), CellRoute.QUEUE);

        routingTable.add(route1);
        routingTable.add(route2);

        Map<String, Long> alternativeRoutes = IntStream
                .generate(() -> 1)
                .limit(10)
                .mapToObj(i -> routingTable.find(new CellAddressCore("cell-A"), Optional.of("zone-B"), true))
                .map(r -> r.getTarget())
                .map(t -> t.toString())
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

        assertEquals(2, alternativeRoutes.size());
    }
}
