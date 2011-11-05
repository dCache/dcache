// $Id: PoolInfoObserverEngineV2.java,v 1.1 2006-06-05 08:51:27 patrick Exp $Cg

package diskCacheV111.services.web;

import dmg.cells.nucleus.CellNucleus;
import dmg.util.HttpException;
import dmg.util.HttpResponseEngine;
import dmg.util.HttpRequest;

import diskCacheV111.util.HTMLWriter;

import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeMap;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.io.OutputStream;
import java.io.PrintWriter;

public class PoolInfoObserverEngineV2 implements HttpResponseEngine
{
    private static final int _menuColumns = 4;

    private final CellNucleus _nucleus;
    private final Map<String,String> _tableSelection =
        new LinkedHashMap<String,String>();
    private boolean _showPoolGroupUsage = false;

    private int _errorCounter = 0;
    private int _requestCounter = 0;

    private PoolCellQueryContainer _container;

    public PoolInfoObserverEngineV2(CellNucleus nucleus, String[] args)
    {
        _nucleus = nucleus;

        for (String s : args) {
            if (s.startsWith("showPoolGroupUsage=")) {
                _showPoolGroupUsage =
                    s.substring("showPoolGroupUsage=".length()).equals("true");
            }
        }

        _tableSelection.put("Cell View"      , "cells");
        _tableSelection.put("Space Usage"    , "spaces");
        _tableSelection.put("Request Queues" , "queues");
    }

    public void queryUrl(HttpRequest request)
        throws HttpException
    {
        String[]    urlItems = request.getRequestTokens();
        int         offset   = request.getRequestTokenOffset();
        OutputStream out     = request.getOutputStream();

        _requestCounter++;

        request.printHttpHeader(0);

        HTMLWriter html = new HTMLWriter(out, _nucleus.getDomainContext());
        try {
            html.addHeader("/styles/poolinfo.css", "Pool Property Tables");

            if (urlItems.length < 1)
                return;

            if (urlItems.length > 1 && urlItems[1].equals("list")) {
                Object o = _nucleus.getDomainContext("poolgroup-map.ser");
                if (o ==  null) {
                    html.println("<h3>Information not yet available</h3>");
                    return;
                } else if (!(o instanceof PoolCellQueryContainer)) {
                    html.println("<h3>Internal error: poolgroup-map.ser contains unknown class</h3>");
                    return;
                }

                _container = (PoolCellQueryContainer)o;
                String className = urlItems.length > 2 ? urlItems[2] : null;
                String groupName = urlItems.length > 3 ? urlItems[3] : null;
                String selection = urlItems.length > 4 ? urlItems[4] : null;

                printClassMenu(html, className);
                if (className == null)
                    return;

                if (_showPoolGroupUsage) {
                    printGroupList(html, className);
                } else {
                    printGroupMenu(html, className, groupName);
                }
                if (groupName == null)
                    return;

                html.println("<h3>Pool group <emph>" + groupName + "</emph></h3>");
                printMenuTable(html, _tableSelection.entrySet(),
                               "/pools/list/" + className + "/"+groupName + "/",
                               selection);

                if (selection == null)
                    return;

                Map<String,PoolCellQueryInfo> poolMap =
                    _container.getPoolMap(className, groupName);
                if (poolMap == null)
                    return;

                if (selection.equals("cells")) {
                    printCells(html, poolMap);
                } else if (selection.equals("spaces")) {
                    printPools(html, poolMap);
                } else if (selection.equals("queues")) {
                    printPoolActions(html, poolMap);
                }
            }
        } catch (Exception e) {
            _errorCounter ++;
            showProblem(html, e.getMessage());
            html.println("<ul>");
            for (int i = 0; i < urlItems.length; i++) {
                html.println("<li> [" + i + "] " + urlItems[i] + "</li>");
            }
            html.println("</ul>");
        } finally {
            html.addFooter(getClass().getName() + " [$Rev$]");
        }
    }

    private void printPoolActions(HTMLWriter html, Map poolMap)
    {
        PoolQueueTableWriter writer = new PoolQueueTableWriter(html);
        writer.print(new TreeMap(poolMap).values());
    }

    private void printPools(HTMLWriter html, Map poolMap)
    {
        PoolInfoTableWriter writer = new PoolInfoTableWriter(html);
        writer.print(new TreeMap(poolMap).values(), !_showPoolGroupUsage);
    }

    private void printCells(HTMLWriter html, Map poolMap)
    {
        CellInfoTableWriter writer = new CellInfoTableWriter(html);
        writer.print(new TreeMap(poolMap).values());
    }

    private void printClassMenu(HTMLWriter pw, String className)
    {
        Set<String> classSet = _container.getPoolClassSet();
        pw.println("<h3>Pool Views</h3>");
        printMenuTable(pw, classSet, "/pools/list/", className);
    }

    private void printGroupMenu(HTMLWriter pw, String className, String groupName)
    {
        Set<String> groupSet =
            _container.getPoolGroupSetByClassName(className);

        if (groupSet != null) {
            pw.println("<h3>Pool groups of <emph>"
                       + className + "</emph></h3>");
            printMenuTable(pw, groupSet,
                           "/pools/list/" + className + "/", groupName);
        }
    }

    private void printGroupList(HTMLWriter html, String className)
    {
        Set<String> groupSet =
            _container.getPoolGroupSetByClassName(className);

        if (groupSet != null) {
            html.println("<h3>Pool groups of <emph>"
                       + className + "</emph></h3>");

            TreeMap<String,Collection<PoolCellQueryInfo>> info =
                new TreeMap<String,Collection<PoolCellQueryInfo>>();

            for (String group : groupSet) {
                info.put(group,
                         _container.getPoolMap(className, group).values());
            }

            PoolGroupInfoTableWriter writer =
                new PoolGroupInfoTableWriter(html);
            writer.print("/pools/list/" + className + "/", info);
        }
    }

    private void printMenuTable(HTMLWriter html, Set itemSet,
                                String linkBase, String currentItem)
    {
        html.beginTable("menu");
        if (!itemSet.isEmpty()) {
            html.beginRow();

            int n = 0;
            for (Object o: itemSet) {
                if (n > 0 && (n % _menuColumns) == 0) {
                    html.endRow();
                    html.beginRow();
                }

                n++;

                String name;
                String linkName;
                if (o instanceof String) {
                    name = linkName = (String)o;
                } else {
                    Map.Entry<String,String> e =
                        (Map.Entry<String,String>)o;
                    name     = e.getKey();
                    linkName = e.getValue();
                }

                boolean active =
                    currentItem != null && currentItem.equals(linkName);

                html.td(active ? "active" : null,
                        "<a href=\"", linkBase, linkName, "/\">", name, "</a>");
            }

            while ((n++ % _menuColumns) != 0)
                html.td(null);
            html.endRow();
        }
        html.endTable();
    }

    private void showProblem(PrintWriter pw, String message)
    {
        pw.print("<h1><emph>");
        pw.print(message);
        pw.println("<emph></h1>");
    }
}
