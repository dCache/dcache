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
import java.io.OutputStream;
import java.io.PrintWriter;

public class PoolInfoObserverEngineV2 implements HttpResponseEngine
{
    private static final int _menuColumns = 4;

    private final CellNucleus _nucleus;
    private final Map<String,String> _tableSelection =
        new HashMap<String,String>();

    private int _errorCounter = 0;
    private int _requestCounter = 0;

    private PoolCellQueryContainer _container;

    public PoolInfoObserverEngineV2(CellNucleus nucleus, String[] args)
    {
        _nucleus = nucleus;
        for (int i = 0; i < args.length; i++) {
            _nucleus.say("PoolInfoObserverEngineV2 : argument : " + i
                         + " : " + args[i]);
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

                printMenu(html, className, groupName, selection);

                if (className == null || groupName == null || selection == null)
                    return;

                Map<String,PoolCellQueryInfo> poolMap =
                    _container.getPoolMap(className, groupName);
                if (poolMap == null)
                    return;

                if (selection.equals("cells")) {
                    html.append("<h3>Cell Info of group <emph>")
                        .append(groupName)
                        .append("</emph> in view <emph>")
                        .append(className)
                        .println("</emph></h3>");
                    printCells(html, poolMap);
                } else if (selection.equals("spaces")) {
                    html.append("<h3>Space Info of group <emph>")
                        .append(groupName)
                        .append("</emph> in view <emph>")
                        .append(className)
                        .println("</emph></h3>");
                    printPools(html, poolMap);
                } else if (selection.equals("queues")) {
                    html.append("<h3>Queue Info of group <emph>")
                        .append(groupName)
                        .append("</empg> in view <emph>")
                        .append(className)
                        .println("</emph></h3>");
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
        writer.print(new TreeMap(poolMap).values());
    }

    private void printCells(HTMLWriter html, Map poolMap)
    {
        CellInfoTableWriter writer = new CellInfoTableWriter(html);
        writer.print(new TreeMap(poolMap).values());
    }

    private void printMenu(HTMLWriter pw, String className,
                           String groupName, String selection)
    {
        printClassMenu(pw, className);
        printGroupMenu(pw, className, groupName);

        if (className == null || groupName == null)
            return;

        pw.println("<h3>Table Selection</h3>");
        printMenuTable(pw, _tableSelection.entrySet(),
                       "/pools/list/" + className + "/"+groupName + "/",
                       selection);
    }

    private void printClassMenu(HTMLWriter pw, String className)
    {
        Set<String> classSet = _container.getPoolClassSet();
        pw.println("<h3>Pool Views</h3>");
        printMenuTable(pw, classSet, "/pools/list/", className);
    }

    private void printGroupMenu(HTMLWriter pw, String className, String groupName)
    {
        if (className == null)
            return;

        Set<String> groupSet =
            _container.getPoolGroupSetByClassName(className);
        //
        // this shouldn't happen
        //
        if (groupSet == null)
            return;

        pw.println("<h3>Pool groups of view <emph>"
                   + className + "</emph></h3>");
        printMenuTable(pw, groupSet,
                       "/pools/list/" + className + "/", groupName);
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
