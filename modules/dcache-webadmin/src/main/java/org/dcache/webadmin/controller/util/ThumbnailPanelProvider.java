  /*
COPYRIGHT STATUS:
Dec 1st 2001, Fermi National Accelerator Laboratory (FNAL) documents and
software are sponsored by the U.S. Department of Energy under Contract No.
DE-AC02-76CH03000. Therefore, the U.S. Government retains a  world-wide
non-exclusive, royalty-free license to publish or reproduce these documents
and software for U.S. Government purposes.  All documents and software
available from this server are protected under the U.S. and Foreign
Copyright Laws, and FNAL reserves all rights.

Distribution of the software available from this server is free of
charge subject to the user following the terms of the Fermitools
Software Legal Information.

Redistribution and/or modification of the software shall be accompanied
by the Fermitools Software Legal Information  (including the copyright
notice).

The user is asked to feed back problems, benefits, and/or suggestions
about the software to the Fermilab Software Providers.

Neither the name of Fermilab, the  URA, nor the names of the contributors
may be used to endorse or promote products derived from this software
without specific prior written permission.

DISCLAIMER OF LIABILITY (BSD):

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED  WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED  WARRANTIES OF MERCHANTABILITY AND FITNESS
FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL FERMILAB,
OR THE URA, OR THE U.S. DEPARTMENT of ENERGY, OR CONTRIBUTORS BE LIABLE
FOR  ANY  DIRECT, INDIRECT,  INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
OF SUBSTITUTE  GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY  OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT  OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE  POSSIBILITY OF SUCH DAMAGE.

Liabilities of the Government:

This software is provided by URA, independent from its Prime Contract
with the U.S. Department of Energy. URA is acting independently from
the Government and in its own private capacity and is not acting on
behalf of the U.S. Government, nor as its contractor nor its agent.
Correspondingly, it is understood and agreed that the U.S. Government
has no connection to this software and in no manner whatsoever shall
be liable for nor assume any responsibility or obligation for any claim,
cost, or damages arising out of or resulting from the use of the software
available from this server.

Export Control:

All documents and software available from this server are subject to U.S.
export control laws.  Anyone downloading information from this server is
obligated to secure any necessary Government licenses before exporting
documents or software obtained from this server.
 */
package org.dcache.webadmin.controller.util;

import org.apache.wicket.extensions.markup.html.repeater.data.sort.SortOrder;
import org.apache.wicket.extensions.markup.html.repeater.util.SortParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;
import java.util.Comparator;
import java.util.List;

import org.dcache.webadmin.model.dataaccess.util.rrd4j.RrdSettings;
import org.dcache.webadmin.view.beans.AbstractRegexFilterBean;
import org.dcache.webadmin.view.beans.PoolPlotOptionBean;
import org.dcache.webadmin.view.beans.ThumbnailPanelBean;
import org.dcache.webadmin.view.beans.WebAdminInterfaceSession;
import org.dcache.webadmin.view.panels.poolqueues.PoolQueuePlotsDisplayPanel;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Provider for the view displaying thumbnail links (see
 * {@link PoolQueuePlotsDisplayPanel}) to popup displays.
 */
public final class ThumbnailPanelProvider extends
                AbstractRegexFilteringProvider<ThumbnailPanelBean, String> {
    private static final Logger LOGGER
                    = LoggerFactory.getLogger(ThumbnailPanelProvider.class);

    private static final long serialVersionUID = 9211014459588122003L;

    private static final FilenameFilter FILENAME_FILTER = (dir, name) ->
                    name.contains(RrdSettings.FILE_SUFFIX)
			        && !name.contains(RrdSettings.ALL_POOLS);

    private static final int MAX_COLS = 8;

    public enum Level {
        ALL_POOLS,
        POOL_GROUPS,
        POOLS_OF_GROUP
    }

    private final File dir;
    private final File all;
    private final int imgWidth;
    private final int imgHeight;
    private final String imgType;

    public ThumbnailPanelProvider(String plotsDirectoryPath, String imgHeight,
                    String imgWidth, String imgType) {
        checkNotNull(plotsDirectoryPath);
        dir = new File(plotsDirectoryPath);
        this.imgHeight = Integer.parseInt(imgHeight);
        this.imgWidth = Integer.parseInt(imgWidth);
        this.imgType = imgType;
        all = RrdSettings.getImagePath(imgType, dir, RrdSettings.ALL_POOLS);
    }

    public Level getLevel() {
        return getPoolPlotBean().getLevel();
    }

    public String getLevelTitle() {
        switch (getLevel()) {
            case ALL_POOLS:
                return "Pool Summary";
            case POOL_GROUPS:
                return "Pool Groups";
            case POOLS_OF_GROUP:
                return getName();
            default:
                return "Error!";
        }
    }

    public String getName() {
        return getPoolPlotBean().getName();
    }

    public int getNumCols() {
        double sqrt = Math.sqrt(getFiltered().size());
        double lower = Math.floor(sqrt);
        return Math.max(1, Math.min(MAX_COLS, (int) lower));
    }

    public String getOrder() {
        return getPoolPlotBean().getOrder();
    }

    @Override
    public SortParam getSort() {
        return getPoolPlotBean().getSort();
    }

    /**
     * This method should be called before rendering.
     */
    public void refresh() {
        LOGGER.trace("refresh called");

        PoolPlotOptionBean options = getPoolPlotBean();
        LOGGER.debug("current options: {}", options);

        List<ThumbnailPanelBean> panels = options.getPanels();
        File subdir;
        File[] files;

        switch (options.getLevel()) {
            case POOLS_OF_GROUP:
                subdir = new File(dir, RrdSettings.GROUP_DIR + options.getName());
                files = subdir.listFiles(FILENAME_FILTER);
                break;
            case POOL_GROUPS:
                subdir = dir;
                files = subdir.listFiles(FILENAME_FILTER);
                break;
            case ALL_POOLS:
            default:
                subdir = dir;
                files = new File[]{all};
        }

	    panels.clear();

        if (files == null || files.length == 0) {
            return;
        }

        for (File file : files) {
            panels.add(new ThumbnailPanelBean(file, imgHeight, imgWidth));
        }

        LOGGER.debug("panels are now: {}", panels);
    }

    public void setLevel(Level level) {
        getPoolPlotBean().setLevel(level);
    }

    public void setName(String name) {
        getPoolPlotBean().setName(name);
    }

    public void setOrder(String order) {
        getPoolPlotBean().setOrder(order);
        setSort("name", SortOrder.valueOf(order));
    }

    @Override
    public void setSort(SortParam param) {
        getPoolPlotBean().setSort(param);
    }

    @Override
    public void setSort(String property, SortOrder order) {
        getPoolPlotBean().setSort(property, order);
    }

    @Override
    protected Comparator<ThumbnailPanelBean> getComparator() {
        return (p0, p1) -> {
            SortParam sort = getSort();
            int dir1;
            if (sort == null) {
                dir1 = 1;
            } else {
                dir1 = sort.isAscending() ? 1 : -1;
            }
            Comparable c0 = p0.getName();
            Comparable c1 = p1.getName();
            if (c0 == null) {
                return dir1;
            }
            return dir1 * c0.compareTo(c1);
        };
    }

    @Override
    protected List<ThumbnailPanelBean> getFiltered() {
        List<ThumbnailPanelBean> panels = getPoolPlotBean().getPanels();
        filterOnExpression(panels);
        return panels;
    }

    protected PoolPlotOptionBean getPoolPlotBean() {
        return WebAdminInterfaceSession.getPoolPlotBean();
    }

    @Override
    protected AbstractRegexFilterBean<ThumbnailPanelBean> getRegexBean() {
        return WebAdminInterfaceSession.getPoolPlotBean();
    }
}
