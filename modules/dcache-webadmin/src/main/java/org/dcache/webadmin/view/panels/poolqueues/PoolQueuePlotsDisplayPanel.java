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
package org.dcache.webadmin.view.panels.poolqueues;

import org.apache.wicket.core.util.resource.UrlResourceStream;
import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.image.Image;
import org.apache.wicket.markup.html.link.Link;
import org.apache.wicket.markup.html.panel.Panel;
import org.apache.wicket.markup.repeater.Item;
import org.apache.wicket.markup.repeater.data.GridView;
import org.apache.wicket.model.IModel;
import org.apache.wicket.model.PropertyModel;
import org.apache.wicket.request.resource.IResource;
import org.apache.wicket.request.resource.ResourceStreamResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;

import org.dcache.webadmin.controller.util.ThumbnailPanelProvider;
import org.dcache.webadmin.controller.util.ThumbnailPanelProvider.Level;
import org.dcache.webadmin.view.beans.ThumbnailPanelBean;

import static org.dcache.webadmin.controller.util.ThumbnailPanelProvider.Level.POOLS_OF_GROUP;
import static org.dcache.webadmin.controller.util.ThumbnailPanelProvider.Level.POOL_GROUPS;

/**
 * Panel which displays the grid view of the thumbnail links to each pool queue
 * histogram.
 */
public class PoolQueuePlotsDisplayPanel extends Panel {
    private static final long   serialVersionUID = 5701767178955064955L;
    private static final Logger LOGGER
                    = LoggerFactory.getLogger(PoolQueuePlotsDisplayPanel.class);

    private static final String VIEW_ID          = "PoolQueuePlotsGridView";
    private static final String UP_PATH
                    = "org/dcache/webadmin/view/pages/poolqueues/up.png";
    private static final String DOWN_PATH
                    = "org/dcache/webadmin/view/pages/poolqueues/down.png";

    enum Direction {
        UP, DOWN
    }

    private final ThumbnailPanelProvider       provider;
    private final GridView<ThumbnailPanelBean> gridView;
    private final Label                        levelTitle;

    public PoolQueuePlotsDisplayPanel(String id,
                                      ThumbnailPanelProvider provider) {
        super(id);
        this.provider = provider;

        levelTitle = new Label("level", new PropertyModel<String>(provider,
                                                                  "levelTitle"));
        add(levelTitle);
        gridView = createGridView();
        add(gridView);

        provider.refresh();
    }

    public void refresh() {
        provider.refresh();
    }

    private void addDownLink(Item item, String name, Level level) {
        item.add(getLabel(level, Direction.DOWN));

        Link<?> link = new Link<String>("downLink") {
            private static final long serialVersionUID = 8376628458228582315L;

            @Override
            public void onClick() {
                switch (level) {
                    case ALL_POOLS:
                        provider.setLevel(POOL_GROUPS);
                        provider.setName(null);
                        break;
                    case POOL_GROUPS:
                        provider.setLevel(POOLS_OF_GROUP);
                        provider.setName(name);
                        break;
                    case POOLS_OF_GROUP:
                    default:
                        provider.setLevel(Level.ALL_POOLS);
                        provider.setName(null);
                        break;
                }
                refresh();
            }
        };

        link.add(getImage(Direction.DOWN));
        item.add(link);
    }

    private void addUpLink(Item item, final Level level) {
        item.add(getLabel(level, Direction.UP));

        Link<?> link = new Link<String>("upLink") {
            private static final long serialVersionUID = -1077631840167339923L;

            @Override
            public void onClick() {
                switch (level) {
                    case POOL_GROUPS:
                        provider.setLevel(Level.ALL_POOLS);
                        provider.setName(null);
                        break;
                    case POOLS_OF_GROUP:
                        provider.setLevel(POOL_GROUPS);
                        provider.setName(null);
                        break;
                    case ALL_POOLS:
                    default:
                        break;
                }
                refresh();
            }
        };

        link.add(getImage(Direction.UP));
        item.add(link);
    }

    private GridView<ThumbnailPanelBean> createGridView() {
        return new GridView<ThumbnailPanelBean>(VIEW_ID, provider) {
            private static final long serialVersionUID = -7245101719065647956L;

            @Override
            protected void onBeforeRender() {
                setColumns(provider.getNumCols());
                super.onBeforeRender();
            }

            @Override
            protected void populateEmptyItem(Item item) {
                addLinks(item, new ThumbnailPanelBean());
            }

            @Override
            protected void populateItem(Item<ThumbnailPanelBean> item) {
                addLinks(item, item.getModelObject());
            }

            private void addLinks(Item<ThumbnailPanelBean> item,
                                        ThumbnailPanelBean bean) {
                Level level = provider.getLevel();
                String name = bean.getName();
                addUpLink(item, level);
                item.add(bean.getLink());
                item.add(new Label("name", name));
                addDownLink(item, name, level);
            }
        };
    }

    private Image getImage(Direction direction) {
        String label;
        String path;

        switch (direction) {
            case UP:
                label = "up";
                path = UP_PATH;
                break;
            case DOWN:
                label = "down";
                path = DOWN_PATH;
                break;
            default:
                throw new IllegalArgumentException("Unknown direction "
                                                                   + direction);
        }

        URL url = Thread.currentThread().getContextClassLoader()
                        .getResource(path);
        if (url == null) {
            LOGGER.error("Could not find resource {}.", path);
            return new Image(label, (IModel)null);
        }

        UrlResourceStream stream = new UrlResourceStream(url);
        IResource resource = new ResourceStreamResource(stream);
        return new Image(label, resource);
    }

    private Label getLabel(Level level, Direction direction) {
        switch (direction) {
            case UP:
                switch (level) {
                    case POOL_GROUPS:
                        return new Label("upLinkTitle", "pool summary");
                    case POOLS_OF_GROUP:
                        return new Label("upLinkTitle", "pool groups");
                    case ALL_POOLS:
                    default:
                        return new Label("upLinkTitle", null);
                }
            case DOWN:
                switch (level) {
                    case ALL_POOLS:
                        return new Label("downLinkTitle", "pool groups");
                    case POOL_GROUPS:
                        return new Label("downLinkTitle", "pools");
                    case POOLS_OF_GROUP:
                    default:
                        return new Label("downLinkTitle", "pool summary");
                }
            default:
                throw new IllegalArgumentException("Unknown direction "
                                                                   + direction);
        }
    }
}
