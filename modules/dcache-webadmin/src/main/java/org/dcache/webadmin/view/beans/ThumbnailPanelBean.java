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
package org.dcache.webadmin.view.beans;

import org.apache.wicket.core.util.resource.UrlResourceStream;
import org.apache.wicket.markup.html.image.Image;
import org.apache.wicket.markup.html.link.Link;
import org.apache.wicket.markup.html.link.PopupSettings;
import org.apache.wicket.markup.html.link.ResourceLink;
import org.apache.wicket.request.resource.ResourceStreamResource;
import org.apache.wicket.util.resource.FileResourceStream;
import org.apache.wicket.util.time.Duration;

import java.io.File;
import java.io.Serializable;

import org.dcache.util.IRegexFilterable;
import org.dcache.webadmin.model.dataaccess.util.rrd4j.RrdSettings;

/**
 * Data abstraction for the panel displayed in the pool queues grid view table.
 *
 * @author arossi
 */
public class ThumbnailPanelBean implements IRegexFilterable,
                Comparable<ThumbnailPanelBean>, Serializable {
    private static final long serialVersionUID = 1264628048199749823L;

    private static final String PLACEHOLDER
        = "org/dcache/webadmin/view/pages/poolqueues/blank.jpg";

    private final String name;
    private final Link<?> link;

    public ThumbnailPanelBean() {
        name = "";
        UrlResourceStream stream
            = new UrlResourceStream(Thread.currentThread().getContextClassLoader()
                                          .getResource(PLACEHOLDER));
        ResourceStreamResource resource = new ResourceStreamResource(stream);
        resource.setCacheDuration(Duration.NONE);
        Image image = new Image("thumbnail", resource);
        link = new Link<String>("plotlink") {
            private static final long serialVersionUID = 4245101719065647956L;

            @Override
            public void onClick() {
            }
        };
        link.add(image);
    }

    public ThumbnailPanelBean(File file, int height, int width) {
        String name = file.getName();
        int end = name.indexOf(RrdSettings.FILE_SUFFIX);
        this.name = name.substring(0, end);
        ResourceStreamResource resource
                        = new ResourceStreamResource(new FileResourceStream(file));
        resource.setCacheDuration(Duration.NONE);
        Image image = new Image("thumbnail", resource);
        PopupSettings popupSettings = new PopupSettings(PopupSettings.RESIZABLE
                        | PopupSettings.SCROLLBARS).setHeight(height).setWidth(
                        width);

        ResourceLink link = new ResourceLink("plotlink", resource);
        link.setPopupSettings(popupSettings);
        link.add(image);
        this.link = link;
    }

    @Override
    public int compareTo(ThumbnailPanelBean arg0) {
        if (arg0 == null) {
            return 1;
        }
        return name.compareTo(arg0.name);
    }

    public Link<?> getLink() {
        return link;
    }

    public String getName() {
        return name;
    }

    @Override
    public String toFilterableString() {
        return name;
    }

    public String toString() { return name + ":" + link.get("plotlink"); }
}
