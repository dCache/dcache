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
package org.dcache.services.billing.plots.jaida;

import hep.aida.IAnalysisFactory;
import hep.aida.IHistogram1D;
import hep.aida.IHistogramFactory;
import hep.aida.ITree;

import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.dcache.services.billing.histograms.AbstractTimeFrameHistogramFactory;
import org.dcache.services.billing.histograms.TimeFrame;
import org.dcache.services.billing.histograms.config.HistogramWrapper;
import org.dcache.services.billing.histograms.data.IHistogramData;
import org.dcache.services.billing.histograms.data.TimeFrameHistogramData;
import org.dcache.services.billing.plots.util.ITimeFramePlot;
import org.dcache.services.billing.plots.util.ITimeFramePlotGenerator;
import org.dcache.services.billing.plots.util.PlotGridPosition;

/**
 * Implements both histogram factory and plot generation interfaces for JAIDA.
 *
 * @author arossi
 */
public final class JaidaTimeFramePlotGenerator extends
                AbstractTimeFrameHistogramFactory<IHistogram1D> implements
                ITimeFramePlotGenerator<IHistogram1D> {
    private static final String JAIDA_PROPERTIES
        = "org/dcache/services/billing/plot/jaida.properties";
    private IAnalysisFactory af;
    private IHistogramFactory factory;
    private ITree tree;

    @Override
    public IHistogram1D create(HistogramWrapper<IHistogram1D> configuration) {
        TimeFrame timeFrame = configuration.getTimeFrame();
        String title = configuration.getTitle();
        double del = timeFrame.getBinWidth() / 2.0;
        IHistogram1D histogram = factory.createHistogram1D(title,
                        timeFrame.getBinCount(),
                        (double)TimeUnit.MILLISECONDS.toSeconds(timeFrame.getLowTime()) - del,
                        (double)TimeUnit.MILLISECONDS.toSeconds(timeFrame.getHighTime()) - del);

        TimeFrameHistogramData data = configuration.getData();
        String field = data.getField();
        Double dfactor = data.getDfactor();
        Collection<IHistogramData> binData = data.getData();
        if (field != null) {
            double df = dfactor == null ? 1.0 : dfactor;
            for (IHistogramData d : binData) {
                 histogram.fill(TimeUnit.MILLISECONDS.toSeconds(d.timestamp().getTime()),
                                d.data().get(field) / df);
            }
        } else {
            for (IHistogramData d : binData) {
                histogram.fill(TimeUnit.MILLISECONDS.toSeconds(d.timestamp().getTime()));
            }
        }

        return histogram;
    }

    @Override
    public ITimeFramePlot createPlot(String plotName, String[] subtitles,
                    PlotGridPosition position,
                    List<HistogramWrapper<IHistogram1D>> histogram) {
        JaidaTimeFramePlot plot = new JaidaTimeFramePlot(af, tree, plotName,
                        subtitles, properties);
        for (HistogramWrapper<IHistogram1D> h : histogram) {
            h.setHistogram(create(h));
            plot.addHistogram(position, h);
        }
        return plot;
    }

    @Override
    public String getPropertiesResource() {
        return JAIDA_PROPERTIES;
    }

    @Override
    public void initialize(Properties properties) {
        setProperties(properties);
        af = IAnalysisFactory.create();
        tree = af.createTreeFactory().create();
        factory = af.createHistogramFactory(tree);
    }
}
