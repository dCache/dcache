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
package org.dcache.services.billing.histograms;

import java.util.Properties;

import org.dcache.services.billing.histograms.TimeFrame.BinType;
import org.dcache.services.billing.histograms.config.HistogramWrapper;
import org.dcache.services.billing.histograms.data.TimeFrameHistogramData;
import org.dcache.services.billing.histograms.data.TimeFrameHistogramData.HistogramDataType;

/**
 * Provides a generic method for obtaining a {@link HistogramWrapper} from the
 * time frame and data specifications.
 *
 * @author arossi
 */
public abstract class AbstractTimeFrameHistogramFactory<H> implements
                ITimeFrameHistogramFactory<H> {

    protected Properties properties;

    @Override
    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    @Override
    public HistogramWrapper<H> getConfigurationFor(TimeFrame timeframe,
                    TimeFrameHistogramData data, Style style, String scaling) {
        HistogramDataType type = data.getType();
        HistogramWrapper<H> config = new HistogramWrapper<>();
        config.setStyle(style);
        config.setScaling(scaling);
        config.setData(data);
        config.setTimeFrame(timeframe);
        config.setXLabel(getProperty(TimeFrameHistogramProperties.LABEL_X_AXIS));
        switch (type) {
            case BYTES_UPLOADED:
                config.setTitle(getProperty(TimeFrameHistogramProperties.LABEL_DC_WR));
                config.setColor(getProperty(TimeFrameHistogramProperties.COLOR_DC));
                configureBytes(config);
                break;
            case BYTES_DOWNLOADED:
                config.setTitle(getProperty(TimeFrameHistogramProperties.LABEL_DC_RD));
                config.setColor(getProperty(TimeFrameHistogramProperties.COLOR_DC));
                configureBytes(config);
                break;
            case BYTES_P2P:
                config.setTitle(getProperty(TimeFrameHistogramProperties.LABEL_DC_P2P));
                config.setColor(getProperty(TimeFrameHistogramProperties.COLOR_DC));
                configureBytes(config);
                break;
            case BYTES_STORED:
                config.setTitle(getProperty(TimeFrameHistogramProperties.LABEL_HSM_WR));
                config.setColor(getProperty(TimeFrameHistogramProperties.COLOR_HSM));
                configureBytes(config);
                break;
            case BYTES_RESTORED:
                config.setTitle(getProperty(TimeFrameHistogramProperties.LABEL_HSM_RD));
                config.setColor(getProperty(TimeFrameHistogramProperties.COLOR_HSM));
                configureBytes(config);
                break;
            case TRANSFERS_UPLOADED:
                config.setTitle(getProperty(TimeFrameHistogramProperties.LABEL_DC_WR));
                config.setColor(getProperty(TimeFrameHistogramProperties.COLOR_DC));
                configureTransfers(config);
                break;
            case TRANSFERS_DOWNLOADED:
                config.setTitle(getProperty(TimeFrameHistogramProperties.LABEL_DC_RD));
                config.setColor(getProperty(TimeFrameHistogramProperties.COLOR_DC));
                configureTransfers(config);
                break;
            case TRANSFERS_P2P:
                config.setTitle(getProperty(TimeFrameHistogramProperties.LABEL_DC_P2P));
                config.setColor(getProperty(TimeFrameHistogramProperties.COLOR_DC));
                configureTransfers(config);
                break;
            case TRANSFERS_STORED:
                config.setTitle(getProperty(TimeFrameHistogramProperties.LABEL_HSM_WR));
                config.setColor(getProperty(TimeFrameHistogramProperties.COLOR_HSM));
                configureTransfers(config);
                break;
            case TRANSFERS_RESTORED:
                config.setTitle(getProperty(TimeFrameHistogramProperties.LABEL_HSM_RD));
                config.setColor(getProperty(TimeFrameHistogramProperties.COLOR_HSM));
                configureTransfers(config);
                break;
            case TIME_AVG:
                config.setTitle(getProperty(TimeFrameHistogramProperties.LABEL_AVG));
                config.setColor(getProperty(TimeFrameHistogramProperties.COLOR_AVG));
                configureConnectTime(config);
                break;
            case TIME_MAX:
                config.setTitle(getProperty(TimeFrameHistogramProperties.LABEL_MAX));
                config.setColor(getProperty(TimeFrameHistogramProperties.COLOR_MAX));
                configureConnectTime(config);
                break;
            case TIME_MIN:
                config.setTitle(getProperty(TimeFrameHistogramProperties.LABEL_MIN));
                config.setColor(getProperty(TimeFrameHistogramProperties.COLOR_MIN));
                configureConnectTime(config);
                break;
            case CACHED:
                config.setTitle(getProperty(TimeFrameHistogramProperties.LABEL_CACHED));
                config.setColor(getProperty(TimeFrameHistogramProperties.COLOR_CACHED));
                configureHitMiss(config);
                break;
            case NOT_CACHED:
                config.setTitle(getProperty(TimeFrameHistogramProperties.LABEL_NCACHED));
                config.setColor(getProperty(TimeFrameHistogramProperties.COLOR_NCACHED));
                configureHitMiss(config);
                break;
        }

        return config;
    }

    private void configureBytes(HistogramWrapper<?> config) {
        if (BinType.HOUR == config.getTimeFrame().getTimebin()) {
            config.setYLabel(getProperty(TimeFrameHistogramProperties.LABEL_Y_AXIS_GBYTES_HR));
        } else {
            config.setYLabel(getProperty(TimeFrameHistogramProperties.LABEL_Y_AXIS_GBYTES_DY));
        }
    }

    private void configureConnectTime(HistogramWrapper<?> config) {
        config.setYLabel(getProperty(TimeFrameHistogramProperties.LABEL_Y_AXIS_CONNECT));
    }

    private void configureHitMiss(HistogramWrapper<?> config) {
        config.setYLabel(getProperty(TimeFrameHistogramProperties.LABEL_Y_AXIS_HITS_HR));
    }

    private void configureTransfers(HistogramWrapper<?> config) {
        if (BinType.HOUR == config.getTimeFrame().getTimebin()) {
            config.setYLabel(getProperty(TimeFrameHistogramProperties.LABEL_Y_AXIS_TRANSF_HR));
        } else {
            config.setYLabel(getProperty(TimeFrameHistogramProperties.LABEL_Y_AXIS_TRANSF_DY));
        }
    }

    private String getProperty(String name) {
        return properties.getProperty(name);
    }
}
