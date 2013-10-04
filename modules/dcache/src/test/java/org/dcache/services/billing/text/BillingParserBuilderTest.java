package org.dcache.services.billing.text;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Map;

import static org.hamcrest.Matchers.hasEntry;
import static org.junit.Assert.assertThat;

public class BillingParserBuilderTest
{
    private static final ImmutableMap<String,String> FORMATS =
            ImmutableMap.of(
                    "mover-info-message", "$date$ [$cellType$:$cellName$:$type$] [$pnfsid$,$filesize$] [$path$] $if(storage)$$storage.storageClass$@$storage.hsm$$else$<Unknown>$endif$ $transferred$ $connectionTime$ $created$ {$protocol$} [$initiator$] {$rc$:\"$message$\"}",
                    "remove-file-info-message", "$date$ [$cellType$:$cellName$:$type$] [$pnfsid$,$filesize$] [$path$] $if(storage)$$storage.storageClass$@$storage.hsm$$else$<Unknown>$endif$ {$rc$:\"$message$\"}",
                    "door-request-info-message", "$date$ [$cellType$:$cellName$:$type$] [\"$owner$\":$uid$:$gid$:$client$] [$pnfsid$,$filesize$] [$path$] $if(storage)$$storage.storageClass$@$storage.hsm$$else$<Unknown>$endif$ $transactionTime$ $queuingTime$ {$rc$:\"$message$\"}",
                    "storage-info-message", "$date$ [$cellType$:$cellName$:$type$] [$pnfsid$,$filesize$] [$path$] $if(storage)$$storage.storageClass$@$storage.hsm$$else$<Unknown>$endif$ $transferTime$ $queuingTime$ {$rc$:\"$message$\"}");

    private BillingParserBuilder builder;

    @Before
    public void setup() throws IOException, URISyntaxException
    {
        builder = new BillingParserBuilder(FORMATS);
    }

    @Test
    public void testDoorRequestInfo()
    {
        Function<String,Map<String,String>> parser = builder.addAllAttributes().buildToMap();
        Map<String, String> values = parser
                .apply("09.30 00:00:00 [door:GFTP-fozzie-<unknown>-1092629@gridftp-fozzieDomain:request] " +
                        "[\"/C=SI/O=SiGNET/O=IJS/OU=F9/CN=Andrej Filipcic\":5002:5000:109.127.252.2] " +
                        "[0000B706DD4045F346F2B90F882B706DA807,0] " +
                        "[/pnfs/ndgf.org/data/atlas/disk/atlasscratchdisk/rucio/user/galhardo/41/d4/user.galhardo.074789._00898.12_SET_2013_FCNCqzl_Pileup_OffsetMu_down.root] " +
                        "atlas:default@osm 21555 0 {0:\"\"}");
        assertThat(values, hasEntry("date", "09.30 00:00:00"));
        assertThat(values, hasEntry("path", "/pnfs/ndgf.org/data/atlas/disk/atlasscratchdisk/rucio/user/galhardo/41/d4/user.galhardo.074789._00898.12_SET_2013_FCNCqzl_Pileup_OffsetMu_down.root"));
        assertThat(values, hasEntry("pnfsid", "0000B706DD4045F346F2B90F882B706DA807"));
        assertThat(values, hasEntry("filesize", "0"));
        assertThat(values, hasEntry("owner", "/C=SI/O=SiGNET/O=IJS/OU=F9/CN=Andrej Filipcic"));
        assertThat(values, hasEntry("uid", "5002"));
        assertThat(values, hasEntry("gid", "5000"));
        assertThat(values, hasEntry("storage.storageClass", "atlas:default"));
        assertThat(values, hasEntry("storage.hsm", "osm"));
        assertThat(values, hasEntry("rc", "0"));
        assertThat(values, hasEntry("message", ""));
    }

    @Test
    public void testDoorRequestInfo2()
    {
        Function<String,Map<String,String>> parser = builder.addAllAttributes().buildToMap();
        Map<String, String> values = parser
                .apply("09.29 09:52:53 [door:WebDAV-fozzie@webdav-fozzieDomain:request] [\"\":-1:-1:180.76.5.139] " +
                        "[0000E36A98139448447187701B80645D6430,0] [/pnfs/ndgf.org/data/behrmann/downloads/upgrade-guide.html] " +
                        "ops:default@osm 300139 0 {10006:\"No connection from client after 300 seconds. Giving up.\"}");
        assertThat(values, hasEntry("date", "09.29 09:52:53"));
        assertThat(values, hasEntry("path", "/pnfs/ndgf.org/data/behrmann/downloads/upgrade-guide.html"));
        assertThat(values, hasEntry("uid", "-1"));
        assertThat(values, hasEntry("gid", "-1"));
    }

    @Test
    public void testMoverInfo()
    {
        Function<String,Map<String,String>> parser = builder.addAllAttributes().buildToMap();
        Map<String, String> values = parser
                .apply("09.30 00:00:00 [pool:hpc2n_umu_se_027:transfer] [00000F2490B964E44D55834C6ED03F8F45DD,256437] " +
                        "[Unknown] atlas:default@osm 256437 2784 true {GFtp-2.0 109.105.124.147 60893} " +
                        "[door:GFTP-gonzo-<unknown>-1106473@gridftp-gonzoDomain:1380491986956-1380491987348] {0:\"\"}");
        assertThat(values, hasEntry("date", "09.30 00:00:00"));
        assertThat(values, hasEntry("path", "Unknown"));
        assertThat(values, hasEntry("pnfsid", "00000F2490B964E44D55834C6ED03F8F45DD"));
        assertThat(values, hasEntry("filesize", "256437"));
        assertThat(values, hasEntry("storage.storageClass", "atlas:default"));
        assertThat(values, hasEntry("storage.hsm", "osm"));
        assertThat(values, hasEntry("rc", "0"));
        assertThat(values, hasEntry("message", ""));
    }

    @Test
    public void testRemoveFileInfo()
    {
        Function<String,Map<String,String>> parser = builder.addAllAttributes().buildToMap();
        Map<String, String> values = parser
                .apply("09.30 00:00:06 [pool:bccs_uib_no_023@nas023_bccs_uib_no_1Domain:remove] [00007A1CD4E8AB8E4F0C81D7054201C424D9,687926] [Unknown] atlas:default@osm {0:\"\"}");
        assertThat(values, hasEntry("date", "09.30 00:00:06"));
        assertThat(values, hasEntry("cellName", "bccs_uib_no_023@nas023_bccs_uib_no_1Domain"));
        assertThat(values, hasEntry("pnfsid", "00007A1CD4E8AB8E4F0C81D7054201C424D9"));
        assertThat(values, hasEntry("filesize", "687926"));
        assertThat(values, hasEntry("storage.storageClass", "atlas:default"));
        assertThat(values, hasEntry("storage.hsm", "osm"));
        assertThat(values, hasEntry("rc", "0"));
        assertThat(values, hasEntry("message", ""));
    }
}
