package org.dcache.tests.util;

import org.junit.Test;

import diskCacheV111.util.FsPath;

import org.dcache.util.PrefixMap;

public class PrefixMapPerformanceTest
{

    protected void populateSmall(PrefixMap<Integer> map)
    {
        map.put(FsPath.create("/pnfs/ndgf.org/data"), 1);
    }

    protected void populateMedium(PrefixMap<Integer> map)
    {
        map.put(FsPath.create("/pnfs/ndgf.org/data"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data/a"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data/b"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data/c"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data/d"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data/e"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data/f"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data/g"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data/h"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data/i"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data/j"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data/k"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data/l"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data/m"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data/n"), 1);
    }

    protected void populateLarge(PrefixMap<Integer> map)
    {
        map.put(FsPath.create("/pnfs/ndgf.org/data"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data/a"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data/b"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data/c"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data/d"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data/e"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data/f"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data/g"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data/h"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data/i"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data/j"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data/k"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data/l"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data/m"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data/n"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data/o"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data/p"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data/q"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data/r"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data/s"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data/t"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data/u"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data/v"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data/w"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data/x"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data/y"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data/z"), 1);
    }

    protected void populateHuge(PrefixMap<Integer> map)
    {
        map.put(FsPath.create("/pnfs/ndgf.org/data"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data/a"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data/b"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data/c"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data/d"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data/e"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data/f"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data/g"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data/h"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data/i"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data/j"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data/k"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data/l"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data/m"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data/n"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data/o"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data/p"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data/q"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data/r"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data/s"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data/t"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data/u"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data/v"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data/w"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data/x"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data/y"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data/z"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data1"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data1/a"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data1/b"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data1/c"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data1/d"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data1/e"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data1/f"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data1/g"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data1/h"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data1/i"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data1/j"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data1/k"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data1/l"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data1/m"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data1/n"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data1/o"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data1/p"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data1/q"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data1/r"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data1/s"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data1/t"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data1/u"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data1/v"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data1/w"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data1/x"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data1/y"), 1);
        map.put(FsPath.create("/pnfs/ndgf.org/data1/z"), 1);
    }

    protected void doLookups(PrefixMap<Integer> map, int n)
    {
        for (int i = 0; i < n; i++) {
            map.get(FsPath.create("/pnfs/ndgf.org/data/atlas/disk/atlasmcdisk/mc08/log/mc08.107041.singlepart_gamma_Et40.simul.log.e342_s462_tid027777/log.027777._00144.job.log.tgz"));
            map.get(FsPath.create("/pnfs/ndgf.org/data/atlas/disk/atlasmcdisk/mc08/log/mc08.105010.J1_pythia_jetjet.simul.log.e344_s479_tid026864/log.026864._08004.job.log.tgz"));
            map.get(FsPath.create("/pnfs/ndgf.org/data/atlas/disk/atlasmcdisk/mc08/log/mc08.107410.singlepart_singlepiplus_logE.digit.log.e342_s439_tid023007/log.023007._24133.job.log.tgz"));
            map.get(FsPath.create("/pnfs/ndgf.org/data/atlas/disk/atlasmcdisk/mc08/log/mc08.107041.singlepart_gamma_Et40.simul.log.e342_s462_tid027777/log.027777._00737.job.log.tgz"));
            map.get(FsPath.create("/pnfs/ndgf.org/data/atlas/disk/atlasmcdisk/mc08/log/mc08.107410.singlepart_singlepiplus_logE.digit.log.e342_s439_tid023007/log.023007._21105.job.log.tgz"));
            map.get(FsPath.create("/pnfs/ndgf.org/data/atlas/disk/atlasmcdisk/mc08/log/mc08.107410.singlepart_singlepiplus_logE.digit.log.e342_s439_tid023007/log.023007._26618.job.log.tgz"));
            map.get(FsPath.create("/pnfs/ndgf.org/data/atlas/disk/atlasmcdisk/mc08/log/mc08.105010.J1_pythia_jetjet.simul.log.e344_s479_tid026864/log.026864._08724.job.log.tgz"));
            map.get(FsPath.create("/pnfs/ndgf.org/data/atlas/disk/atlasmcdisk/mc08/log/mc08.107410.singlepart_singlepiplus_logE.digit.log.e342_s439_tid023007/log.023007._21046.job.log.tgz"));
            map.get(FsPath.create("/pnfs/ndgf.org/data/atlas/disk/atlasmcdisk/mc08/log/mc08.107410.singlepart_singlepiplus_logE.digit.log.e342_s439_tid023007/log.023007._29927.job.log.tgz"));
            map.get(FsPath.create("/pnfs/ndgf.org/data/atlas/disk/atlasmcdisk/mc08/log/mc08.107410.singlepart_singlepiplus_logE.digit.log.e342_s439_tid023007/log.023007._25861.job.log.tgz"));
        }
    }

    @Test
    public void compareSmall()
    {
        PrefixMap<Integer> h = new PrefixMap<>();

        populateSmall(h);

        long t1 = System.currentTimeMillis();
        doLookups(h, 100000);
        long t2 = System.currentTimeMillis();

        System.out.println("1000000 lookups in set with " + h.size()
                           + " entries: " + (t2 - t1) + "ms");
    }

    @Test
    public void compareMedium()
    {
        PrefixMap<Integer> h = new PrefixMap<>();

        populateMedium(h);

        long t1 = System.currentTimeMillis();
        doLookups(h, 100000);
        long t2 = System.currentTimeMillis();

        System.out.println("1000000 lookups in set with " + h.size()
                           + " entries: " + (t2 - t1) + "ms");
    }

    @Test
    public void compareLarge()
    {
        PrefixMap<Integer> h = new PrefixMap<>();

        populateLarge(h);

        long t1 = System.currentTimeMillis();
        doLookups(h, 100000);
        long t2 = System.currentTimeMillis();

        System.out.println("1000000 lookups in set with " + h.size()
                           + " entries: " + (t2 - t1) + "ms");
    }

    @Test
    public void compareHuge()
    {
        PrefixMap<Integer> h = new PrefixMap<>();

        populateHuge(h);

        long t1 = System.currentTimeMillis();
        doLookups(h, 100000);
        long t2 = System.currentTimeMillis();

        System.out.println("1000000 lookups in set with " + h.size()
                           + " entries: " + (t2 - t1) + "ms");
    }
}
