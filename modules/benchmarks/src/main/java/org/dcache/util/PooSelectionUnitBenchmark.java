/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2022 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dcache.util;

import diskCacheV111.poolManager.PoolPreferenceLevel;
import diskCacheV111.poolManager.PoolSelectionUnit.DirectionType;
import diskCacheV111.poolManager.PoolSelectionUnitV2;
import diskCacheV111.pools.PoolV2Mode;
import diskCacheV111.vehicles.GenericStorageInfo;
import dmg.util.CommandException;
import dmg.util.CommandInterpreter;
import java.util.function.Predicate;
import org.dcache.vehicles.FileAttributes;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@State(Scope.Thread)
@BenchmarkMode(Mode.Throughput)
public class PooSelectionUnitBenchmark {

    private PoolSelectionUnitV2 psu;
    private final Predicate<String> excludeNoPools = p -> false;

    private FileAttributes fileAttributes = FileAttributes.of()
          .storageInfo(GenericStorageInfo.valueOf("a:b@osm", "*"))
          .build();

    @Setup
    public void setUp() throws CommandException {

        psu = new PoolSelectionUnitV2();
        var ci = new CommandInterpreter(psu);

        // storage units

        ci.command(new Args("psu create unit -store  h1:u1@osm"));
        ci.command(new Args("psu create unit -store  h1:u2@osm"));

        ci.command(new Args("psu create unit -store  zeus:u1@osm"));
        ci.command(new Args("psu create unit -store  zeus:u2@osm"));

        ci.command(new Args("psu create unit -store  flc:u1@osm"));
        ci.command(new Args("psu create unit -store  flc:u2@osm"));

        ci.command(new Args("psu create unit -store  hermes:u1@osm"));
        ci.command(new Args("psu create unit -store  hermes:u2@osm"));

        ci.command(new Args("psu create unit -store  herab:u1@osm"));
        ci.command(new Args("psu create unit -store  herab:u2@osm"));

        ci.command(new Args("psu create unit -store  *@*"));

        // store unit groups

        ci.command(new Args("psu create ugroup all-h1"));
        ci.command(new Args("psu create ugroup all-zeus"));
        ci.command(new Args("psu create ugroup all-flc"));
        ci.command(new Args("psu create ugroup all-hermes"));
        ci.command(new Args("psu create ugroup all-herab"));
        ci.command(new Args("psu create ugroup all-hera"));
        ci.command(new Args("psu create ugroup all"));

        // populate ugroups

        ci.command(new Args("psu addto ugroup all-h1 h1:u1@osm"));
        ci.command(new Args("psu addto ugroup all-h1 h1:u2@osm"));

        ci.command(new Args("psu addto ugroup all-h1 zeus:u1@osm"));
        ci.command(new Args("psu addto ugroup all-h1 zeus:u2@osm"));

        ci.command(new Args("psu addto ugroup all-h1 flc:u1@osm"));
        ci.command(new Args("psu addto ugroup all-h1 flc:u2@osm"));

        ci.command(new Args("psu addto ugroup all-h1 hermes:u1@osm"));
        ci.command(new Args("psu addto ugroup all-h1 hermes:u2@osm"));

        ci.command(new Args("psu addto ugroup all-h1 herab:u1@osm"));
        ci.command(new Args("psu addto ugroup all-h1 herab:u2@osm"));

        ci.command(new Args("psu addto ugroup all h1:u1@osm"));
        ci.command(new Args("psu addto ugroup all h1:u2@osm"));
        ci.command(new Args("psu addto ugroup all zeus:u1@osm"));
        ci.command(new Args("psu addto ugroup all zeus:u2@osm"));
        ci.command(new Args("psu addto ugroup all flc:u1@osm"));
        ci.command(new Args("psu addto ugroup all flc:u2@osm"));
        ci.command(new Args("psu addto ugroup all hermes:u1@osm"));
        ci.command(new Args("psu addto ugroup all hermes:u2@osm"));
        ci.command(new Args("psu addto ugroup all herab:u1@osm"));
        ci.command(new Args("psu addto ugroup all herab:u2@osm"));
        ci.command(new Args("psu addto ugroup all *@*"));

        ci.command(new Args("psu addto ugroup all-hera h1:u1@osm"));
        ci.command(new Args("psu addto ugroup all-hera h1:u2@osm"));
        ci.command(new Args("psu addto ugroup all-hera zeus:u1@osm"));
        ci.command(new Args("psu addto ugroup all-hera zeus:u2@osm"));
        ci.command(new Args("psu addto ugroup all-hera hermes:u1@osm"));
        ci.command(new Args("psu addto ugroup all-hera hermes:u2@osm"));
        ci.command(new Args("psu addto ugroup all-hera herab:u1@osm"));
        ci.command(new Args("psu addto ugroup all-hera herab:u2@osm"));

        // network
        ci.command(new Args("psu create unit -net    131.169.0.0/255.255.0.0"));
        ci.command(new Args("psu create unit -net    0.0.0.0/0.0.0.0"));
        ci.command(new Args("psu create unit -net    2001:638:700::0/48"));
        ci.command(new Args("psu create unit -net    ::/0"));

        // net groups
        ci.command(new Args("psu create ugroup intern"));
        ci.command(new Args("psu create ugroup extern"));

        // populate net groups
        ci.command(new Args("psu addto ugroup intern 131.169.0.0/255.255.0.0"));
        ci.command(new Args("psu addto ugroup extern 0.0.0.0/0.0.0.0"));
        ci.command(new Args("psu addto ugroup intern 2001:638:700::0/48"));
        ci.command(new Args("psu addto ugroup extern ::/0"));

        // pools
        ci.command(new Args("psu create pool h1-read"));
        psu.getPool("h1-read").setPoolMode(new PoolV2Mode(PoolV2Mode.ENABLED));
        ci.command(new Args("psu create pool h1-write"));
        psu.getPool("h1-write").setPoolMode(new PoolV2Mode(PoolV2Mode.ENABLED));

        ci.command(new Args("psu create pool zeus-read"));
        psu.getPool("zeus-read").setPoolMode(new PoolV2Mode(PoolV2Mode.ENABLED));
        ci.command(new Args("psu create pool zeus-write"));
        psu.getPool("zeus-write").setPoolMode(new PoolV2Mode(PoolV2Mode.ENABLED));

        ci.command(new Args("psu create pool flc-read"));
        psu.getPool("flc-read").setPoolMode(new PoolV2Mode(PoolV2Mode.ENABLED));
        ci.command(new Args("psu create pool flc-write"));
        psu.getPool("flc-write").setPoolMode(new PoolV2Mode(PoolV2Mode.ENABLED));

        ci.command(new Args("psu create pool hermes-read"));
        psu.getPool("hermes-read").setPoolMode(new PoolV2Mode(PoolV2Mode.ENABLED));
        ci.command(new Args("psu create pool hermes-write"));
        psu.getPool("hermes-write").setPoolMode(new PoolV2Mode(PoolV2Mode.ENABLED));

        ci.command(new Args("psu create pool herab-read"));
        psu.getPool("herab-read").setPoolMode(new PoolV2Mode(PoolV2Mode.ENABLED));
        ci.command(new Args("psu create pool herab-write"));
        psu.getPool("herab-write").setPoolMode(new PoolV2Mode(PoolV2Mode.ENABLED));

        ci.command(new Args("psu create pool default-read"));
        psu.getPool("default-read").setPoolMode(new PoolV2Mode(PoolV2Mode.ENABLED));
        ci.command(new Args("psu create pool default-write"));
        psu.getPool("default-write").setPoolMode(new PoolV2Mode(PoolV2Mode.ENABLED));

        // pool groups

        ci.command(new Args("psu create pgroup h1-read-pools"));
        ci.command(new Args("psu create pgroup h1-write-pools"));

        ci.command(new Args("psu create pgroup zeus-read-pools"));
        ci.command(new Args("psu create pgroup zeus-write-pools"));

        ci.command(new Args("psu create pgroup flc-read-pools"));
        ci.command(new Args("psu create pgroup flc-write-pools"));

        ci.command(new Args("psu create pgroup hermes-read-pools"));
        ci.command(new Args("psu create pgroup hermes-write-pools"));

        ci.command(new Args("psu create pgroup herab-read-pools"));
        ci.command(new Args("psu create pgroup herab-write-pools"));

        ci.command(new Args("psu create pgroup default-read-pools"));
        ci.command(new Args("psu create pgroup default-write-pools"));

        // Populate pool groups

        ci.command(new Args("psu addto pgroup h1-read-pools h1-read"));
        ci.command(new Args("psu addto pgroup h1-write-pools h1-write"));

        ci.command(new Args("psu addto pgroup zeus-read-pools zeus-read"));
        ci.command(new Args("psu addto pgroup zeus-write-pools zeus-write"));

        ci.command(new Args("psu addto pgroup flc-read-pools flc-read"));
        ci.command(new Args("psu addto pgroup flc-write-pools flc-write"));

        ci.command(new Args("psu addto pgroup hermes-read-pools hermes-read"));
        ci.command(new Args("psu addto pgroup hermes-write-pools hermes-write"));

        ci.command(new Args("psu addto pgroup herab-read-pools herab-read"));
        ci.command(new Args("psu addto pgroup herab-write-pools herab-write"));

        ci.command(new Args("psu addto pgroup default-read-pools default-read"));
        ci.command(new Args("psu addto pgroup default-write-pools default-write"));

        // links

        ci.command(new Args("psu create link h1-read-link all-h1 intern"));
        ci.command(new Args("psu create link h1-write-link all-h1 intern"));

        ci.command(new Args("psu create link zeus-read-link all-zeus intern"));
        ci.command(new Args("psu create link zeus-write-link all-zeus intern"));

        ci.command(new Args("psu create link flc-read-link all-flc intern"));
        ci.command(new Args("psu create link flc-write-link all-flc intern"));

        ci.command(new Args("psu create link hermes-read-link all-hermes intern"));
        ci.command(new Args("psu create link hermes-write-link all-hermes intern"));

        ci.command(new Args("psu create link herab-read-link all-herab intern"));
        ci.command(new Args("psu create link herab-write-link all-herab intern"));

        ci.command(new Args("psu create link default-read-link-in all intern"));
        ci.command(new Args("psu create link default-write-link-in all intern"));

        ci.command(new Args("psu create link default-read-link-ex all extern"));
        ci.command(new Args("psu create link default-write-link-ex all extern"));

        // link preferences
        /*
         * schema here is the classic case:
         *   write into write-pools
         * 	 read from read-pools
         * 	 fallback: default-pools
         */

        ci.command(new Args(
              "psu set link h1-read-link         -readpref=20 -writepref=0 -cachepref=20"));
        ci.command(new Args(
              "psu set link zeus-read-link       -readpref=20 -writepref=0 -cachepref=20"));
        ci.command(new Args(
              "psu set link flc-read-link        -readpref=20 -writepref=0 -cachepref=20"));
        ci.command(new Args(
              "psu set link hermes-read-link     -readpref=20 -writepref=0 -cachepref=20"));
        ci.command(new Args(
              "psu set link herab-read-link      -readpref=20 -writepref=0 -cachepref=20"));
        ci.command(new Args(
              "psu set link default-read-link-in -readpref=1  -writepref=0 -cachepref=20"));
        ci.command(new Args(
              "psu set link default-read-link-ex -readpref=1  -writepref=0 -cachepref=20"));

        ci.command(new Args(
              "psu set link h1-write-link         -writepref=20 -readpref=0 -cachepref=0"));
        ci.command(new Args(
              "psu set link zeus-write-link       -writepref=20 -readpref=0 -cachepref=0"));
        ci.command(new Args(
              "psu set link flc-write-link        -writepref=20 -readpref=0 -cachepref=0"));
        ci.command(new Args(
              "psu set link hermes-write-link     -writepref=20 -readpref=0 -cachepref=0"));
        ci.command(new Args(
              "psu set link herab-write-link      -writepref=20 -readpref=0 -cachepref=0"));
        ci.command(new Args(
              "psu set link default-write-link-in -writepref=1  -readpref=0 -cachepref=0"));
        ci.command(new Args(
              "psu set link default-write-link-ex -writepref=1  -readpref=0 -cachepref=0"));

        // assign pool groups to links
        ci.command(new Args("psu addto link h1-read-link h1-read-pools"));
        ci.command(new Args("psu addto link h1-write-link h1-write-pools"));

        ci.command(new Args("psu addto link zeus-read-link zeus-read-pools"));
        ci.command(new Args("psu addto link zeus-write-link zeus-write-pools"));

        ci.command(new Args("psu addto link flc-read-link flc-read-pools"));
        ci.command(new Args("psu addto link flc-write-link flc-write-pools"));

        ci.command(new Args("psu addto link hermes-read-link hermes-read-pools"));
        ci.command(new Args("psu addto link hermes-write-link hermes-write-pools"));

        ci.command(new Args("psu addto link herab-read-link herab-read-pools"));
        ci.command(new Args("psu addto link herab-write-link herab-write-pools"));

        ci.command(new Args("psu addto link default-read-link-ex default-read-pools"));
        ci.command(new Args("psu addto link default-write-link-ex default-write-pools"));

        ci.command(new Args("psu addto link default-read-link-in default-read-pools"));
        ci.command(new Args("psu addto link default-write-link-in default-write-pools"));

        ci.command("psu set allpoolsactive on");

    }


    @Benchmark
    @Threads(value = 16)
    public int match() {

        PoolPreferenceLevel[] preference = psu.match(
              DirectionType.WRITE,  // operation
              "131.169.214.149", // net unit
              null,  // protocol
              fileAttributes,
              null, // linkGroup
              excludeNoPools);

        return preference.length;
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
              .include(PooSelectionUnitBenchmark.class.getSimpleName())
              .build();

        new Runner(opt).run();
    }

}
