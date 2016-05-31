/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2016 Deutsches Elektronen-Synchrotron
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

package org.dcache.pool.repository;

import java.io.File;
import java.io.PrintWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.util.Map;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.StorageInfo;

import org.dcache.namespace.FileAttribute;
import org.dcache.vehicles.FileAttributes;

public class MetaDataStoreOpenTool
{
    static MetaDataStore createStore(Class<? extends MetaDataStore> clazz,
                                     FileStore fileStore, File poolDir)
        throws NoSuchMethodException, InstantiationException,
               IllegalAccessException, InvocationTargetException
    {
        Constructor<? extends MetaDataStore> constructor =
            clazz.getConstructor(FileStore.class, File.class, Boolean.TYPE);
        return constructor.newInstance(fileStore, poolDir, true);
    }

    public static void main(String[] args)
        throws Exception
    {
        if (args.length != 2) {
            System.err.println("Synopsis: MetaDataStoreOpenTool DIR TYPE");
            System.err.println();
            System.err.println("Where DIR is the pool directory and TYPE is the meta");
            System.err.println("data store class.");
            System.exit(1);
        }

        File poolDir = new File(args[0]);
        FileStore fileStore = new DummyFileStore();
        MetaDataStore metaStore =
            createStore(Class.forName(args[1]).asSubclass(MetaDataStore.class), fileStore, poolDir);
        metaStore.init();

        System.in.read();
    }

}
