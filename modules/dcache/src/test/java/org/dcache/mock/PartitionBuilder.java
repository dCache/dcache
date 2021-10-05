/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2021 Deutsches Elektronen-Synchrotron
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
package org.dcache.mock;

import java.util.HashMap;
import java.util.Map;
import org.dcache.poolmanager.Partition;

/**
 * Build a Partition object using the builder pattern.  Unfortunately, since RequestContainerV5
 * accesses field members from Partition directly this class returns a real object rather than a
 * mock object.
 */
public class PartitionBuilder {

    private Map<String, String> arguments = new HashMap<>();

    public static PartitionBuilder aPartition() {
        return new PartitionBuilder();
    }

    private PartitionBuilder() {
    }

    public PartitionBuilder withStageAllowed(boolean enabled) {
        arguments.put("stage-allowed", asArgument(enabled));
        return this;
    }

    public PartitionBuilder withP2pAllowed(boolean enabled) {
        arguments.put("p2p-allowed", asArgument(enabled));
        return this;
    }

    public PartitionBuilder withP2pOnCost(boolean enabled) {
        arguments.put("p2p-oncost", asArgument(enabled));
        return this;
    }

    public PartitionBuilder withP2pForTransfer(boolean enabled) {
        arguments.put("p2p-fortransfer", asArgument(enabled));
        return this;
    }

    public PartitionBuilder withStageOnCost(boolean enabled) {
        arguments.put("stage-oncost", asArgument(enabled));
        return this;
    }

    public Partition build() {
        return new SimplePartition(arguments);
    }

    private String asArgument(boolean enabled) {
        return enabled ? "yes" : "no";
    }
}
