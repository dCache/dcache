package org.dcache.pool.statistics;

import org.dcache.pool.repository.RepositoryChannel;

public interface StatsChannel extends RepositoryChannel{

    Statistics getStats();
}
