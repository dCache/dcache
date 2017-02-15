package org.dcache.util;

import liquibase.Contexts;
import liquibase.LabelExpression;
import liquibase.Liquibase;
import liquibase.changelog.ChangeSet;
import liquibase.exception.LiquibaseException;
import liquibase.exception.MigrationFailedException;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.logging.LogFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternUtils;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Vector;
import java.util.jar.Manifest;

public class SpringLiquibase
    extends liquibase.integration.spring.SpringLiquibase
{
    private static final Logger _log =
        LoggerFactory.getLogger(SpringLiquibase.class);

    private boolean _shouldUpdate = true;

    public void setShouldUpdate(boolean shouldUpdate)
    {
        _shouldUpdate = shouldUpdate;
    }

    public boolean getShouldUpdate()
    {
        return _shouldUpdate;
    }

    @Override
    public void afterPropertiesSet()
    {
        getJdbcTemplate().execute(new SchemaMigrator());
    }

    protected JdbcTemplate getJdbcTemplate()
    {
        return new JdbcTemplate(getDataSource());
    }

    protected SpringResourceOpener createResourceOpener() {
        return new DcacheSpringResourceOpener(getChangeLog());
    }

    protected class DcacheSpringResourceOpener extends SpringResourceOpener
    {
        public DcacheSpringResourceOpener(String parentFile)
        {
            super(parentFile);
        }

        // Workaround for https://liquibase.jira.com/browse/CORE-3001
        @Override
        protected void init()
        {
            try {
                Enumeration<URL> baseUrls;
                ClassLoader classLoader = toClassLoader();
                if (classLoader != null) {
                    if (classLoader instanceof URLClassLoader) {
                        baseUrls = new Vector<URL>(Arrays.asList(((URLClassLoader) classLoader).getURLs())).elements();

                        while (baseUrls.hasMoreElements()) {
                            addRootPath(baseUrls.nextElement());
                        }
                    }

                    baseUrls = classLoader.getResources("");

                    while (baseUrls.hasMoreElements()) {
                        addRootPath(baseUrls.nextElement());
                    }
                }
            } catch (IOException e) {
                throw new UnexpectedLiquibaseException(e);
            }
            try {
                Resource[] resources = ResourcePatternUtils.getResourcePatternResolver(getResourceLoader()).getResources("");
				if (resources.length == 0 || resources.length == 1 && !resources[0].exists()) { //sometimes not able to look up by empty string, try all the liquibase packages
					Set<String> liquibasePackages = new HashSet<>();
					for (Resource manifest : ResourcePatternUtils.getResourcePatternResolver(getResourceLoader()).getResources(
                            ResourcePatternResolver.CLASSPATH_ALL_URL_PREFIX + "META-INF/MANIFEST.MF")) {
						if (manifest.exists()) {
							InputStream inputStream = null;
							try {
								inputStream = manifest.getInputStream();
								Manifest manifestObj = new Manifest(inputStream);
								String attr = manifestObj.getMainAttributes().getValue("Liquibase-Package");
								if (attr != null) {
                                    String packages = "\\s*,\\s*";
                                    for (String fullPackage : attr.split(packages)) {
                                        liquibasePackages.add(fullPackage.split("\\.")[0]);
                                    }
                                }
							} finally {
								if (inputStream != null) {
									inputStream.close();
								}
							}
						}
					}

                    if (liquibasePackages.size() == 0) {
                        LogFactory.getInstance().getLog().warning("No Liquibase-Packages entry found in MANIFEST.MF. Using fallback of entire 'liquibase' package");
                        liquibasePackages.add("liquibase");
                    }

                    for (String foundPackage : liquibasePackages) {
						resources = ResourcePatternUtils.getResourcePatternResolver(getResourceLoader()).getResources(foundPackage);
						for (Resource res : resources) {
							addRootPath(res.getURL());
						}
					}
				} else {
					for (Resource res : resources) {
						addRootPath(res.getURL());
					}
				}
            } catch (IOException e) {
                LogFactory.getInstance().getLog().warning("Error initializing SpringLiquibase", e);
            }
        }
    }

    private class SchemaMigrator implements ConnectionCallback<Void>
    {
        @Override
        public Void doInConnection(Connection c)
            throws SQLException, DataAccessException
        {
            try {
                Liquibase liquibase = createLiquibase(c);
                try {
                    Contexts contexts = new Contexts(getContexts());
                    LabelExpression labels = new LabelExpression(getLabels());
                    if (_shouldUpdate) {
                        liquibase.update(contexts, labels);
                    } else {
                        List<ChangeSet> changeSets =
                            liquibase.listUnrunChangeSets(contexts, labels);
                        if (!changeSets.isEmpty()) {
                            throw new MigrationFailedException(changeSets.get(0),
                                                               "Automatic schema migration is disabled. Please apply missing changes.");
                        }
                    }
                } finally {
                    liquibase.forceReleaseLocks();
                }
            } catch (LiquibaseException e) {
                throw new SQLException("Schema migration failed", e);
            }
            return null;
        }
    }
}
