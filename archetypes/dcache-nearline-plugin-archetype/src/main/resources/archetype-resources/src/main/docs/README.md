Nearline Storage Plugin for dCache
==================================

This is nearline storage plugin for dCache.

Using the plugin with dCache
----------------------------

To use this plugin with dCache, place the directory containing this
file in /usr/local/share/dcache/plugins/ on a dCache pool. Restart
the pool to load the plugin.

To verify that the plugin is loaded, navigate to the pool in the dCache admin
shell and issue the command:

    hsm show providers

The plugin should be listed.

To activate the plugin, create an HSM instance using:

    hsm create osm name ${name} [-key=value]...

