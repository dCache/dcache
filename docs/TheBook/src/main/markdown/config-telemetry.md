THE TELEMETRY SERVICE
=====================

dCache includes an optional telemetry service that allows it to
send information about the instance once an hour to stats.dcache.org
where it is stored in a database and deleted after a week. The
following information is collected:

- site name
- dCache version
- online capacity

Optionally the location can be shared, too.

To enable the telemetry, add the cell to the layout file. The telemetry
needs to be activated explicitly by setting `telemetry.cell.enable` to true.

```
[<domainName>/telemetry]
telemetry.cell.enable=
telemetry.instance.site-name=
```

To send your location you need to configure the following options according
to your location:

```
telemetry.instance.location.latitude=
telemetry.instance.location.longitude=
```
