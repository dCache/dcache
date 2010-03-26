/**
 * This script contains the functions to process the plots descriptions
 *
 */

/**
 * Returns all IDs from the configuration
 */
function getIds()
{
  var ids = new Array();
  // Get all IDs
  var ii = xxo.plot.@id;
  for (var jj in ii) {
    ids[jj] = ii[jj];
  }
  return ids
}

/**
 * Returns the datasources for given id
 */
function getPlotDataSource(id)
{
  var dss = new Array();
  var pp = xxo.plot.(@id==id);
  // Get all DSs
  var ss = pp.datasource;
  for (var jj in ss) {
    dss[jj] = [ss[jj].@table, ss[jj]];
  }
  return dss
}


/**
 * Returns the plot title for given id
 */
function getPlotTitle(id)
{
  var tt = xxo.plot.(@id==id).title.text();
  return tt
}


/**
 * Returns the gnusetup for given id
 */
function getGnuSetup(id)
{
  var cmds = new Array();
  var pp = xxo.plot.(@id==id);
  // Get all the commands
  var cc = pp.gnusetup.c;
  for (var jj in cc) {
    cmds[jj] = cc[jj];
  }
  return cmds
}


/**
 * Returns the gnu datasets for given id
 */
function getGnuDataSet(id)
{
  var dss = new Array();
  var pp = xxo.plot.(@id==id);
  // Get all the datasets
  var dd = pp.gnusetup.dataset;
  for (var jj in dd) {
    // dss[jj] = dd[jj];
    dss[jj] = [dd[jj].@src, dd[jj].@title, dd[jj]];
  }
  return dss
}


function process(document)
{
  // Make xxo global 
  xxo = new XML(document);


  for each (var pp in xxo.plot) { 
    //out.println("Process: "+pp);
    var id = pp.@id;
    var t = pp.title;
    out.println("Process "+id+": '"+t+"'");

    for each (var ds in pp.datasource) { 
      var tt = ds.@table.toString();
      var xx = ds.text().toString();
      out.println("Found datasource: '"+tt+" "+xx+"'");
    }

    for each (var cc in pp.gnusetup.c) { 
      var xx = cc.text().toString();
      out.println("Found command: '"+xx+"'");
    }

    for each (var ds in pp.gnusetup.dataset) { 
      var xx = ds.text().toString();
      out.println("Found data '"+xx+"'");
    }

  }
//   var yy = xxo.plot.(@id=='billing.day.bwr');
//   out.println("yy="+yy);
}

"OK"
