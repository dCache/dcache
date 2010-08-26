#! /usr/bin/env python

"""
This program finds all *.eps and *.jpg files and prepare the html table with them
"""

import os
import sys
import time

if __name__ == '__main__':

    print "Content-type: text/html\n\n"

    if len(sys.argv) < 2:
        monthList = {}
        fpath = "."
        flist = os.listdir(fpath)
        flist.sort()
        for fname in flist:    # Process all billing*{eps,jpg} files
            if fname[:7] != "billing":
                continue
            if fname[-4:] != ".jpg":
                continue
            nameTail = fname[8:]
            nameFlds = nameTail.split(".")
            date = "%s.%s" % (nameFlds[0], nameFlds[1])
            if not monthList.has_key(date):
                monthList[date] = []
            monthList[date].append(fname)
                

        print "<table>"
        months = monthList.keys()
        months.sort(); months.reverse()
        for month in months:
            print "<tr>"
            print '<td>Plots for <a href="dcache_plots_pub.py?%s">%s</a></td>' % (month, month)
            for f in monthList[month]:
                print '''
                <td>
                <a href="dcache_plots_pub.py?%s+%s+1"><img src=%s width="120" hight="80" border="0"</a><br>
                </td>
                ''' % (month, f, f.replace('.jpg','.pre'))
            print "</tr>"
        
        print "</table>"
    elif len(sys.argv) == 2:
        fileList = {}
        fpath = "."
        flist = os.listdir(fpath)
        flist.sort()
        for fname in flist:    # Process all billing*{eps,jpg} files
            if fname[:7] != "billing":
                continue
            if fname[-4:] != ".jpg":
                continue
            if fname.find(sys.argv[1]) > 0:
                fileList[fname] = ""

        print """
        <table>
        """
        fileNames = fileList.keys()
        fileNames.sort()
        for fileName in fileNames:
            print """
            <tr>
              <td>
                <img src="%s" alt="" width="1080" hight="756" border="0">
              </td>
            </tr>
            <tr>
              <td>
                <a href="%s">Postscript copy</a>
              </td>
            </tr>
            """ % (fileName, "%s.eps" % fileName[:-4])
        
        print """
        </table>
        """
    else:
        fileName = sys.argv[2]
        plotMode = sys.argv[3]
        if plotMode == "1":
            newMode = "2"
            imgName = fileName
            epsName = fileName.replace('.jpg','.eps')
        else:
            newMode = "1"
            imgName = fileName.replace('.jpg','.2jpg')
            epsName = fileName.replace('.jpg','.2.eps')
        print """
        <table>
        <tr>
          <td>
            <a href="dcache_plots_pub.py?xxxx+%s+%s"><img src="%s" alt="" width="1080" hight="756" border="0"></a>
          </td>
        </tr>
        <tr>
          <td>
            <a href="%s">Postscript copy</a>
          </td>
        </tr>
        """ % (fileName, newMode, imgName, epsName)
        
        print """
        </table>
        """
