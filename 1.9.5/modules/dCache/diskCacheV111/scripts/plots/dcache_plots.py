import os
import time
import os.path
import sys
import shutil

gnucmds2 = """
set size %(size)s,%(size)s
set output '%(fname)s.brd.2.eps'
set terminal postscript eps color solid 'Arial' %(font)s
#
set title 'Total Bytes Read Per Day (Plotted: %(date)s)'
set xlabel 'Date'
set timefmt '%%m-%%d'
set xdata time
set xrange ['%(month)s-01':'%(month)s-31']
set grid
set yrange [0: ]
set format x '%%m-%%d'
#set key right top Right samplen 1 title 'xxxxxxxx'
set ylabel 'Bytes'
plot '%(fname)s' using 1:($5+$3) t 'dCache'  with im lw %(width)s 2, \\
     '%(fname)s' using 1:($5)    t 'Enstore' with im lw %(width)s 4
#
set output '%(fname)s.bwr.2.eps'
set title 'Total Bytes Written Per Day (Plotted: %(date)s)'
#set key right top Right samplen 1 title 'xxxxxxxx'
set ylabel 'Bytes'
plot '%(fname)s' using 1:($4+$2) t 'dCache'  with im lw %(width)s 2, \\
     '%(fname)s' using 1:($4)    t 'Enstore' with im lw %(width)s 4
#
set output '%(fname)s.frd.2.eps'
set title 'Total Read Transfers Per Day (Plotted: %(date)s)'
#set key right top Right samplen 1 title 'xxxxxxxx'
set ylabel 'Transfers'
plot '%(fname)s' using 1:($10+$12) t 'dCache'  with im lw %(width)s 2, \\
     '%(fname)s' using 1:($12)     t 'Enstore' with im lw %(width)s 4
#
"""

gnucmds1 = """
set size %(size)s,%(size)s
set output '%(fname)s.brd.eps'
set terminal postscript eps color solid 'Arial' %(font)s
#
set title 'Total Bytes Read Per Day (Plotted: %(date)s)'
set xlabel 'Date'
set timefmt '%%m-%%d'
set xdata time
set xrange ['%(month)s-01':'%(month)s-31']
set grid
set yrange [ : ]
set format x '%%m-%%d'
#set key right top Right samplen 1 title 'xxxxxxxx'
set ylabel 'Bytes'
plot '%(fname)s' using 1:($3)  t 'dCache'  with im lw %(width)s 2, \\
     '%(fname)s' using 1:(-$5) t 'Enstore' with im lw %(width)s 4
#
set output '%(fname)s.bwr.eps'
set title 'Total Bytes Written Per Day (Plotted: %(date)s)'
#set key right top Right samplen 1 title 'xxxxxxxx'
set ylabel 'Bytes'
plot '%(fname)s' using 1:($2)  t 'dCache'  with im lw %(width)s 2, \\
     '%(fname)s' using 1:(-$4) t 'Enstore' with im lw %(width)s 4
#
set output '%(fname)s.frd.eps'
set title 'Total Read Transfers Per Day (Plotted: %(date)s)'
#set key right top Right samplen 1 title 'xxxxxxxx'
set ylabel 'Transfers'
plot '%(fname)s' using 1:($10)  t 'dCache'  with im lw %(width)s 2, \\
     '%(fname)s' using 1:(-$12) t 'Enstore' with im lw %(width)s 4
#
"""

def makePlots(fname):
    """
    """
    # print fname

    nameElem = fname.split('.')
    monthNo = nameElem[1]
    vars = { 'fname':fname,
             'date':time.asctime(time.localtime(time.time())),
             'month':monthNo,
             'size' :'3.0',
             'font' :'32',
             'width':'80'
             }

    cmdfnam = "%s.cmd" % fname
    cmdfile = open(cmdfnam, 'w');    cmdfile.write(gnucmds1 % vars);    cmdfile.close()
    os.system("gnuplot %s" % cmdfnam)

    ## 'os.system("gs -dNOPAUSE -sDEVICE=jpeg -sOutputFile=%(fname)s.brd.jpg -dBATCH -g1200x800 %(fname)s.brd.eps" % vars)'
    ## 'os.system("gs -dNOPAUSE -sDEVICE=jpeg -sOutputFile=%(fname)s.bwr.jpg -dBATCH -g1200x800 %(fname)s.bwr.eps" % vars)'
    ## 'os.system("gs -dNOPAUSE -sDEVICE=jpeg -sOutputFile=%(fname)s.frd.jpg -dBATCH -g1200x800 %(fname)s.frd.eps" % vars)'
    os.system("convert -modulate 95,80 -quality 90 %(fname)s.brd.eps %(fname)s.brd.jpg" % vars)
    os.system("convert -modulate 95,80 -quality 90 %(fname)s.bwr.eps %(fname)s.bwr.jpg" % vars)
    os.system("convert -modulate 95,80 -quality 90 %(fname)s.frd.eps %(fname)s.frd.jpg" % vars)
    #
    ## 'os.system("gs -dNOPAUSE -sDEVICE=jpeg -sOutputFile=%(fname)s.brd.jpg -dBATCH -g120x80 -r7 %(fname)s.brd.eps" % vars)'
    ## 'os.system("gs -dNOPAUSE -sDEVICE=jpeg -sOutputFile=%(fname)s.bwr.jpg -dBATCH -g120x80 -r7 %(fname)s.bwr.eps" % vars)'
    ## 'os.system("gs -dNOPAUSE -sDEVICE=jpeg -sOutputFile=%(fname)s.frd.jpg -dBATCH -g120x80 -r7 %(fname)s.frd.eps" % vars)'
    os.system("convert -geometry 120x120 -modulate 90,40 %(fname)s.brd.eps jpg:%(fname)s.brd.pre" % vars)
    os.system("convert -geometry 120x120 -modulate 90,40 %(fname)s.bwr.eps jpg:%(fname)s.bwr.pre" % vars)
    os.system("convert -geometry 120x120 -modulate 90,40 %(fname)s.frd.eps jpg:%(fname)s.frd.pre" % vars)

    cmdfile = open(cmdfnam, 'w');    cmdfile.write(gnucmds2 % vars);    cmdfile.close()
    os.system("gnuplot %s" % cmdfnam)
    #
    os.system("convert -modulate 95,80 -quality 90 %(fname)s.brd.2.eps jpg:%(fname)s.brd.2jpg" % vars)
    os.system("convert -modulate 95,80 -quality 90 %(fname)s.bwr.2.eps jpg:%(fname)s.bwr.2jpg" % vars)
    os.system("convert -modulate 95,80 -quality 90 %(fname)s.frd.2.eps jpg:%(fname)s.frd.2jpg" % vars)
    
    vars['size']  = '1.5'
    vars['font']  = '18'
    vars['width'] = '40'

    cmdfile = open(cmdfnam, 'w');    cmdfile.write(gnucmds1 % vars);    cmdfile.close()
    os.system("gnuplot %s" % cmdfnam)
    #
    cmdfile = open(cmdfnam, 'w');    cmdfile.write(gnucmds2 % vars);    cmdfile.close()
    os.system("gnuplot %s" % cmdfnam)
    
    


if __name__ == '__main__':

    if len(sys.argv) < 2:
        print "USAGE: %s gnuplot_datafile..."%(sys.argv[0])
    else:
        filenames = sys.argv[1:]
        for filename in filenames:
            stampname = ".%s" % filename
            if os.path.exists(filename):
                tstamp = 0
                if os.path.exists(stampname):
                    tstamp = os.path.getmtime(stampname)
                tdata  = os.path.getmtime(filename)
                if tdata > tstamp:
                    makePlots(filename)
                    os.system("touch %s" % stampname)
                    
