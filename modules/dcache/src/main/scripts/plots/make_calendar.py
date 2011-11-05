#!/usr/bin/env python

import calendar
import string

# write out an html formatted calendar

BILLING = 'billing-'
HTML = '.html'
ASCII = 1
INTEGER = 0

def get_date(filename, mode=INTEGER):
    date = string.replace(filename, BILLING, "")
    date = string.replace(date, HTML, "")
    if mode == INTEGER:
        date = string.split(date, '.')
        date = [int(date[0]), int(date[1]), int(date[2]), int(date[3])]
    return date


class HtmlCalendar:

    HEADER = '\n<tr><td><BR></td></tr><tr><td><TABLE align="LEFT" border="2" cellpadding="3" cellspacing="5" bgcolor="#DFF0FF">'
    TRAILER = '</TABLE></td></tr>\n'

    def __init__(self, fd, http_path=""):
        # file to write the html to
        self.fd = fd
        self.http_path = http_path

    # given a dict of html billing files, create a list of lists which divides the
    # html billing files up by month and year. most recent goes first
    # return a dict as follows -
    #     {"month year" : {day1 : { hour1 : file
    #                               hour2 : file2},
    #                      day2 : {...}
    #                     },
    #      "month2 year" : {...}
    #      }
    def find_months(self, file_l):
        file_l.sort()
	file_l.reverse()
	file_months = {}
	dates = []
	for file in file_l:
	    [year, month, day, hour] = get_date(file)
            if year == 0:
                # there was an error in get_date
                continue
	    date = "%s %s"%(calendar.month_name[month], year)
	    if not dates or not (year, month, date) in dates:
		dates.append((year, month, date))
	    if not file_months.has_key(date):
		file_months[date] = {}
            if not file_months[date].has_key(day):
                file_months[date][day] = {}
	    file_months[date][day][hour] = file
	dates.sort()
	dates.reverse()
	return (dates, file_months)

    HOURS_L = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17,
               18, 19, 20, 21, 22, 23]

    # write out a table  with a data element for each hour.
    def write_hours(self, hours_d, day):
        if not hours_d:
            # there was no log file for this day
            self.fd.write('<TD bgcolor="#FFFFF0"><STRONG>%s</STRONG></TD>\n'%(day,))
        else:
            # there were some log files, we need to determine which ones
            self.fd.write('<TD bgcolor="#DFF0FF">\n<TABLE bgcolor="#FFFFF0" align="center" border cellpadding=2>\n')
            self.fd.write('<TR><CAPTION><STRONG>%s</STRONG></CAPTION></TR>\n<TR>'%(day,))
            i = 0
            for hour in self.HOURS_L:
                if i == 6:
                    # we want 6 hours per row
                    i = 0
                    self.fd.write("</TR>\n<TR>")
                if hours_d.has_key(hour):
                    self.fd.write('<TD bgcolor="#DFF0FF"><A HREF="%s/%s">%s</A></TD>'%(self.http_path,
                                                                                       hours_d[hour], hour))
                else:
                    self.fd.write('<TD>%s</TD>'%(hour,))
                i = i + 1
            else:
                # finish up the table
                self.fd.write("</TR>\n</TABLE>")
                    
        

    # generate the calendar looking months with url's for each day for which 
    # there exists an html billing file. the data  should be a list of files
    def generate_months(self, file_l):
	(dates, days_d) = self.find_months(file_l)
        did_title = 0
	for (year, month, date) in dates:
            self.fd.write(self.HEADER)
	    if not did_title:
		did_title = 1
                self.fd.write('<CAPTION><FONT color="#770000" size="+3"><STRONG>Completed dCache Transfers</STRONG></FONT><BR><BR><FONT color="#770000" size="+2"><STRONG>')
            else:
                self.fd.write('<TR><CAPTION><FONT color="#770000" size="+2"><STRONG>')
            self.fd.write("%s</STRONG></FONT></CAPTION></TR>\n"%(date,))

            self.fd.write('<TR>')
	    for day in calendar.day_abbr:
                self.fd.write('<TD><FONT color="#770000" size="+1"><STRONG>%s</STRONG></FONT></TD>\n'%(day,))
            else:
                self.fd.write('</TR>\n')
            
	    # the following generates a  list of lists, which specifies how to 
	    # draw a calendar with the first of the month occuring in the
            # correct day of the week slot.
	    mweeks = calendar.monthcalendar(year, month)
	    for mweek in mweeks:
                self.fd.write('<TR>')
		for day in [0,1,2,3,4,5,6]:
		    if mweek[day] == 0:
			# this is null entry represented by a blank entry on the 
			# calendar
			self.fd.write('<td html_escape="OFF">&nbsp;</td>')
		    else:
                        hours_d = days_d[date].get(mweek[day], [])
                        self.write_hours(hours_d, mweek[day])
                else:
                    self.fd.write('</TR>\n')
            self.fd.write(self.TRAILER)

    def write(self, file_l):
        self.generate_months(file_l)
