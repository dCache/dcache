$(function() {
    $.extend($.tablesorter.themes.bootstrap, {
        // these classes are added to the table.
        table      : '',
        caption    : 'caption',
        header     : '',
        footerRow  : '',
        footerCells: '',
        icons      : '', // add "icon-white" to make them white; this icon class is added to the <i> in the header
        sortNone   : 'bootstrap-icon-unsorted',
        sortAsc    : 'glyphicon glyphicon-chevron-up',
        sortDesc   : 'glyphicon glyphicon-chevron-down',
        active     : '', // applied when column is sorted
        hover      : '', // use custom css here - bootstrap class may not override it
        filterRow  : '', // filter row class
        even       : '', // odd row zebra striping
        odd        : ''  // even row zebra striping
    });

    $.tablesorter.addParser({
        id: 'customtime',
        is: function (s) {
            return false;
        },
        format: function (s) {
            var date = s.trim().split(' ');
            var dateString = date[1] + "\n" + date[2] + "\n" + date[3] + "\n" + date[5] + "\n";
            return (new Date(dateString).getTime());
        },
        type: 'numeric'
    });

    $('table.sortable').tablesorter({
        // this will apply the bootstrap theme if "uitheme" widget is included
        theme : "bootstrap",

        headerTemplate : '{content} {icon}', // new in v2.7. Needed to add the bootstrap icon!

        // widget code contained in the jquery.tablesorter.widgets.js file
        // use the zebra stripe widget if you plan on hiding any rows (filter widget)
        widgets : [ "uitheme" ]
    });
});
