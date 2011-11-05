/**
 * Accessible Tabs - jQuery plugin for accessible, unobtrusive tabs
 * Build to seemlessly work with the CCS-Framework YAML (yaml.de) not depending on YAML though
 * @requires jQuery v1.0.3
 *
 * english article: http://blog.ginader.de/archives/2009/02/07/jQuery-Accessible-Tabs-How-to-make-tabs-REALLY-accessible.php
 * german article: http://blog.ginader.de/archives/2009/02/07/jQuery-Accessible-Tabs-Wie-man-Tabs-WIRKLICH-zugaenglich-macht.php
 * 
 * code: http://github.com/ginader/Accessible-Tabs
 * please report issues at: http://github.com/ginader/Accessible-Tabs/issues
 *
 * Copyright (c) 2007 Dirk Ginader (ginader.de)
 * Dual licensed under the MIT and GPL licenses:
 * http://www.opensource.org/licenses/mit-license.php
 * http://www.gnu.org/licenses/gpl.html
 *
 * Version: 1.5
 * 
 * History:
 * * 1.0 initial release
 * * 1.1 added a lot of Accessibility enhancements
 * * * rewrite to use "fn.extend" structure
 * * * added check for existing ids on the content containers to use to proper anchors in the tabs
 * * 1.1.1 changed the headline markup. thanks to Mike Davies for the hint.
 * * 1.5 thanks to Dirk Jesse, Ansgar Hein, David Maciejewski and Mike West for commiting patches to this release
 * * * new option syncheights that syncs the heights of the tab contents when the SyncHeight plugin 
 * *   is available http://blog.ginader.de/dev/jquery/syncheight/index.php
 * * * fixed the hardcoded current class
 * * * new option tabsListClass to be applied to the generated list of tabs above the content so lists 
 * *   inside the tabscontent can be styled differently
 * * * added clearfix and tabcounter that adds a class in the schema "tabamount{number amount of tabs}" 
 * *   to the ul containg the tabs so one can style the tabs to fit 100% into the width
 * * * new option "syncHeightMethodName" fixed issue: http://github.com/ginader/Accessible-Tabs/issues/2/find
 * * * new Method showAccessibleTab({index number of the tab to show starting with 0})  fixed issue: http://github.com/ginader/Accessible-Tabs/issues/3/find
 * * * added support for the Cursor Keys to come closer to the WAI ARIA Tab Panel Best Practices http://github.com/ginader/Accessible-Tabs/issues/1/find
 */


(function($) {
    var debugMode = false;
    $.fn.extend({
        getUniqueId: function(p){
            return p + new Date().getTime();
        },
        accessibleTabs: function(config) {
            var defaults = {
                wrapperClass: 'content', // Classname to apply to the div that is wrapped around the original Markup
                currentClass: 'current', // Classname to apply to the LI of the selected Tab
                tabhead: 'h3', // Tag or valid Query Selector of the Elements to Transform the Tabs-Navigation from (originals are removed)
                tabbody: '.tabbody', // Tag or valid Query Selector of the Elements to be treated as the Tab Body
                fx:'show', // can be "fadeIn", "slideDown", "show"
                fxspeed: 'normal', // speed (String|Number): "slow", "normal", or "fast") or the number of milliseconds to run the animation
                currentInfoText: 'current tab: ', // text to indicate for screenreaders which tab is the current one
                currentInfoPosition: 'prepend', // Definition where to insert the Info Text. Can be either "prepend" or "append"
                currentInfoClass: 'current-info', // Class to apply to the span wrapping the CurrentInfoText
                tabsListClass:'tabs-list', // Class to apply to the generated list of tabs above the content
                syncheights:false, // syncs the heights of the tab contents when the SyncHeight plugin is available http://blog.ginader.de/dev/jquery/syncheight/index.php
                syncHeightMethodName:'syncHeight' // set the Method name of the plugin you want to use to sync the tab contents. Defaults to the SyncHeight plugin: http://github.com/ginader/syncHeight
            };
            // cursor key codes
            /*
            backspace  	8
            tab 	9
            enter 	13
            shift 	16
            ctrl 	17
            alt 	18
            pause/break 	19
            caps lock 	20
            escape 	27
            page up 	33
            page down 	34
            end 	35
            home 	36
            left arrow 	37
            up arrow 	38
            right arrow 	39
            down arrow 	40
            insert 	45
            delete 	46
            */
            var keyCodes = {
                37 : -1, //LEFT
                38 : -1, //UP
                39 : +1, //RIGHT 
                40 : +1 //DOWN
            };
            this.options = $.extend(defaults, config);
            var o = this;
            return this.each(function() {
                var el = $(this);
                var list = '';
                var tabCount = 0;

                var contentAnchor = o.getUniqueId('accessibletabscontent');
                var tabsAnchor = o.getUniqueId('accessibletabs');
                $(el).wrapInner('<div class="'+o.options.wrapperClass+'"></div>');

                $(el).find(o.options.tabhead).each(function(i){
                    var id = '';
                    if(i === 0){
                        id =' id="'+tabsAnchor+'"';
                    }
                    list += '<li><a'+id+' href="#'+contentAnchor+'">'+$(this).text()+'</a></li>';
                    $(this).remove();
                    tabCount++;
                });

                $(el).prepend('<ul class="clearfix '+o.options.tabsListClass+' tabamount'+tabCount+'">'+list+'</ul>');
                $(el).find(o.options.tabbody).hide();
                $(el).find(o.options.tabbody+':first').show().before('<'+o.options.tabhead+'><a tabindex="0" class="accessibletabsanchor" name="'+contentAnchor+'" id="'+contentAnchor+'">'+$(el).find("ul>li:first").text()+'</a></'+o.options.tabhead+'>');
                $(el).find("ul>li:first").addClass(o.options.currentClass)
                .find('a')[o.options.currentInfoPosition]('<span class="'+o.options.currentInfoClass+'">'+o.options.currentInfoText+'</span>');

                if (o.options.syncheights && $.fn[o.options.syncHeightMethodName]) {
                    $(el).find(o.options.tabbody)[o.options.syncHeightMethodName]();
                    $(window).resize(function(){ 
                        $(el).find(o.options.tabbody)[o.options.syncHeightMethodName]();
                    });
                }

                $(el).find('ul.'+o.options.tabsListClass+'>li>a').each(function(i){
                    $(this).click(function(event){
                        event.preventDefault();
                        $(el).find('ul>li.'+o.options.currentClass).removeClass(o.options.currentClass)
                        .find("span."+o.options.currentInfoClass).remove();
                        $(this).blur();
                        $(el).find(o.options.tabbody+':visible').hide();
                        $(el).find(o.options.tabbody).eq(i)[o.options.fx](o.options.fxspeed);
                        $( '#'+contentAnchor ).text( $(this).text() ).focus().keyup(function(event){
                            if(keyCodes[event.keyCode]){
                                o.showAccessibleTab(i+keyCodes[event.keyCode]);
                                debug(i);
                                $(this).unbind( "keyup" );
                            }
                        });
                        $(this)[o.options.currentInfoPosition]('<span class="'+o.options.currentInfoClass+'">'+o.options.currentInfoText+'</span>')
                        .parent().addClass(o.options.currentClass);


                        // $(el).find('.accessibletabsanchor').keyup(function(event){
                        //     if(keyCodes[event.keyCode]){
                        //         o.showAccessibleTab(i+keyCodes[event.keyCode]);
                        //     }
                        // });
                        
                        
                    });

                    $(this).focus(function(event){
                        debug($(this));
                        $(document).keyup(function(event){
                            if(keyCodes[event.keyCode]){
                                o.showAccessibleTab(i+keyCodes[event.keyCode]);
                            }
                        });
                    });
                    $(this).blur(function(event){
                        $(document).unbind( "keyup" );
                    });
                    

                });
            });
        },
        showAccessibleTab: function(index){
            debug('showAccessibleTab');
            debug(index);
            var o = this;
            return this.each(function() {
                var el = $(this);
                var links = el.find('ul.'+o.options.tabsListClass+'>li>a');
                debug(links);
                links.eq(index).click();
            });
        }
    });
    // private Methods
    function debug(msg){
        if(debugMode && window.console && window.console.log){
            window.console.log(msg);
        }
    }
})(jQuery);