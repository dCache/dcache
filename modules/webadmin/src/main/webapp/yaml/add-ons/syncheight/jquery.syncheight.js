/**
 * syncHeight - jQuery plugin to automagically Snyc the heights of columns
 * Made to seemlessly work with the CCS-Framework YAML (yaml.de)
 * @requires jQuery v1.0.3
 *
 * http://blog.ginader.de/dev/syncheight/
 *
 * Copyright (c) 2007-2009 
 * Dirk Ginader (ginader.de)
 * Dirk Jesse (yaml.de)
 * Dual licensed under the MIT and GPL licenses:
 * http://www.opensource.org/licenses/mit-license.php
 * http://www.gnu.org/licenses/gpl.html
 *
 * Version: 1.1
 *
 * Usage:
 	$(document).ready(function(){
		$('p').syncHeight();
	});
 */

(function($) {
	$.fn.syncHeight = function(config) {
		var defaults = {
			updateOnResize: false	// re-sync element heights after a browser resize event (useful in flexible layouts)
		};
		var options = $.extend(defaults, config);
		
		var e = this;
		
		var max = 0;
		var browser_id = 0;
		var property = [
			// To avoid content overflow in synchronised boxes on font scaling, we 
			// use 'min-height' property for modern browsers ...
			['min-height','0px'],
			// and 'height' property for Internet Explorer.
			['height','1%']
		];

		// check for IE6 ...
		if($.browser.msie && $.browser.version < 7){
			browser_id = 1;
		}
		
		// get maximum element height ...
		$(this).each(function() {
			// fallback to auto height before height check ...
			$(this).css(property[browser_id][0],property[browser_id][1]);
			var val=$(this).height();
			if(val > max){
			   max = val;
			}
		});
		
		// set synchronized element height ...
 		$(this).each(function() {
  			$(this).css(property[browser_id][0],max+'px');
		});
		
		// optional sync refresh on resize event ...
		if (options.updateOnResize == true) {
			$(window).resize(function(){ 
				$(e).syncHeight();
			});
		}
		return this;
	};	
})(jQuery);