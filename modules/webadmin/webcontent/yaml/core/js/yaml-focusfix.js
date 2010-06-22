/**
 * "Yet Another Multicolumn Layout" - (X)HTML/CSS Framework
 *
 * (en) Workaround for Webkit browsers to fix focus problems when using skiplinks
 * (de) Workaround für Webkit browsers, um den Focus zu korrigieren, bei Verwendung von Skiplinks
 *
 * @note			inspired by Paul Ratcliffe's article 
 *					http://www.communis.co.uk/blog/2009-06-02-skip-links-chrome-safari-and-added-wai-aria
 *
 * @copyright       Copyright 2005-2010, Dirk Jesse
 * @license         CC-A 2.0 (http://creativecommons.org/licenses/by/2.0/),
 *                  YAML-C (http://www.yaml.de/en/license/license-conditions.html)
 * @link            http://www.yaml.de
 * @package         yaml
 * @version         3.2.1
 * @revision        $Revision: 443 $
 * @lastmodified    $Date: 2009-12-31 18:05:05 +0100 (Do, 31. Dez 2009) $
 */

var YAML_focusFix = {
	init: function() {
		
		var userAgent = navigator.userAgent.toLowerCase();
		var	is_webkit = userAgent.indexOf('webkit') > -1;
		var	is_ie = userAgent.indexOf('msie') > -1;
		var i = 0;
		var links, skiplinks = [];
		
		if (is_webkit || is_ie)
		{
			// find skiplinks in modern browsers ...
			if ( document.getElementsByClassName !== undefined) {
				skiplinks = document.getElementsByClassName('skip');
	
				for (i=0; i<skiplinks.length; i++) {
					this.setTabIndex(skiplinks[i]);
				}
			} else {
				// find skiplinks in older browsers ...
				links = document.getElementsByTagName('a');
				for (i=0; i<links.length; i++) {
					var s = links[i].getAttribute('href');
					if (s.length > 1 && s.substr(0, 1) == '#' ) {
						this.setTabIndex(links[i]);				
					}
				}
			}	
		}
	},
	
	setTabIndex: function( skiplink ){
		var target = skiplink.href.substr(skiplink.href.indexOf('#')+1);
		var targetElement = document.getElementById(target);
	
		if (targetElement !== null) {
			// make element accessible for .focus() method  
			targetElement.setAttribute("tabindex", "-1");
			skiplink.setAttribute("onclick", "document.getElementById('"+target+"').focus();");		
		}
	}
};

YAML_focusFix.init();