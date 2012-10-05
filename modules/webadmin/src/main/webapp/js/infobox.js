if (document.layers) {navigator.family = "nn4"}
if (document.all) {navigator.family = "ie4"}
if (window.navigator.userAgent.toLowerCase().match("gecko")) {navigator.family = "gecko"}



overdiv="0";

function showBox(content)
{


if(navigator.family =="nn4") {
        document.infobox.document.write(content);
        document.infobox.document.close();
        document.infobox.left=x+15;
        document.infobox.top=y-5;
        }
else if(navigator.family =="ie4"){
        infobox.innerHTML=content;
        infobox.style.pixelLeft=x+15;
        infobox.style.pixelTop=y-5;
        }
else if(navigator.family =="gecko"){
        document.getElementById("infobox").innerHTML=content;
        document.getElementById("infobox").style.left=x+15;
        document.getElementById("infobox").style.top=y-5;
        }
}


function hideBox(){
if (overdiv == "0") {
        if(navigator.family =="nn4") {eval(document.infobox.top="-1000");}
        else if(navigator.family =="ie4"){infobox.innerHTML="";infobox.style.pixelTop=y-1000;}
        else if(navigator.family =="gecko") {document.getElementById("infobox").style.top="-1000";}
        }
}

var isNav = (navigator.appName.indexOf("Netscape") !=-1);

function Position(e){

x = (isNav) ? e.pageX : event.clientX + document.body.scrollLeft;
y = (isNav) ? e.pageY : event.clientY + document.body.scrollTop;

}

if (isNav){document.captureEvents(Event.mouseMove);}
document.onmousemove = Position;
