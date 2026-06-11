(function(f){if(typeof exports==="object"&&typeof module!=="undefined"){module.exports=f()}else if(typeof define==="function"&&define.amd){define([],f)}else{var g;if(typeof window!=="undefined"){g=window}else if(typeof global!=="undefined"){g=global}else if(typeof self!=="undefined"){g=self}else{g=this}g.RpcBuilder = f()}})(function(){var define,module,exports;return (function(){function r(e,n,t){function o(i,f){if(!n[i]){if(!e[i]){var c="function"==typeof require&&require;if(!f&&c)return c(i,!0);if(u)return u(i,!0);var a=new Error("Cannot find module '"+i+"'");throw a.code="MODULE_NOT_FOUND",a}var p=n[i]={exports:{}};e[i][0].call(p.exports,function(r){var n=e[i][1][r];return o(n||r)},p,p.exports,r,e,n,t)}return n[i].exports}for(var u="function"==typeof require&&require,i=0;i<t.length;i++)o(t[i]);return o}return r})()({1:[function(require,module,exports){
function Mapper() {
  var sources = {};

  this.forEach = function (callback) {
    for (var key in sources) {
      var source = sources[key];

      for (var key2 in source)
        callback(source[key2]);
    };
  };

  this.get = function (id, source) {
    var ids = sources[source];
    if (ids == undefined)
      return undefined;

    return ids[id];
  };

  this.remove = function (id, source) {
    var ids = sources[source];
    if (ids == undefined)
      return;

    delete ids[id];

    // Check it's empty
    for (var i in ids) {
      return false
    }

    delete sources[source];
  };

  this.set = function (value, id, source) {
    if (value == undefined)
      return this.remove(id, source);

    var ids = sources[source];
    if (ids == undefined)
      sources[source] = ids = {};

    ids[id] = value;
  };
};

Mapper.prototype.pop = function (id, source) {
  var value = this.get(id, source);
  if (value == undefined)
    return undefined;

  this.remove(id, source);

  return value;
};

module.exports = Mapper;

},{}],2:[function(require,module,exports){
/*
 * (C) Copyright 2014 Kurento (http://kurento.org/)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

var JsonRpcClient = require('./jsonrpcclient');

exports.JsonRpcClient = JsonRpcClient;

},{"./jsonrpcclient":3}],3:[function(require,module,exports){
/*
 * (C) Copyright 2014 Kurento (http://kurento.org/)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

var RpcBuilder = require('../..');
var WebSocketWithReconnection = require(
  './transports/webSocketWithReconnection');

Date.now = Date.now || function () {
  return +new Date;
};

var PING_INTERVAL = 5000;

var RECONNECTING = 'RECONNECTING';
var CONNECTED = 'CONNECTED';
var DISCONNECTED = 'DISCONNECTED';

var Logger = console;

/**
 *
 * heartbeat: interval in ms for each heartbeat message,
 * sendCloseMessage : true / false, before closing the connection, it sends a closeSession message
 * <pre>
 * ws : {
 * 	uri : URI to conntect to,
 *  useSockJS : true (use SockJS) / false (use WebSocket) by default,
 * 	onconnected : callback method to invoke when connection is successful,
 * 	ondisconnect : callback method to invoke when the connection is lost,
 * 	onreconnecting : callback method to invoke when the client is reconnecting,
 * 	onreconnected : callback method to invoke when the client succesfully reconnects,
 * 	onerror : callback method to invoke when there is an error
 * },
 * rpc : {
 * 	requestTimeout : timeout for a request,
 * 	sessionStatusChanged: callback method for changes in session status,
 * 	mediaRenegotiation: mediaRenegotiation
 * }
 * </pre>
 */
function JsonRpcClient(configuration) {

  var self = this;

  var wsConfig = configuration.ws;

  var notReconnectIfNumLessThan = -1;

  var pingNextNum = 0;
  var enabledPings = true;
  var pingPongStarted = false;
  var pingInterval;

  var status = DISCONNECTED;

  var onreconnecting = wsConfig.onreconnecting;
  var onreconnected = wsConfig.onreconnected;
  var onconnected = wsConfig.onconnected;
  var onerror = wsConfig.onerror;

  configuration.rpc.pull = function (params, request) {
    request.reply(null, "push");
  }

  wsConfig.onreconnecting = function () {
    Logger.debug("--------- ONRECONNECTING -----------");
    if (status === RECONNECTING) {
      Logger.error(
        "Websocket already in RECONNECTING state when receiving a new ONRECONNECTING message. Ignoring it"
      );
      return;
    }

    status = RECONNECTING;
    if (onreconnecting) {
      onreconnecting();
    }
  }

  wsConfig.onreconnected = function () {
    Logger.debug("--------- ONRECONNECTED -----------");
    if (status === CONNECTED) {
      Logger.error(
        "Websocket already in CONNECTED state when receiving a new ONRECONNECTED message. Ignoring it"
      );
      return;
    }
    status = CONNECTED;

    enabledPings = true;
    updateNotReconnectIfLessThan();
    usePing();

    if (onreconnected) {
      onreconnected();
    }
  }

  wsConfig.onconnected = function () {
    Logger.debug("--------- ONCONNECTED -----------");
    if (status === CONNECTED) {
      Logger.error(
        "Websocket already in CONNECTED state when receiving a new ONCONNECTED message. Ignoring it"
      );
      return;
    }
    status = CONNECTED;

    enabledPings = true;
    usePing();

    if (onconnected) {
      onconnected();
    }
  }

  wsConfig.onerror = function (error) {
    Logger.debug("--------- ONERROR -----------");

    status = DISCONNECTED;

    if (onerror) {
      onerror(error);
    }
  }

  var ws = new WebSocketWithReconnection(wsConfig);

  Logger.debug('Connecting websocket to URI: ' + wsConfig.uri);

  var rpcBuilderOptions = {
    request_timeout: configuration.rpc.requestTimeout,
    ping_request_timeout: configuration.rpc.heartbeatRequestTimeout
  };

  var rpc = new RpcBuilder(RpcBuilder.packers.JsonRPC, rpcBuilderOptions, ws,
    function (request) {

      Logger.debug('Received request: ' + JSON.stringify(request));

      try {
        var func = configuration.rpc[request.method];

        if (func === undefined) {
          Logger.error("Method " + request.method +
            " not registered in client");
        } else {
          func(request.params, request);
        }
      } catch (err) {
        Logger.error('Exception processing request: ' + JSON.stringify(
          request));
        Logger.error(err);
      }
    });

  this.send = function (method, params, callback) {
    if (method !== 'ping') {
      Logger.debug('Request: method:' + method + " params:" + JSON.stringify(
        params));
    }

    var requestTime = Date.now();

    rpc.encode(method, params, function (error, result) {
      if (error) {
        try {
          Logger.error("ERROR:" + error.message + " in Request: method:" +
            method + " params:" + JSON.stringify(params) + " request:" +
            error.request);
          if (error.data) {
            Logger.error("ERROR DATA:" + JSON.stringify(error.data));
          }
        } catch (e) {}
        error.requestTime = requestTime;
      }
      if (callback) {
        if (result != undefined && result.value !== 'pong') {
          Logger.debug('Response: ' + JSON.stringify(result));
        }
        callback(error, result);
      }
    });
  }

  function updateNotReconnectIfLessThan() {
    Logger.debug("notReconnectIfNumLessThan = " + pingNextNum + ' (old=' +
      notReconnectIfNumLessThan + ')');
    notReconnectIfNumLessThan = pingNextNum;
  }

  function sendPing() {
    if (enabledPings) {
      var params = null;
      if (pingNextNum == 0 || pingNextNum == notReconnectIfNumLessThan) {
        params = {
          interval: configuration.heartbeat || PING_INTERVAL
        };
      }
      pingNextNum++;

      self.send('ping', params, (function (pingNum) {
        return function (error, result) {
          if (error) {
            Logger.debug("Error in ping request #" + pingNum + " (" +
              error.message + ")");
            if (pingNum > notReconnectIfNumLessThan) {
              enabledPings = false;
              updateNotReconnectIfLessThan();
              Logger.debug("Server did not respond to ping message #" +
                pingNum + ". Reconnecting... ");
              ws.reconnectWs();
            }
          }
        }
      })(pingNextNum));
    } else {
      Logger.debug("Trying to send ping, but ping is not enabled");
    }
  }

  /*
   * If configuration.hearbeat has any value, the ping-pong will work with the interval
   * of configuration.hearbeat
   */
  function usePing() {
    if (!pingPongStarted) {
      Logger.debug("Starting ping (if configured)")
      pingPongStarted = true;

      if (configuration.heartbeat != undefined) {
        pingInterval = setInterval(sendPing, configuration.heartbeat);
        sendPing();
      }
    }
  }

  this.close = function () {
    Logger.debug("Closing jsonRpcClient explicitly by client");

    if (pingInterval != undefined) {
      Logger.debug("Clearing ping interval");
      clearInterval(pingInterval);
    }
    pingPongStarted = false;
    enabledPings = false;

    if (configuration.sendCloseMessage) {
      Logger.debug("Sending close message")
      this.send('closeSession', null, function (error, result) {
        if (error) {
          Logger.error("Error sending close message: " + JSON.stringify(
            error));
        }
        ws.close();
      });
    } else {
      ws.close();
    }
  }

  // This method is only for testing
  this.forceClose = function (millis) {
    ws.forceClose(millis);
  }

  this.reconnect = function () {
    ws.reconnectWs();
  }
}

module.exports = JsonRpcClient;

},{"../..":6,"./transports/webSocketWithReconnection":5}],4:[function(require,module,exports){
/*
 * (C) Copyright 2014 Kurento (http://kurento.org/)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

var WebSocketWithReconnection = require('./webSocketWithReconnection');

exports.WebSocketWithReconnection = WebSocketWithReconnection;

},{"./webSocketWithReconnection":5}],5:[function(require,module,exports){
(function (global){(function (){
/*
 * (C) Copyright 2013-2015 Kurento (http://kurento.org/)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

"use strict";

var BrowserWebSocket = global.WebSocket || global.MozWebSocket;

var Logger = console;

/**
 * Get either the `WebSocket` or `MozWebSocket` globals
 * in the browser or try to resolve WebSocket-compatible
 * interface exposed by `ws` for Node-like environment.
 */

var WebSocket = BrowserWebSocket;
if (!WebSocket && typeof window === 'undefined') {
  try {
    WebSocket = require('ws');
  } catch (e) {}
}

//var SockJS = require('sockjs-client');

var MAX_RETRIES = 2000; // Forever...
var RETRY_TIME_MS = 3000; // FIXME: Implement exponential wait times...

var CONNECTING = 0;
var OPEN = 1;
var CLOSING = 2;
var CLOSED = 3;

/*
config = {
		uri : wsUri,
		useSockJS : true (use SockJS) / false (use WebSocket) by default,
		onconnected : callback method to invoke when connection is successful,
		ondisconnect : callback method to invoke when the connection is lost,
		onreconnecting : callback method to invoke when the client is reconnecting,
		onreconnected : callback method to invoke when the client succesfully reconnects,
	};
*/
function WebSocketWithReconnection(config) {

  var closing = false;
  var registerMessageHandler;
  var wsUri = config.uri;
  var useSockJS = config.useSockJS;
  var reconnecting = false;

  var forcingDisconnection = false;

  var ws;

  if (useSockJS) {
    ws = new SockJS(wsUri);
  } else {
    ws = new WebSocket(wsUri);
  }

  ws.onopen = function () {
    logConnected(ws, wsUri);
    if (config.onconnected) {
      config.onconnected();
    }
  };

  ws.onerror = function (error) {
    Logger.error("Could not connect to " + wsUri +
      " (invoking onerror if defined)", error);
    if (config.onerror) {
      config.onerror(error);
    }
  };

  function logConnected(ws, wsUri) {
    try {
      Logger.debug("WebSocket connected to " + wsUri);
    } catch (e) {
      Logger.error(e);
    }
  }

  var reconnectionOnClose = function () {
    if (ws.readyState === CLOSED) {
      if (closing) {
        Logger.debug("Connection closed by user");
      } else {
        Logger.debug("Connection closed unexpectecly. Reconnecting...");
        reconnectToSameUri(MAX_RETRIES, 1);
      }
    } else {
      Logger.debug("Close callback from previous websocket. Ignoring it");
    }
  };

  ws.onclose = reconnectionOnClose;

  function reconnectToSameUri(maxRetries, numRetries) {
    Logger.debug("reconnectToSameUri (attempt #" + numRetries + ", max=" +
      maxRetries + ")");

    if (numRetries === 1) {
      if (reconnecting) {
        Logger.warn(
          "Trying to reconnectToNewUri when reconnecting... Ignoring this reconnection."
        )
        return;
      } else {
        reconnecting = true;
      }

      if (config.onreconnecting) {
        config.onreconnecting();
      }
    }

    if (forcingDisconnection) {
      reconnectToNewUri(maxRetries, numRetries, wsUri);

    } else {
      if (config.newWsUriOnReconnection) {
        config.newWsUriOnReconnection(function (error, newWsUri) {

          if (error) {
            Logger.debug(error);
            setTimeout(function () {
              reconnectToSameUri(maxRetries, numRetries + 1);
            }, RETRY_TIME_MS);
          } else {
            reconnectToNewUri(maxRetries, numRetries, newWsUri);
          }
        })
      } else {
        reconnectToNewUri(maxRetries, numRetries, wsUri);
      }
    }
  }

  // TODO Test retries. How to force not connection?
  function reconnectToNewUri(maxRetries, numRetries, reconnectWsUri) {
    Logger.debug("Reconnection attempt #" + numRetries);

    ws.close();

    wsUri = reconnectWsUri || wsUri;

    var newWs;
    if (useSockJS) {
      newWs = new SockJS(wsUri);
    } else {
      newWs = new WebSocket(wsUri);
    }

    newWs.onopen = function () {
      Logger.debug("Reconnected after " + numRetries + " attempts...");
      logConnected(newWs, wsUri);
      reconnecting = false;
      registerMessageHandler();
      if (config.onreconnected()) {
        config.onreconnected();
      }

      newWs.onclose = reconnectionOnClose;
    };

    var onErrorOrClose = function (error) {
      Logger.warn("Reconnection error: ", error);

      if (numRetries === maxRetries) {
        if (config.ondisconnect) {
          config.ondisconnect();
        }
      } else {
        setTimeout(function () {
          reconnectToSameUri(maxRetries, numRetries + 1);
        }, RETRY_TIME_MS);
      }
    };

    newWs.onerror = onErrorOrClose;

    ws = newWs;
  }

  this.close = function () {
    closing = true;
    ws.close();
  };

  // This method is only for testing
  this.forceClose = function (millis) {
    Logger.debug("Testing: Force WebSocket close");

    if (millis) {
      Logger.debug("Testing: Change wsUri for " + millis +
        " millis to simulate net failure");
      var goodWsUri = wsUri;
      wsUri = "wss://21.234.12.34.4:443/";

      forcingDisconnection = true;

      setTimeout(function () {
        Logger.debug("Testing: Recover good wsUri " + goodWsUri);
        wsUri = goodWsUri;

        forcingDisconnection = false;

      }, millis);
    }

    ws.close();
  };

  this.reconnectWs = function () {
    Logger.debug("reconnectWs");
    reconnectToSameUri(MAX_RETRIES, 1, wsUri);
  };

  this.send = function (message) {
    ws.send(message);
  };

  this.addEventListener = function (type, callback) {
    registerMessageHandler = function () {
      ws.addEventListener(type, callback);
    };

    registerMessageHandler();
  };
}

module.exports = WebSocketWithReconnection;

}).call(this)}).call(this,typeof global !== "undefined" ? global : typeof self !== "undefined" ? self : typeof window !== "undefined" ? window : {})

},{"ws":undefined}],6:[function(require,module,exports){
/*
 * (C) Copyright 2014 Kurento (http://kurento.org/)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

var defineProperty_IE8 = false
if (Object.defineProperty) {
  try {
    Object.defineProperty({}, "x", {});
  } catch (e) {
    defineProperty_IE8 = true
  }
}

// https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Function/bind
if (!Function.prototype.bind) {
  Function.prototype.bind = function (oThis) {
    if (typeof this !== 'function') {
      // closest thing possible to the ECMAScript 5
      // internal IsCallable function
      throw new TypeError(
        'Function.prototype.bind - what is trying to be bound is not callable'
      );
    }

    var aArgs = Array.prototype.slice.call(arguments, 1),
      fToBind = this,
      fNOP = function () {},
      fBound = function () {
        return fToBind.apply(this instanceof fNOP && oThis ?
          this :
          oThis,
          aArgs.concat(Array.prototype.slice.call(arguments)));
      };

    fNOP.prototype = this.prototype;
    fBound.prototype = new fNOP();

    return fBound;
  };
}

var EventEmitter = require('events').EventEmitter;

var inherits = require('inherits');

var packers = require('./packers');
var Mapper = require('./Mapper');

var BASE_TIMEOUT = 5000;

function unifyResponseMethods(responseMethods) {
  if (!responseMethods) return {};

  for (var key in responseMethods) {
    var value = responseMethods[key];

    if (typeof value == 'string')
      responseMethods[key] = {
        response: value
      }
  };

  return responseMethods;
};

function unifyTransport(transport) {
  if (!transport) return;

  // Transport as a function
  if (transport instanceof Function)
    return {
      send: transport
    };

  // WebSocket & DataChannel
  if (transport.send instanceof Function)
    return transport;

  // Message API (Inter-window & WebWorker)
  if (transport.postMessage instanceof Function) {
    transport.send = transport.postMessage;
    return transport;
  }

  // Stream API
  if (transport.write instanceof Function) {
    transport.send = transport.write;
    return transport;
  }

  // Transports that only can receive messages, but not send
  if (transport.onmessage !== undefined) return;
  if (transport.pause instanceof Function) return;

  throw new SyntaxError("Transport is not a function nor a valid object");
};

/**
 * Representation of a RPC notification
 *
 * @class
 *
 * @constructor
 *
 * @param {String} method -method of the notification
 * @param params - parameters of the notification
 */
function RpcNotification(method, params) {
  if (defineProperty_IE8) {
    this.method = method
    this.params = params
  } else {
    Object.defineProperty(this, 'method', {
      value: method,
      enumerable: true
    });
    Object.defineProperty(this, 'params', {
      value: params,
      enumerable: true
    });
  }
};

/**
 * @class
 *
 * @constructor
 *
 * @param {object} packer
 *
 * @param {object} [options]
 *
 * @param {object} [transport]
 *
 * @param {Function} [onRequest]
 */
function RpcBuilder(packer, options, transport, onRequest) {
  var self = this;

  if (!packer)
    throw new SyntaxError('Packer is not defined');

  if (!packer.pack || !packer.unpack)
    throw new SyntaxError('Packer is invalid');

  var responseMethods = unifyResponseMethods(packer.responseMethods);

  if (options instanceof Function) {
    if (transport != undefined)
      throw new SyntaxError("There can't be parameters after onRequest");

    onRequest = options;
    transport = undefined;
    options = undefined;
  };

  if (options && options.send instanceof Function) {
    if (transport && !(transport instanceof Function))
      throw new SyntaxError("Only a function can be after transport");

    onRequest = transport;
    transport = options;
    options = undefined;
  };

  if (transport instanceof Function) {
    if (onRequest != undefined)
      throw new SyntaxError("There can't be parameters after onRequest");

    onRequest = transport;
    transport = undefined;
  };

  if (transport && transport.send instanceof Function)
    if (onRequest && !(onRequest instanceof Function))
      throw new SyntaxError("Only a function can be after transport");

  options = options || {};

  EventEmitter.call(this);

  if (onRequest)
    this.on('request', onRequest);

  if (defineProperty_IE8)
    this.peerID = options.peerID
  else
    Object.defineProperty(this, 'peerID', {
      value: options.peerID
    });

  var max_retries = options.max_retries || 0;

  function transportMessage(event) {
    self.decode(event.data || event.toString());
  };

  this.getTransport = function () {
    return transport;
  }
  this.setTransport = function (value) {
    // Remove listener from old transport
    if (transport) {
      // W3C transports
      if (transport.removeEventListener)
        transport.removeEventListener('message', transportMessage);

      // Node.js Streams API
      else if (transport.removeListener)
        transport.removeListener('data', transportMessage);
    };

    // Set listener on new transport
    if (value) {
      // W3C transports
      if (value.addEventListener)
        value.addEventListener('message', transportMessage);

      // Node.js Streams API
      else if (value.addListener)
        value.addListener('data', transportMessage);
    };

    transport = unifyTransport(value);
  }

  if (!defineProperty_IE8)
    Object.defineProperty(this, 'transport', {
      get: this.getTransport.bind(this),
      set: this.setTransport.bind(this)
    })

  this.setTransport(transport);

  var request_timeout = options.request_timeout || BASE_TIMEOUT;
  var ping_request_timeout = options.ping_request_timeout || request_timeout;
  var response_timeout = options.response_timeout || BASE_TIMEOUT;
  var duplicates_timeout = options.duplicates_timeout || BASE_TIMEOUT;

  var requestID = 0;

  var requests = new Mapper();
  var responses = new Mapper();
  var processedResponses = new Mapper();

  var message2Key = {};

  /**
   * Store the response to prevent to process duplicate request later
   */
  function storeResponse(message, id, dest) {
    var response = {
      message: message,
      /** Timeout to auto-clean old responses */
      timeout: setTimeout(function () {
          responses.remove(id, dest);
        },
        response_timeout)
    };

    responses.set(response, id, dest);
  };

  /**
   * Store the response to ignore duplicated messages later
   */
  function storeProcessedResponse(ack, from) {
    var timeout = setTimeout(function () {
        processedResponses.remove(ack, from);
      },
      duplicates_timeout);

    processedResponses.set(timeout, ack, from);
  };

  /**
   * Representation of a RPC request
   *
   * @class
   * @extends RpcNotification
   *
   * @constructor
   *
   * @param {String} method -method of the notification
   * @param params - parameters of the notification
   * @param {Integer} id - identifier of the request
   * @param [from] - source of the notification
   */
  function RpcRequest(method, params, id, from, transport) {
    RpcNotification.call(this, method, params);

    this.getTransport = function () {
      return transport;
    }
    this.setTransport = function (value) {
      transport = unifyTransport(value);
    }

    if (!defineProperty_IE8)
      Object.defineProperty(this, 'transport', {
        get: this.getTransport.bind(this),
        set: this.setTransport.bind(this)
      })

    var response = responses.get(id, from);

    /**
     * @constant {Boolean} duplicated
     */
    if (!(transport || self.getTransport())) {
      if (defineProperty_IE8)
        this.duplicated = Boolean(response)
      else
        Object.defineProperty(this, 'duplicated', {
          value: Boolean(response)
        });
    }

    var responseMethod = responseMethods[method];

    this.pack = packer.pack.bind(packer, this, id)

    /**
     * Generate a response to this request
     *
     * @param {Error} [error]
     * @param {*} [result]
     *
     * @returns {string}
     */
    this.reply = function (error, result, transport) {
      // Fix optional parameters
      if (error instanceof Function || error && error
        .send instanceof Function) {
        if (result != undefined)
          throw new SyntaxError("There can't be parameters after callback");

        transport = error;
        result = null;
        error = undefined;
      } else if (result instanceof Function ||
        result && result.send instanceof Function) {
        if (transport != undefined)
          throw new SyntaxError("There can't be parameters after callback");

        transport = result;
        result = null;
      };

      transport = unifyTransport(transport);

      // Duplicated request, remove old response timeout
      if (response)
        clearTimeout(response.timeout);

      if (from != undefined) {
        if (error)
          error.dest = from;

        if (result)
          result.dest = from;
      };

      var message;

      // New request or overriden one, create new response with provided data
      if (error || result != undefined) {
        if (self.peerID != undefined) {
          if (error)
            error.from = self.peerID;
          else
            result.from = self.peerID;
        }

        // Protocol indicates that responses has own request methods
        if (responseMethod) {
          if (responseMethod.error == undefined && error)
            message = {
              error: error
            };

          else {
            var method = error ?
              responseMethod.error :
              responseMethod.response;

            message = {
              method: method,
              params: error || result
            };
          }
        } else
          message = {
            error: error,
            result: result
          };

        message = packer.pack(message, id);
      }

      // Duplicate & not-overriden request, re-send old response
      else if (response)
        message = response.message;

      // New empty reply, response null value
      else
        message = packer.pack({
          result: null
        }, id);

      // Store the response to prevent to process a duplicated request later
      storeResponse(message, id, from);

      // Return the stored response so it can be directly send back
      transport = transport || this.getTransport() || self.getTransport();

      if (transport)
        return transport.send(message);

      return message;
    }
  };
  inherits(RpcRequest, RpcNotification);

  function cancel(message) {
    var key = message2Key[message];
    if (!key) return;

    delete message2Key[message];

    var request = requests.pop(key.id, key.dest);
    if (!request) return;

    clearTimeout(request.timeout);

    // Start duplicated responses timeout
    storeProcessedResponse(key.id, key.dest);
  };

  /**
   * Allow to cancel a request and don't wait for a response
   *
   * If `message` is not given, cancel all the request
   */
  this.cancel = function (message) {
    if (message) return cancel(message);

    for (var message in message2Key)
      cancel(message);
  };

  this.close = function () {
    // Prevent to receive new messages
    var transport = this.getTransport();
    if (transport && transport.close)
      transport.close();

    // Request & processed responses
    this.cancel();

    processedResponses.forEach(clearTimeout);

    // Responses
    responses.forEach(function (response) {
      clearTimeout(response.timeout);
    });
  };

  /**
   * Generates and encode a JsonRPC 2.0 message
   *
   * @param {String} method -method of the notification
   * @param params - parameters of the notification
   * @param [dest] - destination of the notification
   * @param {object} [transport] - transport where to send the message
   * @param [callback] - function called when a response to this request is
   *   received. If not defined, a notification will be send instead
   *
   * @returns {string} A raw JsonRPC 2.0 request or notification string
   */
  this.encode = function (method, params, dest, transport, callback) {
    // Fix optional parameters
    if (params instanceof Function) {
      if (dest != undefined)
        throw new SyntaxError("There can't be parameters after callback");

      callback = params;
      transport = undefined;
      dest = undefined;
      params = undefined;
    } else if (dest instanceof Function) {
      if (transport != undefined)
        throw new SyntaxError("There can't be parameters after callback");

      callback = dest;
      transport = undefined;
      dest = undefined;
    } else if (transport instanceof Function) {
      if (callback != undefined)
        throw new SyntaxError("There can't be parameters after callback");

      callback = transport;
      transport = undefined;
    };

    if (self.peerID != undefined) {
      params = params || {};

      params.from = self.peerID;
    };

    if (dest != undefined) {
      params = params || {};

      params.dest = dest;
    };

    // Encode message
    var message = {
      method: method,
      params: params
    };

    if (callback) {
      var id = requestID++;
      var retried = 0;

      message = packer.pack(message, id);

      function dispatchCallback(error, result) {
        self.cancel(message);

        callback(error, result);
      };

      var request = {
        message: message,
        callback: dispatchCallback,
        responseMethods: responseMethods[method] || {}
      };

      var encode_transport = unifyTransport(transport);

      function sendRequest(transport) {
        var rt = (method === 'ping' ? ping_request_timeout : request_timeout);
        request.timeout = setTimeout(timeout, rt * Math.pow(2, retried++));
        message2Key[message] = {
          id: id,
          dest: dest
        };
        requests.set(request, id, dest);

        transport = transport || encode_transport || self.getTransport();
        if (transport)
          return transport.send(message);

        return message;
      };

      function retry(transport) {
        transport = unifyTransport(transport);

        console.warn(retried + ' retry for request message:', message);

        var timeout = processedResponses.pop(id, dest);
        clearTimeout(timeout);

        return sendRequest(transport);
      };

      function timeout() {
        if (retried < max_retries)
          return retry(transport);

        var error = new Error('Request has timed out');
        error.request = message;

        error.retry = retry;

        dispatchCallback(error)
      };

      return sendRequest(transport);
    };

    // Return the packed message
    message = packer.pack(message);

    transport = transport || this.getTransport();
    if (transport)
      return transport.send(message);

    return message;
  };

  /**
   * Decode and process a JsonRPC 2.0 message
   *
   * @param {string} message - string with the content of the message
   *
   * @returns {RpcNotification|RpcRequest|undefined} - the representation of the
   *   notification or the request. If a response was processed, it will return
   *   `undefined` to notify that it was processed
   *
   * @throws {TypeError} - Message is not defined
   */
  this.decode = function (message, transport) {
    if (!message)
      throw new TypeError("Message is not defined");

    try {
      message = packer.unpack(message);
    } catch (e) {
      // Ignore invalid messages
      return console.debug(e, message);
    };

    var id = message.id;
    var ack = message.ack;
    var method = message.method;
    var params = message.params || {};

    var from = params.from;
    var dest = params.dest;

    // Ignore messages send by us
    if (self.peerID != undefined && from == self.peerID) return;

    // Notification
    if (id == undefined && ack == undefined) {
      var notification = new RpcNotification(method, params);

      if (self.emit('request', notification)) return;
      return notification;
    };

    function processRequest() {
      // If we have a transport and it's a duplicated request, reply inmediatly
      transport = unifyTransport(transport) || self.getTransport();
      if (transport) {
        var response = responses.get(id, from);
        if (response)
          return transport.send(response.message);
      };

      var idAck = (id != undefined) ? id : ack;
      var request = new RpcRequest(method, params, idAck, from, transport);

      if (self.emit('request', request)) return;
      return request;
    };

    function processResponse(request, error, result) {
      request.callback(error, result);
    };

    function duplicatedResponse(timeout) {
      console.warn("Response already processed", message);

      // Update duplicated responses timeout
      clearTimeout(timeout);
      storeProcessedResponse(ack, from);
    };

    // Request, or response with own method
    if (method) {
      // Check if it's a response with own method
      if (dest == undefined || dest == self.peerID) {
        var request = requests.get(ack, from);
        if (request) {
          var responseMethods = request.responseMethods;

          if (method == responseMethods.error)
            return processResponse(request, params);

          if (method == responseMethods.response)
            return processResponse(request, null, params);

          return processRequest();
        }

        var processed = processedResponses.get(ack, from);
        if (processed)
          return duplicatedResponse(processed);
      }

      // Request
      return processRequest();
    };

    var error = message.error;
    var result = message.result;

    // Ignore responses not send to us
    if (error && error.dest && error.dest != self.peerID) return;
    if (result && result.dest && result.dest != self.peerID) return;

    // Response
    var request = requests.get(ack, from);
    if (!request) {
      var processed = processedResponses.get(ack, from);
      if (processed)
        return duplicatedResponse(processed);

      return console.warn("No callback was defined for this message",
        message);
    };

    // Process response
    processResponse(request, error, result);
  };
};
inherits(RpcBuilder, EventEmitter);

RpcBuilder.RpcNotification = RpcNotification;

module.exports = RpcBuilder;

var clients = require('./clients');
var transports = require('./clients/transports');

RpcBuilder.clients = clients;
RpcBuilder.clients.transports = transports;
RpcBuilder.packers = packers;

},{"./Mapper":1,"./clients":2,"./clients/transports":4,"./packers":9,"events":10,"inherits":11}],7:[function(require,module,exports){
/**
 * JsonRPC 2.0 packer
 */

/**
 * Pack a JsonRPC 2.0 message
 *
 * @param {Object} message - object to be packaged. It requires to have all the
 *   fields needed by the JsonRPC 2.0 message that it's going to be generated
 *
 * @return {String} - the stringified JsonRPC 2.0 message
 */
function pack(message, id) {
  var result = {
    jsonrpc: "2.0"
  };

  // Request
  if (message.method) {
    result.method = message.method;

    if (message.params)
      result.params = message.params;

    // Request is a notification
    if (id != undefined)
      result.id = id;
  }

  // Response
  else if (id != undefined) {
    if (message.error) {
      if (message.result !== undefined)
        throw new TypeError("Both result and error are defined");

      result.error = message.error;
    } else if (message.result !== undefined)
      result.result = message.result;
    else
      throw new TypeError("No result or error is defined");

    result.id = id;
  };

  return JSON.stringify(result);
};

/**
 * Unpack a JsonRPC 2.0 message
 *
 * @param {String} message - string with the content of the JsonRPC 2.0 message
 *
 * @throws {TypeError} - Invalid JsonRPC version
 *
 * @return {Object} - object filled with the JsonRPC 2.0 message content
 */
function unpack(message) {
  var result = message;

  if (typeof message === 'string' || message instanceof String) {
    result = JSON.parse(message);
  }

  // Check if it's a valid message

  var version = result.jsonrpc;
  if (version !== '2.0')
    throw new TypeError("Invalid JsonRPC version '" + version + "': " +
      message);

  // Response
  if (result.method == undefined) {
    if (result.id == undefined)
      throw new TypeError("Invalid message: " + message);

    var result_defined = result.result !== undefined;
    var error_defined = result.error !== undefined;

    // Check only result or error is defined, not both or none
    if (result_defined && error_defined)
      throw new TypeError("Both result and error are defined: " + message);

    if (!result_defined && !error_defined)
      throw new TypeError("No result or error is defined: " + message);

    result.ack = result.id;
    delete result.id;
  }

  // Return unpacked message
  return result;
};

exports.pack = pack;
exports.unpack = unpack;

},{}],8:[function(require,module,exports){
function pack(message) {
  throw new TypeError("Not yet implemented");
};

function unpack(message) {
  throw new TypeError("Not yet implemented");
};

exports.pack = pack;
exports.unpack = unpack;

},{}],9:[function(require,module,exports){
var JsonRPC = require('./JsonRPC');
var XmlRPC = require('./XmlRPC');

exports.JsonRPC = JsonRPC;
exports.XmlRPC = XmlRPC;

},{"./JsonRPC":7,"./XmlRPC":8}],10:[function(require,module,exports){
// Copyright Joyent, Inc. and other Node contributors.
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to permit
// persons to whom the Software is furnished to do so, subject to the
// following conditions:
//
// The above copyright notice and this permission notice shall be included
// in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
// NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
// DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
// OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE
// USE OR OTHER DEALINGS IN THE SOFTWARE.

var objectCreate = Object.create || objectCreatePolyfill
var objectKeys = Object.keys || objectKeysPolyfill
var bind = Function.prototype.bind || functionBindPolyfill

function EventEmitter() {
  if (!this._events || !Object.prototype.hasOwnProperty.call(this, '_events')) {
    this._events = objectCreate(null);
    this._eventsCount = 0;
  }

  this._maxListeners = this._maxListeners || undefined;
}
module.exports = EventEmitter;

// Backwards-compat with node 0.10.x
EventEmitter.EventEmitter = EventEmitter;

EventEmitter.prototype._events = undefined;
EventEmitter.prototype._maxListeners = undefined;

// By default EventEmitters will print a warning if more than 10 listeners are
// added to it. This is a useful default which helps finding memory leaks.
var defaultMaxListeners = 10;

var hasDefineProperty;
try {
  var o = {};
  if (Object.defineProperty) Object.defineProperty(o, 'x', { value: 0 });
  hasDefineProperty = o.x === 0;
} catch (err) { hasDefineProperty = false }
if (hasDefineProperty) {
  Object.defineProperty(EventEmitter, 'defaultMaxListeners', {
    enumerable: true,
    get: function() {
      return defaultMaxListeners;
    },
    set: function(arg) {
      // check whether the input is a positive number (whose value is zero or
      // greater and not a NaN).
      if (typeof arg !== 'number' || arg < 0 || arg !== arg)
        throw new TypeError('"defaultMaxListeners" must be a positive number');
      defaultMaxListeners = arg;
    }
  });
} else {
  EventEmitter.defaultMaxListeners = defaultMaxListeners;
}

// Obviously not all Emitters should be limited to 10. This function allows
// that to be increased. Set to zero for unlimited.
EventEmitter.prototype.setMaxListeners = function setMaxListeners(n) {
  if (typeof n !== 'number' || n < 0 || isNaN(n))
    throw new TypeError('"n" argument must be a positive number');
  this._maxListeners = n;
  return this;
};

function $getMaxListeners(that) {
  if (that._maxListeners === undefined)
    return EventEmitter.defaultMaxListeners;
  return that._maxListeners;
}

EventEmitter.prototype.getMaxListeners = function getMaxListeners() {
  return $getMaxListeners(this);
};

// These standalone emit* functions are used to optimize calling of event
// handlers for fast cases because emit() itself often has a variable number of
// arguments and can be deoptimized because of that. These functions always have
// the same number of arguments and thus do not get deoptimized, so the code
// inside them can execute faster.
function emitNone(handler, isFn, self) {
  if (isFn)
    handler.call(self);
  else {
    var len = handler.length;
    var listeners = arrayClone(handler, len);
    for (var i = 0; i < len; ++i)
      listeners[i].call(self);
  }
}
function emitOne(handler, isFn, self, arg1) {
  if (isFn)
    handler.call(self, arg1);
  else {
    var len = handler.length;
    var listeners = arrayClone(handler, len);
    for (var i = 0; i < len; ++i)
      listeners[i].call(self, arg1);
  }
}
function emitTwo(handler, isFn, self, arg1, arg2) {
  if (isFn)
    handler.call(self, arg1, arg2);
  else {
    var len = handler.length;
    var listeners = arrayClone(handler, len);
    for (var i = 0; i < len; ++i)
      listeners[i].call(self, arg1, arg2);
  }
}
function emitThree(handler, isFn, self, arg1, arg2, arg3) {
  if (isFn)
    handler.call(self, arg1, arg2, arg3);
  else {
    var len = handler.length;
    var listeners = arrayClone(handler, len);
    for (var i = 0; i < len; ++i)
      listeners[i].call(self, arg1, arg2, arg3);
  }
}

function emitMany(handler, isFn, self, args) {
  if (isFn)
    handler.apply(self, args);
  else {
    var len = handler.length;
    var listeners = arrayClone(handler, len);
    for (var i = 0; i < len; ++i)
      listeners[i].apply(self, args);
  }
}

EventEmitter.prototype.emit = function emit(type) {
  var er, handler, len, args, i, events;
  var doError = (type === 'error');

  events = this._events;
  if (events)
    doError = (doError && events.error == null);
  else if (!doError)
    return false;

  // If there is no 'error' event listener then throw.
  if (doError) {
    if (arguments.length > 1)
      er = arguments[1];
    if (er instanceof Error) {
      throw er; // Unhandled 'error' event
    } else {
      // At least give some kind of context to the user
      var err = new Error('Unhandled "error" event. (' + er + ')');
      err.context = er;
      throw err;
    }
    return false;
  }

  handler = events[type];

  if (!handler)
    return false;

  var isFn = typeof handler === 'function';
  len = arguments.length;
  switch (len) {
      // fast cases
    case 1:
      emitNone(handler, isFn, this);
      break;
    case 2:
      emitOne(handler, isFn, this, arguments[1]);
      break;
    case 3:
      emitTwo(handler, isFn, this, arguments[1], arguments[2]);
      break;
    case 4:
      emitThree(handler, isFn, this, arguments[1], arguments[2], arguments[3]);
      break;
      // slower
    default:
      args = new Array(len - 1);
      for (i = 1; i < len; i++)
        args[i - 1] = arguments[i];
      emitMany(handler, isFn, this, args);
  }

  return true;
};

function _addListener(target, type, listener, prepend) {
  var m;
  var events;
  var existing;

  if (typeof listener !== 'function')
    throw new TypeError('"listener" argument must be a function');

  events = target._events;
  if (!events) {
    events = target._events = objectCreate(null);
    target._eventsCount = 0;
  } else {
    // To avoid recursion in the case that type === "newListener"! Before
    // adding it to the listeners, first emit "newListener".
    if (events.newListener) {
      target.emit('newListener', type,
          listener.listener ? listener.listener : listener);

      // Re-assign `events` because a newListener handler could have caused the
      // this._events to be assigned to a new object
      events = target._events;
    }
    existing = events[type];
  }

  if (!existing) {
    // Optimize the case of one listener. Don't need the extra array object.
    existing = events[type] = listener;
    ++target._eventsCount;
  } else {
    if (typeof existing === 'function') {
      // Adding the second element, need to change to array.
      existing = events[type] =
          prepend ? [listener, existing] : [existing, listener];
    } else {
      // If we've already got an array, just append.
      if (prepend) {
        existing.unshift(listener);
      } else {
        existing.push(listener);
      }
    }

    // Check for listener leak
    if (!existing.warned) {
      m = $getMaxListeners(target);
      if (m && m > 0 && existing.length > m) {
        existing.warned = true;
        var w = new Error('Possible EventEmitter memory leak detected. ' +
            existing.length + ' "' + String(type) + '" listeners ' +
            'added. Use emitter.setMaxListeners() to ' +
            'increase limit.');
        w.name = 'MaxListenersExceededWarning';
        w.emitter = target;
        w.type = type;
        w.count = existing.length;
        if (typeof console === 'object' && console.warn) {
          console.warn('%s: %s', w.name, w.message);
        }
      }
    }
  }

  return target;
}

EventEmitter.prototype.addListener = function addListener(type, listener) {
  return _addListener(this, type, listener, false);
};

EventEmitter.prototype.on = EventEmitter.prototype.addListener;

EventEmitter.prototype.prependListener =
    function prependListener(type, listener) {
      return _addListener(this, type, listener, true);
    };

function onceWrapper() {
  if (!this.fired) {
    this.target.removeListener(this.type, this.wrapFn);
    this.fired = true;
    switch (arguments.length) {
      case 0:
        return this.listener.call(this.target);
      case 1:
        return this.listener.call(this.target, arguments[0]);
      case 2:
        return this.listener.call(this.target, arguments[0], arguments[1]);
      case 3:
        return this.listener.call(this.target, arguments[0], arguments[1],
            arguments[2]);
      default:
        var args = new Array(arguments.length);
        for (var i = 0; i < args.length; ++i)
          args[i] = arguments[i];
        this.listener.apply(this.target, args);
    }
  }
}

function _onceWrap(target, type, listener) {
  var state = { fired: false, wrapFn: undefined, target: target, type: type, listener: listener };
  var wrapped = bind.call(onceWrapper, state);
  wrapped.listener = listener;
  state.wrapFn = wrapped;
  return wrapped;
}

EventEmitter.prototype.once = function once(type, listener) {
  if (typeof listener !== 'function')
    throw new TypeError('"listener" argument must be a function');
  this.on(type, _onceWrap(this, type, listener));
  return this;
};

EventEmitter.prototype.prependOnceListener =
    function prependOnceListener(type, listener) {
      if (typeof listener !== 'function')
        throw new TypeError('"listener" argument must be a function');
      this.prependListener(type, _onceWrap(this, type, listener));
      return this;
    };

// Emits a 'removeListener' event if and only if the listener was removed.
EventEmitter.prototype.removeListener =
    function removeListener(type, listener) {
      var list, events, position, i, originalListener;

      if (typeof listener !== 'function')
        throw new TypeError('"listener" argument must be a function');

      events = this._events;
      if (!events)
        return this;

      list = events[type];
      if (!list)
        return this;

      if (list === listener || list.listener === listener) {
        if (--this._eventsCount === 0)
          this._events = objectCreate(null);
        else {
          delete events[type];
          if (events.removeListener)
            this.emit('removeListener', type, list.listener || listener);
        }
      } else if (typeof list !== 'function') {
        position = -1;

        for (i = list.length - 1; i >= 0; i--) {
          if (list[i] === listener || list[i].listener === listener) {
            originalListener = list[i].listener;
            position = i;
            break;
          }
        }

        if (position < 0)
          return this;

        if (position === 0)
          list.shift();
        else
          spliceOne(list, position);

        if (list.length === 1)
          events[type] = list[0];

        if (events.removeListener)
          this.emit('removeListener', type, originalListener || listener);
      }

      return this;
    };

EventEmitter.prototype.removeAllListeners =
    function removeAllListeners(type) {
      var listeners, events, i;

      events = this._events;
      if (!events)
        return this;

      // not listening for removeListener, no need to emit
      if (!events.removeListener) {
        if (arguments.length === 0) {
          this._events = objectCreate(null);
          this._eventsCount = 0;
        } else if (events[type]) {
          if (--this._eventsCount === 0)
            this._events = objectCreate(null);
          else
            delete events[type];
        }
        return this;
      }

      // emit removeListener for all listeners on all events
      if (arguments.length === 0) {
        var keys = objectKeys(events);
        var key;
        for (i = 0; i < keys.length; ++i) {
          key = keys[i];
          if (key === 'removeListener') continue;
          this.removeAllListeners(key);
        }
        this.removeAllListeners('removeListener');
        this._events = objectCreate(null);
        this._eventsCount = 0;
        return this;
      }

      listeners = events[type];

      if (typeof listeners === 'function') {
        this.removeListener(type, listeners);
      } else if (listeners) {
        // LIFO order
        for (i = listeners.length - 1; i >= 0; i--) {
          this.removeListener(type, listeners[i]);
        }
      }

      return this;
    };

function _listeners(target, type, unwrap) {
  var events = target._events;

  if (!events)
    return [];

  var evlistener = events[type];
  if (!evlistener)
    return [];

  if (typeof evlistener === 'function')
    return unwrap ? [evlistener.listener || evlistener] : [evlistener];

  return unwrap ? unwrapListeners(evlistener) : arrayClone(evlistener, evlistener.length);
}

EventEmitter.prototype.listeners = function listeners(type) {
  return _listeners(this, type, true);
};

EventEmitter.prototype.rawListeners = function rawListeners(type) {
  return _listeners(this, type, false);
};

EventEmitter.listenerCount = function(emitter, type) {
  if (typeof emitter.listenerCount === 'function') {
    return emitter.listenerCount(type);
  } else {
    return listenerCount.call(emitter, type);
  }
};

EventEmitter.prototype.listenerCount = listenerCount;
function listenerCount(type) {
  var events = this._events;

  if (events) {
    var evlistener = events[type];

    if (typeof evlistener === 'function') {
      return 1;
    } else if (evlistener) {
      return evlistener.length;
    }
  }

  return 0;
}

EventEmitter.prototype.eventNames = function eventNames() {
  return this._eventsCount > 0 ? Reflect.ownKeys(this._events) : [];
};

// About 1.5x faster than the two-arg version of Array#splice().
function spliceOne(list, index) {
  for (var i = index, k = i + 1, n = list.length; k < n; i += 1, k += 1)
    list[i] = list[k];
  list.pop();
}

function arrayClone(arr, n) {
  var copy = new Array(n);
  for (var i = 0; i < n; ++i)
    copy[i] = arr[i];
  return copy;
}

function unwrapListeners(arr) {
  var ret = new Array(arr.length);
  for (var i = 0; i < ret.length; ++i) {
    ret[i] = arr[i].listener || arr[i];
  }
  return ret;
}

function objectCreatePolyfill(proto) {
  var F = function() {};
  F.prototype = proto;
  return new F;
}
function objectKeysPolyfill(obj) {
  var keys = [];
  for (var k in obj) if (Object.prototype.hasOwnProperty.call(obj, k)) {
    keys.push(k);
  }
  return k;
}
function functionBindPolyfill(context) {
  var fn = this;
  return function () {
    return fn.apply(context, arguments);
  };
}

},{}],11:[function(require,module,exports){
if (typeof Object.create === 'function') {
  // implementation from standard node.js 'util' module
  module.exports = function inherits(ctor, superCtor) {
    if (superCtor) {
      ctor.super_ = superCtor
      ctor.prototype = Object.create(superCtor.prototype, {
        constructor: {
          value: ctor,
          enumerable: false,
          writable: true,
          configurable: true
        }
      })
    }
  };
} else {
  // old school shim for old browsers
  module.exports = function inherits(ctor, superCtor) {
    if (superCtor) {
      ctor.super_ = superCtor
      var TempCtor = function () {}
      TempCtor.prototype = superCtor.prototype
      ctor.prototype = new TempCtor()
      ctor.prototype.constructor = ctor
    }
  }
}

},{}]},{},[6])(6)
});

//# sourceMappingURL=data:application/json;charset=utf-8;base64,eyJ2ZXJzaW9uIjozLCJzb3VyY2VzIjpbIm5vZGVfbW9kdWxlcy9icm93c2VyLXBhY2svX3ByZWx1ZGUuanMiLCJsaWIvTWFwcGVyLmpzIiwibGliL2NsaWVudHMvaW5kZXguanMiLCJsaWIvY2xpZW50cy9qc29ucnBjY2xpZW50LmpzIiwibGliL2NsaWVudHMvdHJhbnNwb3J0cy9pbmRleC5qcyIsImxpYi9jbGllbnRzL3RyYW5zcG9ydHMvd2ViU29ja2V0V2l0aFJlY29ubmVjdGlvbi5qcyIsImxpYi9pbmRleC5qcyIsImxpYi9wYWNrZXJzL0pzb25SUEMuanMiLCJsaWIvcGFja2Vycy9YbWxSUEMuanMiLCJsaWIvcGFja2Vycy9pbmRleC5qcyIsIm5vZGVfbW9kdWxlcy9ncnVudC1icm93c2VyaWZ5L25vZGVfbW9kdWxlcy9ldmVudHMvZXZlbnRzLmpzIiwibm9kZV9tb2R1bGVzL2luaGVyaXRzL2luaGVyaXRzX2Jyb3dzZXIuanMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6IkFBQUE7QUNBQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBOztBQzFEQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7O0FDcEJBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7O0FDOVJBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTs7O0FDcEJBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBOzs7O0FDdFBBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTs7QUNqdUJBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTs7QUMvRkE7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTs7QUNWQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7O0FDTEE7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTs7QUMzZ0JBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBIiwiZmlsZSI6ImdlbmVyYXRlZC5qcyIsInNvdXJjZVJvb3QiOiIiLCJzb3VyY2VzQ29udGVudCI6WyIoZnVuY3Rpb24oKXtmdW5jdGlvbiByKGUsbix0KXtmdW5jdGlvbiBvKGksZil7aWYoIW5baV0pe2lmKCFlW2ldKXt2YXIgYz1cImZ1bmN0aW9uXCI9PXR5cGVvZiByZXF1aXJlJiZyZXF1aXJlO2lmKCFmJiZjKXJldHVybiBjKGksITApO2lmKHUpcmV0dXJuIHUoaSwhMCk7dmFyIGE9bmV3IEVycm9yKFwiQ2Fubm90IGZpbmQgbW9kdWxlICdcIitpK1wiJ1wiKTt0aHJvdyBhLmNvZGU9XCJNT0RVTEVfTk9UX0ZPVU5EXCIsYX12YXIgcD1uW2ldPXtleHBvcnRzOnt9fTtlW2ldWzBdLmNhbGwocC5leHBvcnRzLGZ1bmN0aW9uKHIpe3ZhciBuPWVbaV1bMV1bcl07cmV0dXJuIG8obnx8cil9LHAscC5leHBvcnRzLHIsZSxuLHQpfXJldHVybiBuW2ldLmV4cG9ydHN9Zm9yKHZhciB1PVwiZnVuY3Rpb25cIj09dHlwZW9mIHJlcXVpcmUmJnJlcXVpcmUsaT0wO2k8dC5sZW5ndGg7aSsrKW8odFtpXSk7cmV0dXJuIG99cmV0dXJuIHJ9KSgpIiwiZnVuY3Rpb24gTWFwcGVyKCkge1xuICB2YXIgc291cmNlcyA9IHt9O1xuXG4gIHRoaXMuZm9yRWFjaCA9IGZ1bmN0aW9uIChjYWxsYmFjaykge1xuICAgIGZvciAodmFyIGtleSBpbiBzb3VyY2VzKSB7XG4gICAgICB2YXIgc291cmNlID0gc291cmNlc1trZXldO1xuXG4gICAgICBmb3IgKHZhciBrZXkyIGluIHNvdXJjZSlcbiAgICAgICAgY2FsbGJhY2soc291cmNlW2tleTJdKTtcbiAgICB9O1xuICB9O1xuXG4gIHRoaXMuZ2V0ID0gZnVuY3Rpb24gKGlkLCBzb3VyY2UpIHtcbiAgICB2YXIgaWRzID0gc291cmNlc1tzb3VyY2VdO1xuICAgIGlmIChpZHMgPT0gdW5kZWZpbmVkKVxuICAgICAgcmV0dXJuIHVuZGVmaW5lZDtcblxuICAgIHJldHVybiBpZHNbaWRdO1xuICB9O1xuXG4gIHRoaXMucmVtb3ZlID0gZnVuY3Rpb24gKGlkLCBzb3VyY2UpIHtcbiAgICB2YXIgaWRzID0gc291cmNlc1tzb3VyY2VdO1xuICAgIGlmIChpZHMgPT0gdW5kZWZpbmVkKVxuICAgICAgcmV0dXJuO1xuXG4gICAgZGVsZXRlIGlkc1tpZF07XG5cbiAgICAvLyBDaGVjayBpdCdzIGVtcHR5XG4gICAgZm9yICh2YXIgaSBpbiBpZHMpIHtcbiAgICAgIHJldHVybiBmYWxzZVxuICAgIH1cblxuICAgIGRlbGV0ZSBzb3VyY2VzW3NvdXJjZV07XG4gIH07XG5cbiAgdGhpcy5zZXQgPSBmdW5jdGlvbiAodmFsdWUsIGlkLCBzb3VyY2UpIHtcbiAgICBpZiAodmFsdWUgPT0gdW5kZWZpbmVkKVxuICAgICAgcmV0dXJuIHRoaXMucmVtb3ZlKGlkLCBzb3VyY2UpO1xuXG4gICAgdmFyIGlkcyA9IHNvdXJjZXNbc291cmNlXTtcbiAgICBpZiAoaWRzID09IHVuZGVmaW5lZClcbiAgICAgIHNvdXJjZXNbc291cmNlXSA9IGlkcyA9IHt9O1xuXG4gICAgaWRzW2lkXSA9IHZhbHVlO1xuICB9O1xufTtcblxuTWFwcGVyLnByb3RvdHlwZS5wb3AgPSBmdW5jdGlvbiAoaWQsIHNvdXJjZSkge1xuICB2YXIgdmFsdWUgPSB0aGlzLmdldChpZCwgc291cmNlKTtcbiAgaWYgKHZhbHVlID09IHVuZGVmaW5lZClcbiAgICByZXR1cm4gdW5kZWZpbmVkO1xuXG4gIHRoaXMucmVtb3ZlKGlkLCBzb3VyY2UpO1xuXG4gIHJldHVybiB2YWx1ZTtcbn07XG5cbm1vZHVsZS5leHBvcnRzID0gTWFwcGVyO1xuIiwiLypcbiAqIChDKSBDb3B5cmlnaHQgMjAxNCBLdXJlbnRvIChodHRwOi8va3VyZW50by5vcmcvKVxuICpcbiAqIExpY2Vuc2VkIHVuZGVyIHRoZSBBcGFjaGUgTGljZW5zZSwgVmVyc2lvbiAyLjAgKHRoZSBcIkxpY2Vuc2VcIik7XG4gKiB5b3UgbWF5IG5vdCB1c2UgdGhpcyBmaWxlIGV4Y2VwdCBpbiBjb21wbGlhbmNlIHdpdGggdGhlIExpY2Vuc2UuXG4gKiBZb3UgbWF5IG9idGFpbiBhIGNvcHkgb2YgdGhlIExpY2Vuc2UgYXRcbiAqXG4gKiAgIGh0dHA6Ly93d3cuYXBhY2hlLm9yZy9saWNlbnNlcy9MSUNFTlNFLTIuMFxuICpcbiAqIFVubGVzcyByZXF1aXJlZCBieSBhcHBsaWNhYmxlIGxhdyBvciBhZ3JlZWQgdG8gaW4gd3JpdGluZywgc29mdHdhcmVcbiAqIGRpc3RyaWJ1dGVkIHVuZGVyIHRoZSBMaWNlbnNlIGlzIGRpc3RyaWJ1dGVkIG9uIGFuIFwiQVMgSVNcIiBCQVNJUyxcbiAqIFdJVEhPVVQgV0FSUkFOVElFUyBPUiBDT05ESVRJT05TIE9GIEFOWSBLSU5ELCBlaXRoZXIgZXhwcmVzcyBvciBpbXBsaWVkLlxuICogU2VlIHRoZSBMaWNlbnNlIGZvciB0aGUgc3BlY2lmaWMgbGFuZ3VhZ2UgZ292ZXJuaW5nIHBlcm1pc3Npb25zIGFuZFxuICogbGltaXRhdGlvbnMgdW5kZXIgdGhlIExpY2Vuc2UuXG4gKlxuICovXG5cbnZhciBKc29uUnBjQ2xpZW50ID0gcmVxdWlyZSgnLi9qc29ucnBjY2xpZW50Jyk7XG5cbmV4cG9ydHMuSnNvblJwY0NsaWVudCA9IEpzb25ScGNDbGllbnQ7XG4iLCIvKlxuICogKEMpIENvcHlyaWdodCAyMDE0IEt1cmVudG8gKGh0dHA6Ly9rdXJlbnRvLm9yZy8pXG4gKlxuICogTGljZW5zZWQgdW5kZXIgdGhlIEFwYWNoZSBMaWNlbnNlLCBWZXJzaW9uIDIuMCAodGhlIFwiTGljZW5zZVwiKTtcbiAqIHlvdSBtYXkgbm90IHVzZSB0aGlzIGZpbGUgZXhjZXB0IGluIGNvbXBsaWFuY2Ugd2l0aCB0aGUgTGljZW5zZS5cbiAqIFlvdSBtYXkgb2J0YWluIGEgY29weSBvZiB0aGUgTGljZW5zZSBhdFxuICpcbiAqICAgaHR0cDovL3d3dy5hcGFjaGUub3JnL2xpY2Vuc2VzL0xJQ0VOU0UtMi4wXG4gKlxuICogVW5sZXNzIHJlcXVpcmVkIGJ5IGFwcGxpY2FibGUgbGF3IG9yIGFncmVlZCB0byBpbiB3cml0aW5nLCBzb2Z0d2FyZVxuICogZGlzdHJpYnV0ZWQgdW5kZXIgdGhlIExpY2Vuc2UgaXMgZGlzdHJpYnV0ZWQgb24gYW4gXCJBUyBJU1wiIEJBU0lTLFxuICogV0lUSE9VVCBXQVJSQU5USUVTIE9SIENPTkRJVElPTlMgT0YgQU5ZIEtJTkQsIGVpdGhlciBleHByZXNzIG9yIGltcGxpZWQuXG4gKiBTZWUgdGhlIExpY2Vuc2UgZm9yIHRoZSBzcGVjaWZpYyBsYW5ndWFnZSBnb3Zlcm5pbmcgcGVybWlzc2lvbnMgYW5kXG4gKiBsaW1pdGF0aW9ucyB1bmRlciB0aGUgTGljZW5zZS5cbiAqXG4gKi9cblxudmFyIFJwY0J1aWxkZXIgPSByZXF1aXJlKCcuLi8uLicpO1xudmFyIFdlYlNvY2tldFdpdGhSZWNvbm5lY3Rpb24gPSByZXF1aXJlKFxuICAnLi90cmFuc3BvcnRzL3dlYlNvY2tldFdpdGhSZWNvbm5lY3Rpb24nKTtcblxuRGF0ZS5ub3cgPSBEYXRlLm5vdyB8fCBmdW5jdGlvbiAoKSB7XG4gIHJldHVybiArbmV3IERhdGU7XG59O1xuXG52YXIgUElOR19JTlRFUlZBTCA9IDUwMDA7XG5cbnZhciBSRUNPTk5FQ1RJTkcgPSAnUkVDT05ORUNUSU5HJztcbnZhciBDT05ORUNURUQgPSAnQ09OTkVDVEVEJztcbnZhciBESVNDT05ORUNURUQgPSAnRElTQ09OTkVDVEVEJztcblxudmFyIExvZ2dlciA9IGNvbnNvbGU7XG5cbi8qKlxuICpcbiAqIGhlYXJ0YmVhdDogaW50ZXJ2YWwgaW4gbXMgZm9yIGVhY2ggaGVhcnRiZWF0IG1lc3NhZ2UsXG4gKiBzZW5kQ2xvc2VNZXNzYWdlIDogdHJ1ZSAvIGZhbHNlLCBiZWZvcmUgY2xvc2luZyB0aGUgY29ubmVjdGlvbiwgaXQgc2VuZHMgYSBjbG9zZVNlc3Npb24gbWVzc2FnZVxuICogPHByZT5cbiAqIHdzIDoge1xuICogXHR1cmkgOiBVUkkgdG8gY29ubnRlY3QgdG8sXG4gKiAgdXNlU29ja0pTIDogdHJ1ZSAodXNlIFNvY2tKUykgLyBmYWxzZSAodXNlIFdlYlNvY2tldCkgYnkgZGVmYXVsdCxcbiAqIFx0b25jb25uZWN0ZWQgOiBjYWxsYmFjayBtZXRob2QgdG8gaW52b2tlIHdoZW4gY29ubmVjdGlvbiBpcyBzdWNjZXNzZnVsLFxuICogXHRvbmRpc2Nvbm5lY3QgOiBjYWxsYmFjayBtZXRob2QgdG8gaW52b2tlIHdoZW4gdGhlIGNvbm5lY3Rpb24gaXMgbG9zdCxcbiAqIFx0b25yZWNvbm5lY3RpbmcgOiBjYWxsYmFjayBtZXRob2QgdG8gaW52b2tlIHdoZW4gdGhlIGNsaWVudCBpcyByZWNvbm5lY3RpbmcsXG4gKiBcdG9ucmVjb25uZWN0ZWQgOiBjYWxsYmFjayBtZXRob2QgdG8gaW52b2tlIHdoZW4gdGhlIGNsaWVudCBzdWNjZXNmdWxseSByZWNvbm5lY3RzLFxuICogXHRvbmVycm9yIDogY2FsbGJhY2sgbWV0aG9kIHRvIGludm9rZSB3aGVuIHRoZXJlIGlzIGFuIGVycm9yXG4gKiB9LFxuICogcnBjIDoge1xuICogXHRyZXF1ZXN0VGltZW91dCA6IHRpbWVvdXQgZm9yIGEgcmVxdWVzdCxcbiAqIFx0c2Vzc2lvblN0YXR1c0NoYW5nZWQ6IGNhbGxiYWNrIG1ldGhvZCBmb3IgY2hhbmdlcyBpbiBzZXNzaW9uIHN0YXR1cyxcbiAqIFx0bWVkaWFSZW5lZ290aWF0aW9uOiBtZWRpYVJlbmVnb3RpYXRpb25cbiAqIH1cbiAqIDwvcHJlPlxuICovXG5mdW5jdGlvbiBKc29uUnBjQ2xpZW50KGNvbmZpZ3VyYXRpb24pIHtcblxuICB2YXIgc2VsZiA9IHRoaXM7XG5cbiAgdmFyIHdzQ29uZmlnID0gY29uZmlndXJhdGlvbi53cztcblxuICB2YXIgbm90UmVjb25uZWN0SWZOdW1MZXNzVGhhbiA9IC0xO1xuXG4gIHZhciBwaW5nTmV4dE51bSA9IDA7XG4gIHZhciBlbmFibGVkUGluZ3MgPSB0cnVlO1xuICB2YXIgcGluZ1BvbmdTdGFydGVkID0gZmFsc2U7XG4gIHZhciBwaW5nSW50ZXJ2YWw7XG5cbiAgdmFyIHN0YXR1cyA9IERJU0NPTk5FQ1RFRDtcblxuICB2YXIgb25yZWNvbm5lY3RpbmcgPSB3c0NvbmZpZy5vbnJlY29ubmVjdGluZztcbiAgdmFyIG9ucmVjb25uZWN0ZWQgPSB3c0NvbmZpZy5vbnJlY29ubmVjdGVkO1xuICB2YXIgb25jb25uZWN0ZWQgPSB3c0NvbmZpZy5vbmNvbm5lY3RlZDtcbiAgdmFyIG9uZXJyb3IgPSB3c0NvbmZpZy5vbmVycm9yO1xuXG4gIGNvbmZpZ3VyYXRpb24ucnBjLnB1bGwgPSBmdW5jdGlvbiAocGFyYW1zLCByZXF1ZXN0KSB7XG4gICAgcmVxdWVzdC5yZXBseShudWxsLCBcInB1c2hcIik7XG4gIH1cblxuICB3c0NvbmZpZy5vbnJlY29ubmVjdGluZyA9IGZ1bmN0aW9uICgpIHtcbiAgICBMb2dnZXIuZGVidWcoXCItLS0tLS0tLS0gT05SRUNPTk5FQ1RJTkcgLS0tLS0tLS0tLS1cIik7XG4gICAgaWYgKHN0YXR1cyA9PT0gUkVDT05ORUNUSU5HKSB7XG4gICAgICBMb2dnZXIuZXJyb3IoXG4gICAgICAgIFwiV2Vic29ja2V0IGFscmVhZHkgaW4gUkVDT05ORUNUSU5HIHN0YXRlIHdoZW4gcmVjZWl2aW5nIGEgbmV3IE9OUkVDT05ORUNUSU5HIG1lc3NhZ2UuIElnbm9yaW5nIGl0XCJcbiAgICAgICk7XG4gICAgICByZXR1cm47XG4gICAgfVxuXG4gICAgc3RhdHVzID0gUkVDT05ORUNUSU5HO1xuICAgIGlmIChvbnJlY29ubmVjdGluZykge1xuICAgICAgb25yZWNvbm5lY3RpbmcoKTtcbiAgICB9XG4gIH1cblxuICB3c0NvbmZpZy5vbnJlY29ubmVjdGVkID0gZnVuY3Rpb24gKCkge1xuICAgIExvZ2dlci5kZWJ1ZyhcIi0tLS0tLS0tLSBPTlJFQ09OTkVDVEVEIC0tLS0tLS0tLS0tXCIpO1xuICAgIGlmIChzdGF0dXMgPT09IENPTk5FQ1RFRCkge1xuICAgICAgTG9nZ2VyLmVycm9yKFxuICAgICAgICBcIldlYnNvY2tldCBhbHJlYWR5IGluIENPTk5FQ1RFRCBzdGF0ZSB3aGVuIHJlY2VpdmluZyBhIG5ldyBPTlJFQ09OTkVDVEVEIG1lc3NhZ2UuIElnbm9yaW5nIGl0XCJcbiAgICAgICk7XG4gICAgICByZXR1cm47XG4gICAgfVxuICAgIHN0YXR1cyA9IENPTk5FQ1RFRDtcblxuICAgIGVuYWJsZWRQaW5ncyA9IHRydWU7XG4gICAgdXBkYXRlTm90UmVjb25uZWN0SWZMZXNzVGhhbigpO1xuICAgIHVzZVBpbmcoKTtcblxuICAgIGlmIChvbnJlY29ubmVjdGVkKSB7XG4gICAgICBvbnJlY29ubmVjdGVkKCk7XG4gICAgfVxuICB9XG5cbiAgd3NDb25maWcub25jb25uZWN0ZWQgPSBmdW5jdGlvbiAoKSB7XG4gICAgTG9nZ2VyLmRlYnVnKFwiLS0tLS0tLS0tIE9OQ09OTkVDVEVEIC0tLS0tLS0tLS0tXCIpO1xuICAgIGlmIChzdGF0dXMgPT09IENPTk5FQ1RFRCkge1xuICAgICAgTG9nZ2VyLmVycm9yKFxuICAgICAgICBcIldlYnNvY2tldCBhbHJlYWR5IGluIENPTk5FQ1RFRCBzdGF0ZSB3aGVuIHJlY2VpdmluZyBhIG5ldyBPTkNPTk5FQ1RFRCBtZXNzYWdlLiBJZ25vcmluZyBpdFwiXG4gICAgICApO1xuICAgICAgcmV0dXJuO1xuICAgIH1cbiAgICBzdGF0dXMgPSBDT05ORUNURUQ7XG5cbiAgICBlbmFibGVkUGluZ3MgPSB0cnVlO1xuICAgIHVzZVBpbmcoKTtcblxuICAgIGlmIChvbmNvbm5lY3RlZCkge1xuICAgICAgb25jb25uZWN0ZWQoKTtcbiAgICB9XG4gIH1cblxuICB3c0NvbmZpZy5vbmVycm9yID0gZnVuY3Rpb24gKGVycm9yKSB7XG4gICAgTG9nZ2VyLmRlYnVnKFwiLS0tLS0tLS0tIE9ORVJST1IgLS0tLS0tLS0tLS1cIik7XG5cbiAgICBzdGF0dXMgPSBESVNDT05ORUNURUQ7XG5cbiAgICBpZiAob25lcnJvcikge1xuICAgICAgb25lcnJvcihlcnJvcik7XG4gICAgfVxuICB9XG5cbiAgdmFyIHdzID0gbmV3IFdlYlNvY2tldFdpdGhSZWNvbm5lY3Rpb24od3NDb25maWcpO1xuXG4gIExvZ2dlci5kZWJ1ZygnQ29ubmVjdGluZyB3ZWJzb2NrZXQgdG8gVVJJOiAnICsgd3NDb25maWcudXJpKTtcblxuICB2YXIgcnBjQnVpbGRlck9wdGlvbnMgPSB7XG4gICAgcmVxdWVzdF90aW1lb3V0OiBjb25maWd1cmF0aW9uLnJwYy5yZXF1ZXN0VGltZW91dCxcbiAgICBwaW5nX3JlcXVlc3RfdGltZW91dDogY29uZmlndXJhdGlvbi5ycGMuaGVhcnRiZWF0UmVxdWVzdFRpbWVvdXRcbiAgfTtcblxuICB2YXIgcnBjID0gbmV3IFJwY0J1aWxkZXIoUnBjQnVpbGRlci5wYWNrZXJzLkpzb25SUEMsIHJwY0J1aWxkZXJPcHRpb25zLCB3cyxcbiAgICBmdW5jdGlvbiAocmVxdWVzdCkge1xuXG4gICAgICBMb2dnZXIuZGVidWcoJ1JlY2VpdmVkIHJlcXVlc3Q6ICcgKyBKU09OLnN0cmluZ2lmeShyZXF1ZXN0KSk7XG5cbiAgICAgIHRyeSB7XG4gICAgICAgIHZhciBmdW5jID0gY29uZmlndXJhdGlvbi5ycGNbcmVxdWVzdC5tZXRob2RdO1xuXG4gICAgICAgIGlmIChmdW5jID09PSB1bmRlZmluZWQpIHtcbiAgICAgICAgICBMb2dnZXIuZXJyb3IoXCJNZXRob2QgXCIgKyByZXF1ZXN0Lm1ldGhvZCArXG4gICAgICAgICAgICBcIiBub3QgcmVnaXN0ZXJlZCBpbiBjbGllbnRcIik7XG4gICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgZnVuYyhyZXF1ZXN0LnBhcmFtcywgcmVxdWVzdCk7XG4gICAgICAgIH1cbiAgICAgIH0gY2F0Y2ggKGVycikge1xuICAgICAgICBMb2dnZXIuZXJyb3IoJ0V4Y2VwdGlvbiBwcm9jZXNzaW5nIHJlcXVlc3Q6ICcgKyBKU09OLnN0cmluZ2lmeShcbiAgICAgICAgICByZXF1ZXN0KSk7XG4gICAgICAgIExvZ2dlci5lcnJvcihlcnIpO1xuICAgICAgfVxuICAgIH0pO1xuXG4gIHRoaXMuc2VuZCA9IGZ1bmN0aW9uIChtZXRob2QsIHBhcmFtcywgY2FsbGJhY2spIHtcbiAgICBpZiAobWV0aG9kICE9PSAncGluZycpIHtcbiAgICAgIExvZ2dlci5kZWJ1ZygnUmVxdWVzdDogbWV0aG9kOicgKyBtZXRob2QgKyBcIiBwYXJhbXM6XCIgKyBKU09OLnN0cmluZ2lmeShcbiAgICAgICAgcGFyYW1zKSk7XG4gICAgfVxuXG4gICAgdmFyIHJlcXVlc3RUaW1lID0gRGF0ZS5ub3coKTtcblxuICAgIHJwYy5lbmNvZGUobWV0aG9kLCBwYXJhbXMsIGZ1bmN0aW9uIChlcnJvciwgcmVzdWx0KSB7XG4gICAgICBpZiAoZXJyb3IpIHtcbiAgICAgICAgdHJ5IHtcbiAgICAgICAgICBMb2dnZXIuZXJyb3IoXCJFUlJPUjpcIiArIGVycm9yLm1lc3NhZ2UgKyBcIiBpbiBSZXF1ZXN0OiBtZXRob2Q6XCIgK1xuICAgICAgICAgICAgbWV0aG9kICsgXCIgcGFyYW1zOlwiICsgSlNPTi5zdHJpbmdpZnkocGFyYW1zKSArIFwiIHJlcXVlc3Q6XCIgK1xuICAgICAgICAgICAgZXJyb3IucmVxdWVzdCk7XG4gICAgICAgICAgaWYgKGVycm9yLmRhdGEpIHtcbiAgICAgICAgICAgIExvZ2dlci5lcnJvcihcIkVSUk9SIERBVEE6XCIgKyBKU09OLnN0cmluZ2lmeShlcnJvci5kYXRhKSk7XG4gICAgICAgICAgfVxuICAgICAgICB9IGNhdGNoIChlKSB7fVxuICAgICAgICBlcnJvci5yZXF1ZXN0VGltZSA9IHJlcXVlc3RUaW1lO1xuICAgICAgfVxuICAgICAgaWYgKGNhbGxiYWNrKSB7XG4gICAgICAgIGlmIChyZXN1bHQgIT0gdW5kZWZpbmVkICYmIHJlc3VsdC52YWx1ZSAhPT0gJ3BvbmcnKSB7XG4gICAgICAgICAgTG9nZ2VyLmRlYnVnKCdSZXNwb25zZTogJyArIEpTT04uc3RyaW5naWZ5KHJlc3VsdCkpO1xuICAgICAgICB9XG4gICAgICAgIGNhbGxiYWNrKGVycm9yLCByZXN1bHQpO1xuICAgICAgfVxuICAgIH0pO1xuICB9XG5cbiAgZnVuY3Rpb24gdXBkYXRlTm90UmVjb25uZWN0SWZMZXNzVGhhbigpIHtcbiAgICBMb2dnZXIuZGVidWcoXCJub3RSZWNvbm5lY3RJZk51bUxlc3NUaGFuID0gXCIgKyBwaW5nTmV4dE51bSArICcgKG9sZD0nICtcbiAgICAgIG5vdFJlY29ubmVjdElmTnVtTGVzc1RoYW4gKyAnKScpO1xuICAgIG5vdFJlY29ubmVjdElmTnVtTGVzc1RoYW4gPSBwaW5nTmV4dE51bTtcbiAgfVxuXG4gIGZ1bmN0aW9uIHNlbmRQaW5nKCkge1xuICAgIGlmIChlbmFibGVkUGluZ3MpIHtcbiAgICAgIHZhciBwYXJhbXMgPSBudWxsO1xuICAgICAgaWYgKHBpbmdOZXh0TnVtID09IDAgfHwgcGluZ05leHROdW0gPT0gbm90UmVjb25uZWN0SWZOdW1MZXNzVGhhbikge1xuICAgICAgICBwYXJhbXMgPSB7XG4gICAgICAgICAgaW50ZXJ2YWw6IGNvbmZpZ3VyYXRpb24uaGVhcnRiZWF0IHx8IFBJTkdfSU5URVJWQUxcbiAgICAgICAgfTtcbiAgICAgIH1cbiAgICAgIHBpbmdOZXh0TnVtKys7XG5cbiAgICAgIHNlbGYuc2VuZCgncGluZycsIHBhcmFtcywgKGZ1bmN0aW9uIChwaW5nTnVtKSB7XG4gICAgICAgIHJldHVybiBmdW5jdGlvbiAoZXJyb3IsIHJlc3VsdCkge1xuICAgICAgICAgIGlmIChlcnJvcikge1xuICAgICAgICAgICAgTG9nZ2VyLmRlYnVnKFwiRXJyb3IgaW4gcGluZyByZXF1ZXN0ICNcIiArIHBpbmdOdW0gKyBcIiAoXCIgK1xuICAgICAgICAgICAgICBlcnJvci5tZXNzYWdlICsgXCIpXCIpO1xuICAgICAgICAgICAgaWYgKHBpbmdOdW0gPiBub3RSZWNvbm5lY3RJZk51bUxlc3NUaGFuKSB7XG4gICAgICAgICAgICAgIGVuYWJsZWRQaW5ncyA9IGZhbHNlO1xuICAgICAgICAgICAgICB1cGRhdGVOb3RSZWNvbm5lY3RJZkxlc3NUaGFuKCk7XG4gICAgICAgICAgICAgIExvZ2dlci5kZWJ1ZyhcIlNlcnZlciBkaWQgbm90IHJlc3BvbmQgdG8gcGluZyBtZXNzYWdlICNcIiArXG4gICAgICAgICAgICAgICAgcGluZ051bSArIFwiLiBSZWNvbm5lY3RpbmcuLi4gXCIpO1xuICAgICAgICAgICAgICB3cy5yZWNvbm5lY3RXcygpO1xuICAgICAgICAgICAgfVxuICAgICAgICAgIH1cbiAgICAgICAgfVxuICAgICAgfSkocGluZ05leHROdW0pKTtcbiAgICB9IGVsc2Uge1xuICAgICAgTG9nZ2VyLmRlYnVnKFwiVHJ5aW5nIHRvIHNlbmQgcGluZywgYnV0IHBpbmcgaXMgbm90IGVuYWJsZWRcIik7XG4gICAgfVxuICB9XG5cbiAgLypcbiAgICogSWYgY29uZmlndXJhdGlvbi5oZWFyYmVhdCBoYXMgYW55IHZhbHVlLCB0aGUgcGluZy1wb25nIHdpbGwgd29yayB3aXRoIHRoZSBpbnRlcnZhbFxuICAgKiBvZiBjb25maWd1cmF0aW9uLmhlYXJiZWF0XG4gICAqL1xuICBmdW5jdGlvbiB1c2VQaW5nKCkge1xuICAgIGlmICghcGluZ1BvbmdTdGFydGVkKSB7XG4gICAgICBMb2dnZXIuZGVidWcoXCJTdGFydGluZyBwaW5nIChpZiBjb25maWd1cmVkKVwiKVxuICAgICAgcGluZ1BvbmdTdGFydGVkID0gdHJ1ZTtcblxuICAgICAgaWYgKGNvbmZpZ3VyYXRpb24uaGVhcnRiZWF0ICE9IHVuZGVmaW5lZCkge1xuICAgICAgICBwaW5nSW50ZXJ2YWwgPSBzZXRJbnRlcnZhbChzZW5kUGluZywgY29uZmlndXJhdGlvbi5oZWFydGJlYXQpO1xuICAgICAgICBzZW5kUGluZygpO1xuICAgICAgfVxuICAgIH1cbiAgfVxuXG4gIHRoaXMuY2xvc2UgPSBmdW5jdGlvbiAoKSB7XG4gICAgTG9nZ2VyLmRlYnVnKFwiQ2xvc2luZyBqc29uUnBjQ2xpZW50IGV4cGxpY2l0bHkgYnkgY2xpZW50XCIpO1xuXG4gICAgaWYgKHBpbmdJbnRlcnZhbCAhPSB1bmRlZmluZWQpIHtcbiAgICAgIExvZ2dlci5kZWJ1ZyhcIkNsZWFyaW5nIHBpbmcgaW50ZXJ2YWxcIik7XG4gICAgICBjbGVhckludGVydmFsKHBpbmdJbnRlcnZhbCk7XG4gICAgfVxuICAgIHBpbmdQb25nU3RhcnRlZCA9IGZhbHNlO1xuICAgIGVuYWJsZWRQaW5ncyA9IGZhbHNlO1xuXG4gICAgaWYgKGNvbmZpZ3VyYXRpb24uc2VuZENsb3NlTWVzc2FnZSkge1xuICAgICAgTG9nZ2VyLmRlYnVnKFwiU2VuZGluZyBjbG9zZSBtZXNzYWdlXCIpXG4gICAgICB0aGlzLnNlbmQoJ2Nsb3NlU2Vzc2lvbicsIG51bGwsIGZ1bmN0aW9uIChlcnJvciwgcmVzdWx0KSB7XG4gICAgICAgIGlmIChlcnJvcikge1xuICAgICAgICAgIExvZ2dlci5lcnJvcihcIkVycm9yIHNlbmRpbmcgY2xvc2UgbWVzc2FnZTogXCIgKyBKU09OLnN0cmluZ2lmeShcbiAgICAgICAgICAgIGVycm9yKSk7XG4gICAgICAgIH1cbiAgICAgICAgd3MuY2xvc2UoKTtcbiAgICAgIH0pO1xuICAgIH0gZWxzZSB7XG4gICAgICB3cy5jbG9zZSgpO1xuICAgIH1cbiAgfVxuXG4gIC8vIFRoaXMgbWV0aG9kIGlzIG9ubHkgZm9yIHRlc3RpbmdcbiAgdGhpcy5mb3JjZUNsb3NlID0gZnVuY3Rpb24gKG1pbGxpcykge1xuICAgIHdzLmZvcmNlQ2xvc2UobWlsbGlzKTtcbiAgfVxuXG4gIHRoaXMucmVjb25uZWN0ID0gZnVuY3Rpb24gKCkge1xuICAgIHdzLnJlY29ubmVjdFdzKCk7XG4gIH1cbn1cblxubW9kdWxlLmV4cG9ydHMgPSBKc29uUnBjQ2xpZW50O1xuIiwiLypcbiAqIChDKSBDb3B5cmlnaHQgMjAxNCBLdXJlbnRvIChodHRwOi8va3VyZW50by5vcmcvKVxuICpcbiAqIExpY2Vuc2VkIHVuZGVyIHRoZSBBcGFjaGUgTGljZW5zZSwgVmVyc2lvbiAyLjAgKHRoZSBcIkxpY2Vuc2VcIik7XG4gKiB5b3UgbWF5IG5vdCB1c2UgdGhpcyBmaWxlIGV4Y2VwdCBpbiBjb21wbGlhbmNlIHdpdGggdGhlIExpY2Vuc2UuXG4gKiBZb3UgbWF5IG9idGFpbiBhIGNvcHkgb2YgdGhlIExpY2Vuc2UgYXRcbiAqXG4gKiAgIGh0dHA6Ly93d3cuYXBhY2hlLm9yZy9saWNlbnNlcy9MSUNFTlNFLTIuMFxuICpcbiAqIFVubGVzcyByZXF1aXJlZCBieSBhcHBsaWNhYmxlIGxhdyBvciBhZ3JlZWQgdG8gaW4gd3JpdGluZywgc29mdHdhcmVcbiAqIGRpc3RyaWJ1dGVkIHVuZGVyIHRoZSBMaWNlbnNlIGlzIGRpc3RyaWJ1dGVkIG9uIGFuIFwiQVMgSVNcIiBCQVNJUyxcbiAqIFdJVEhPVVQgV0FSUkFOVElFUyBPUiBDT05ESVRJT05TIE9GIEFOWSBLSU5ELCBlaXRoZXIgZXhwcmVzcyBvciBpbXBsaWVkLlxuICogU2VlIHRoZSBMaWNlbnNlIGZvciB0aGUgc3BlY2lmaWMgbGFuZ3VhZ2UgZ292ZXJuaW5nIHBlcm1pc3Npb25zIGFuZFxuICogbGltaXRhdGlvbnMgdW5kZXIgdGhlIExpY2Vuc2UuXG4gKlxuICovXG5cbnZhciBXZWJTb2NrZXRXaXRoUmVjb25uZWN0aW9uID0gcmVxdWlyZSgnLi93ZWJTb2NrZXRXaXRoUmVjb25uZWN0aW9uJyk7XG5cbmV4cG9ydHMuV2ViU29ja2V0V2l0aFJlY29ubmVjdGlvbiA9IFdlYlNvY2tldFdpdGhSZWNvbm5lY3Rpb247XG4iLCIvKlxuICogKEMpIENvcHlyaWdodCAyMDEzLTIwMTUgS3VyZW50byAoaHR0cDovL2t1cmVudG8ub3JnLylcbiAqXG4gKiBMaWNlbnNlZCB1bmRlciB0aGUgQXBhY2hlIExpY2Vuc2UsIFZlcnNpb24gMi4wICh0aGUgXCJMaWNlbnNlXCIpO1xuICogeW91IG1heSBub3QgdXNlIHRoaXMgZmlsZSBleGNlcHQgaW4gY29tcGxpYW5jZSB3aXRoIHRoZSBMaWNlbnNlLlxuICogWW91IG1heSBvYnRhaW4gYSBjb3B5IG9mIHRoZSBMaWNlbnNlIGF0XG4gKlxuICogICBodHRwOi8vd3d3LmFwYWNoZS5vcmcvbGljZW5zZXMvTElDRU5TRS0yLjBcbiAqXG4gKiBVbmxlc3MgcmVxdWlyZWQgYnkgYXBwbGljYWJsZSBsYXcgb3IgYWdyZWVkIHRvIGluIHdyaXRpbmcsIHNvZnR3YXJlXG4gKiBkaXN0cmlidXRlZCB1bmRlciB0aGUgTGljZW5zZSBpcyBkaXN0cmlidXRlZCBvbiBhbiBcIkFTIElTXCIgQkFTSVMsXG4gKiBXSVRIT1VUIFdBUlJBTlRJRVMgT1IgQ09ORElUSU9OUyBPRiBBTlkgS0lORCwgZWl0aGVyIGV4cHJlc3Mgb3IgaW1wbGllZC5cbiAqIFNlZSB0aGUgTGljZW5zZSBmb3IgdGhlIHNwZWNpZmljIGxhbmd1YWdlIGdvdmVybmluZyBwZXJtaXNzaW9ucyBhbmRcbiAqIGxpbWl0YXRpb25zIHVuZGVyIHRoZSBMaWNlbnNlLlxuICovXG5cblwidXNlIHN0cmljdFwiO1xuXG52YXIgQnJvd3NlcldlYlNvY2tldCA9IGdsb2JhbC5XZWJTb2NrZXQgfHwgZ2xvYmFsLk1veldlYlNvY2tldDtcblxudmFyIExvZ2dlciA9IGNvbnNvbGU7XG5cbi8qKlxuICogR2V0IGVpdGhlciB0aGUgYFdlYlNvY2tldGAgb3IgYE1veldlYlNvY2tldGAgZ2xvYmFsc1xuICogaW4gdGhlIGJyb3dzZXIgb3IgdHJ5IHRvIHJlc29sdmUgV2ViU29ja2V0LWNvbXBhdGlibGVcbiAqIGludGVyZmFjZSBleHBvc2VkIGJ5IGB3c2AgZm9yIE5vZGUtbGlrZSBlbnZpcm9ubWVudC5cbiAqL1xuXG52YXIgV2ViU29ja2V0ID0gQnJvd3NlcldlYlNvY2tldDtcbmlmICghV2ViU29ja2V0ICYmIHR5cGVvZiB3aW5kb3cgPT09ICd1bmRlZmluZWQnKSB7XG4gIHRyeSB7XG4gICAgV2ViU29ja2V0ID0gcmVxdWlyZSgnd3MnKTtcbiAgfSBjYXRjaCAoZSkge31cbn1cblxuLy92YXIgU29ja0pTID0gcmVxdWlyZSgnc29ja2pzLWNsaWVudCcpO1xuXG52YXIgTUFYX1JFVFJJRVMgPSAyMDAwOyAvLyBGb3JldmVyLi4uXG52YXIgUkVUUllfVElNRV9NUyA9IDMwMDA7IC8vIEZJWE1FOiBJbXBsZW1lbnQgZXhwb25lbnRpYWwgd2FpdCB0aW1lcy4uLlxuXG52YXIgQ09OTkVDVElORyA9IDA7XG52YXIgT1BFTiA9IDE7XG52YXIgQ0xPU0lORyA9IDI7XG52YXIgQ0xPU0VEID0gMztcblxuLypcbmNvbmZpZyA9IHtcblx0XHR1cmkgOiB3c1VyaSxcblx0XHR1c2VTb2NrSlMgOiB0cnVlICh1c2UgU29ja0pTKSAvIGZhbHNlICh1c2UgV2ViU29ja2V0KSBieSBkZWZhdWx0LFxuXHRcdG9uY29ubmVjdGVkIDogY2FsbGJhY2sgbWV0aG9kIHRvIGludm9rZSB3aGVuIGNvbm5lY3Rpb24gaXMgc3VjY2Vzc2Z1bCxcblx0XHRvbmRpc2Nvbm5lY3QgOiBjYWxsYmFjayBtZXRob2QgdG8gaW52b2tlIHdoZW4gdGhlIGNvbm5lY3Rpb24gaXMgbG9zdCxcblx0XHRvbnJlY29ubmVjdGluZyA6IGNhbGxiYWNrIG1ldGhvZCB0byBpbnZva2Ugd2hlbiB0aGUgY2xpZW50IGlzIHJlY29ubmVjdGluZyxcblx0XHRvbnJlY29ubmVjdGVkIDogY2FsbGJhY2sgbWV0aG9kIHRvIGludm9rZSB3aGVuIHRoZSBjbGllbnQgc3VjY2VzZnVsbHkgcmVjb25uZWN0cyxcblx0fTtcbiovXG5mdW5jdGlvbiBXZWJTb2NrZXRXaXRoUmVjb25uZWN0aW9uKGNvbmZpZykge1xuXG4gIHZhciBjbG9zaW5nID0gZmFsc2U7XG4gIHZhciByZWdpc3Rlck1lc3NhZ2VIYW5kbGVyO1xuICB2YXIgd3NVcmkgPSBjb25maWcudXJpO1xuICB2YXIgdXNlU29ja0pTID0gY29uZmlnLnVzZVNvY2tKUztcbiAgdmFyIHJlY29ubmVjdGluZyA9IGZhbHNlO1xuXG4gIHZhciBmb3JjaW5nRGlzY29ubmVjdGlvbiA9IGZhbHNlO1xuXG4gIHZhciB3cztcblxuICBpZiAodXNlU29ja0pTKSB7XG4gICAgd3MgPSBuZXcgU29ja0pTKHdzVXJpKTtcbiAgfSBlbHNlIHtcbiAgICB3cyA9IG5ldyBXZWJTb2NrZXQod3NVcmkpO1xuICB9XG5cbiAgd3Mub25vcGVuID0gZnVuY3Rpb24gKCkge1xuICAgIGxvZ0Nvbm5lY3RlZCh3cywgd3NVcmkpO1xuICAgIGlmIChjb25maWcub25jb25uZWN0ZWQpIHtcbiAgICAgIGNvbmZpZy5vbmNvbm5lY3RlZCgpO1xuICAgIH1cbiAgfTtcblxuICB3cy5vbmVycm9yID0gZnVuY3Rpb24gKGVycm9yKSB7XG4gICAgTG9nZ2VyLmVycm9yKFwiQ291bGQgbm90IGNvbm5lY3QgdG8gXCIgKyB3c1VyaSArXG4gICAgICBcIiAoaW52b2tpbmcgb25lcnJvciBpZiBkZWZpbmVkKVwiLCBlcnJvcik7XG4gICAgaWYgKGNvbmZpZy5vbmVycm9yKSB7XG4gICAgICBjb25maWcub25lcnJvcihlcnJvcik7XG4gICAgfVxuICB9O1xuXG4gIGZ1bmN0aW9uIGxvZ0Nvbm5lY3RlZCh3cywgd3NVcmkpIHtcbiAgICB0cnkge1xuICAgICAgTG9nZ2VyLmRlYnVnKFwiV2ViU29ja2V0IGNvbm5lY3RlZCB0byBcIiArIHdzVXJpKTtcbiAgICB9IGNhdGNoIChlKSB7XG4gICAgICBMb2dnZXIuZXJyb3IoZSk7XG4gICAgfVxuICB9XG5cbiAgdmFyIHJlY29ubmVjdGlvbk9uQ2xvc2UgPSBmdW5jdGlvbiAoKSB7XG4gICAgaWYgKHdzLnJlYWR5U3RhdGUgPT09IENMT1NFRCkge1xuICAgICAgaWYgKGNsb3NpbmcpIHtcbiAgICAgICAgTG9nZ2VyLmRlYnVnKFwiQ29ubmVjdGlvbiBjbG9zZWQgYnkgdXNlclwiKTtcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIExvZ2dlci5kZWJ1ZyhcIkNvbm5lY3Rpb24gY2xvc2VkIHVuZXhwZWN0ZWNseS4gUmVjb25uZWN0aW5nLi4uXCIpO1xuICAgICAgICByZWNvbm5lY3RUb1NhbWVVcmkoTUFYX1JFVFJJRVMsIDEpO1xuICAgICAgfVxuICAgIH0gZWxzZSB7XG4gICAgICBMb2dnZXIuZGVidWcoXCJDbG9zZSBjYWxsYmFjayBmcm9tIHByZXZpb3VzIHdlYnNvY2tldC4gSWdub3JpbmcgaXRcIik7XG4gICAgfVxuICB9O1xuXG4gIHdzLm9uY2xvc2UgPSByZWNvbm5lY3Rpb25PbkNsb3NlO1xuXG4gIGZ1bmN0aW9uIHJlY29ubmVjdFRvU2FtZVVyaShtYXhSZXRyaWVzLCBudW1SZXRyaWVzKSB7XG4gICAgTG9nZ2VyLmRlYnVnKFwicmVjb25uZWN0VG9TYW1lVXJpIChhdHRlbXB0ICNcIiArIG51bVJldHJpZXMgKyBcIiwgbWF4PVwiICtcbiAgICAgIG1heFJldHJpZXMgKyBcIilcIik7XG5cbiAgICBpZiAobnVtUmV0cmllcyA9PT0gMSkge1xuICAgICAgaWYgKHJlY29ubmVjdGluZykge1xuICAgICAgICBMb2dnZXIud2FybihcbiAgICAgICAgICBcIlRyeWluZyB0byByZWNvbm5lY3RUb05ld1VyaSB3aGVuIHJlY29ubmVjdGluZy4uLiBJZ25vcmluZyB0aGlzIHJlY29ubmVjdGlvbi5cIlxuICAgICAgICApXG4gICAgICAgIHJldHVybjtcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIHJlY29ubmVjdGluZyA9IHRydWU7XG4gICAgICB9XG5cbiAgICAgIGlmIChjb25maWcub25yZWNvbm5lY3RpbmcpIHtcbiAgICAgICAgY29uZmlnLm9ucmVjb25uZWN0aW5nKCk7XG4gICAgICB9XG4gICAgfVxuXG4gICAgaWYgKGZvcmNpbmdEaXNjb25uZWN0aW9uKSB7XG4gICAgICByZWNvbm5lY3RUb05ld1VyaShtYXhSZXRyaWVzLCBudW1SZXRyaWVzLCB3c1VyaSk7XG5cbiAgICB9IGVsc2Uge1xuICAgICAgaWYgKGNvbmZpZy5uZXdXc1VyaU9uUmVjb25uZWN0aW9uKSB7XG4gICAgICAgIGNvbmZpZy5uZXdXc1VyaU9uUmVjb25uZWN0aW9uKGZ1bmN0aW9uIChlcnJvciwgbmV3V3NVcmkpIHtcblxuICAgICAgICAgIGlmIChlcnJvcikge1xuICAgICAgICAgICAgTG9nZ2VyLmRlYnVnKGVycm9yKTtcbiAgICAgICAgICAgIHNldFRpbWVvdXQoZnVuY3Rpb24gKCkge1xuICAgICAgICAgICAgICByZWNvbm5lY3RUb1NhbWVVcmkobWF4UmV0cmllcywgbnVtUmV0cmllcyArIDEpO1xuICAgICAgICAgICAgfSwgUkVUUllfVElNRV9NUyk7XG4gICAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICAgIHJlY29ubmVjdFRvTmV3VXJpKG1heFJldHJpZXMsIG51bVJldHJpZXMsIG5ld1dzVXJpKTtcbiAgICAgICAgICB9XG4gICAgICAgIH0pXG4gICAgICB9IGVsc2Uge1xuICAgICAgICByZWNvbm5lY3RUb05ld1VyaShtYXhSZXRyaWVzLCBudW1SZXRyaWVzLCB3c1VyaSk7XG4gICAgICB9XG4gICAgfVxuICB9XG5cbiAgLy8gVE9ETyBUZXN0IHJldHJpZXMuIEhvdyB0byBmb3JjZSBub3QgY29ubmVjdGlvbj9cbiAgZnVuY3Rpb24gcmVjb25uZWN0VG9OZXdVcmkobWF4UmV0cmllcywgbnVtUmV0cmllcywgcmVjb25uZWN0V3NVcmkpIHtcbiAgICBMb2dnZXIuZGVidWcoXCJSZWNvbm5lY3Rpb24gYXR0ZW1wdCAjXCIgKyBudW1SZXRyaWVzKTtcblxuICAgIHdzLmNsb3NlKCk7XG5cbiAgICB3c1VyaSA9IHJlY29ubmVjdFdzVXJpIHx8IHdzVXJpO1xuXG4gICAgdmFyIG5ld1dzO1xuICAgIGlmICh1c2VTb2NrSlMpIHtcbiAgICAgIG5ld1dzID0gbmV3IFNvY2tKUyh3c1VyaSk7XG4gICAgfSBlbHNlIHtcbiAgICAgIG5ld1dzID0gbmV3IFdlYlNvY2tldCh3c1VyaSk7XG4gICAgfVxuXG4gICAgbmV3V3Mub25vcGVuID0gZnVuY3Rpb24gKCkge1xuICAgICAgTG9nZ2VyLmRlYnVnKFwiUmVjb25uZWN0ZWQgYWZ0ZXIgXCIgKyBudW1SZXRyaWVzICsgXCIgYXR0ZW1wdHMuLi5cIik7XG4gICAgICBsb2dDb25uZWN0ZWQobmV3V3MsIHdzVXJpKTtcbiAgICAgIHJlY29ubmVjdGluZyA9IGZhbHNlO1xuICAgICAgcmVnaXN0ZXJNZXNzYWdlSGFuZGxlcigpO1xuICAgICAgaWYgKGNvbmZpZy5vbnJlY29ubmVjdGVkKCkpIHtcbiAgICAgICAgY29uZmlnLm9ucmVjb25uZWN0ZWQoKTtcbiAgICAgIH1cblxuICAgICAgbmV3V3Mub25jbG9zZSA9IHJlY29ubmVjdGlvbk9uQ2xvc2U7XG4gICAgfTtcblxuICAgIHZhciBvbkVycm9yT3JDbG9zZSA9IGZ1bmN0aW9uIChlcnJvcikge1xuICAgICAgTG9nZ2VyLndhcm4oXCJSZWNvbm5lY3Rpb24gZXJyb3I6IFwiLCBlcnJvcik7XG5cbiAgICAgIGlmIChudW1SZXRyaWVzID09PSBtYXhSZXRyaWVzKSB7XG4gICAgICAgIGlmIChjb25maWcub25kaXNjb25uZWN0KSB7XG4gICAgICAgICAgY29uZmlnLm9uZGlzY29ubmVjdCgpO1xuICAgICAgICB9XG4gICAgICB9IGVsc2Uge1xuICAgICAgICBzZXRUaW1lb3V0KGZ1bmN0aW9uICgpIHtcbiAgICAgICAgICByZWNvbm5lY3RUb1NhbWVVcmkobWF4UmV0cmllcywgbnVtUmV0cmllcyArIDEpO1xuICAgICAgICB9LCBSRVRSWV9USU1FX01TKTtcbiAgICAgIH1cbiAgICB9O1xuXG4gICAgbmV3V3Mub25lcnJvciA9IG9uRXJyb3JPckNsb3NlO1xuXG4gICAgd3MgPSBuZXdXcztcbiAgfVxuXG4gIHRoaXMuY2xvc2UgPSBmdW5jdGlvbiAoKSB7XG4gICAgY2xvc2luZyA9IHRydWU7XG4gICAgd3MuY2xvc2UoKTtcbiAgfTtcblxuICAvLyBUaGlzIG1ldGhvZCBpcyBvbmx5IGZvciB0ZXN0aW5nXG4gIHRoaXMuZm9yY2VDbG9zZSA9IGZ1bmN0aW9uIChtaWxsaXMpIHtcbiAgICBMb2dnZXIuZGVidWcoXCJUZXN0aW5nOiBGb3JjZSBXZWJTb2NrZXQgY2xvc2VcIik7XG5cbiAgICBpZiAobWlsbGlzKSB7XG4gICAgICBMb2dnZXIuZGVidWcoXCJUZXN0aW5nOiBDaGFuZ2Ugd3NVcmkgZm9yIFwiICsgbWlsbGlzICtcbiAgICAgICAgXCIgbWlsbGlzIHRvIHNpbXVsYXRlIG5ldCBmYWlsdXJlXCIpO1xuICAgICAgdmFyIGdvb2RXc1VyaSA9IHdzVXJpO1xuICAgICAgd3NVcmkgPSBcIndzczovLzIxLjIzNC4xMi4zNC40OjQ0My9cIjtcblxuICAgICAgZm9yY2luZ0Rpc2Nvbm5lY3Rpb24gPSB0cnVlO1xuXG4gICAgICBzZXRUaW1lb3V0KGZ1bmN0aW9uICgpIHtcbiAgICAgICAgTG9nZ2VyLmRlYnVnKFwiVGVzdGluZzogUmVjb3ZlciBnb29kIHdzVXJpIFwiICsgZ29vZFdzVXJpKTtcbiAgICAgICAgd3NVcmkgPSBnb29kV3NVcmk7XG5cbiAgICAgICAgZm9yY2luZ0Rpc2Nvbm5lY3Rpb24gPSBmYWxzZTtcblxuICAgICAgfSwgbWlsbGlzKTtcbiAgICB9XG5cbiAgICB3cy5jbG9zZSgpO1xuICB9O1xuXG4gIHRoaXMucmVjb25uZWN0V3MgPSBmdW5jdGlvbiAoKSB7XG4gICAgTG9nZ2VyLmRlYnVnKFwicmVjb25uZWN0V3NcIik7XG4gICAgcmVjb25uZWN0VG9TYW1lVXJpKE1BWF9SRVRSSUVTLCAxLCB3c1VyaSk7XG4gIH07XG5cbiAgdGhpcy5zZW5kID0gZnVuY3Rpb24gKG1lc3NhZ2UpIHtcbiAgICB3cy5zZW5kKG1lc3NhZ2UpO1xuICB9O1xuXG4gIHRoaXMuYWRkRXZlbnRMaXN0ZW5lciA9IGZ1bmN0aW9uICh0eXBlLCBjYWxsYmFjaykge1xuICAgIHJlZ2lzdGVyTWVzc2FnZUhhbmRsZXIgPSBmdW5jdGlvbiAoKSB7XG4gICAgICB3cy5hZGRFdmVudExpc3RlbmVyKHR5cGUsIGNhbGxiYWNrKTtcbiAgICB9O1xuXG4gICAgcmVnaXN0ZXJNZXNzYWdlSGFuZGxlcigpO1xuICB9O1xufVxuXG5tb2R1bGUuZXhwb3J0cyA9IFdlYlNvY2tldFdpdGhSZWNvbm5lY3Rpb247XG4iLCIvKlxuICogKEMpIENvcHlyaWdodCAyMDE0IEt1cmVudG8gKGh0dHA6Ly9rdXJlbnRvLm9yZy8pXG4gKlxuICogTGljZW5zZWQgdW5kZXIgdGhlIEFwYWNoZSBMaWNlbnNlLCBWZXJzaW9uIDIuMCAodGhlIFwiTGljZW5zZVwiKTtcbiAqIHlvdSBtYXkgbm90IHVzZSB0aGlzIGZpbGUgZXhjZXB0IGluIGNvbXBsaWFuY2Ugd2l0aCB0aGUgTGljZW5zZS5cbiAqIFlvdSBtYXkgb2J0YWluIGEgY29weSBvZiB0aGUgTGljZW5zZSBhdFxuICpcbiAqICAgaHR0cDovL3d3dy5hcGFjaGUub3JnL2xpY2Vuc2VzL0xJQ0VOU0UtMi4wXG4gKlxuICogVW5sZXNzIHJlcXVpcmVkIGJ5IGFwcGxpY2FibGUgbGF3IG9yIGFncmVlZCB0byBpbiB3cml0aW5nLCBzb2Z0d2FyZVxuICogZGlzdHJpYnV0ZWQgdW5kZXIgdGhlIExpY2Vuc2UgaXMgZGlzdHJpYnV0ZWQgb24gYW4gXCJBUyBJU1wiIEJBU0lTLFxuICogV0lUSE9VVCBXQVJSQU5USUVTIE9SIENPTkRJVElPTlMgT0YgQU5ZIEtJTkQsIGVpdGhlciBleHByZXNzIG9yIGltcGxpZWQuXG4gKiBTZWUgdGhlIExpY2Vuc2UgZm9yIHRoZSBzcGVjaWZpYyBsYW5ndWFnZSBnb3Zlcm5pbmcgcGVybWlzc2lvbnMgYW5kXG4gKiBsaW1pdGF0aW9ucyB1bmRlciB0aGUgTGljZW5zZS5cbiAqXG4gKi9cblxudmFyIGRlZmluZVByb3BlcnR5X0lFOCA9IGZhbHNlXG5pZiAoT2JqZWN0LmRlZmluZVByb3BlcnR5KSB7XG4gIHRyeSB7XG4gICAgT2JqZWN0LmRlZmluZVByb3BlcnR5KHt9LCBcInhcIiwge30pO1xuICB9IGNhdGNoIChlKSB7XG4gICAgZGVmaW5lUHJvcGVydHlfSUU4ID0gdHJ1ZVxuICB9XG59XG5cbi8vIGh0dHBzOi8vZGV2ZWxvcGVyLm1vemlsbGEub3JnL2VuLVVTL2RvY3MvV2ViL0phdmFTY3JpcHQvUmVmZXJlbmNlL0dsb2JhbF9PYmplY3RzL0Z1bmN0aW9uL2JpbmRcbmlmICghRnVuY3Rpb24ucHJvdG90eXBlLmJpbmQpIHtcbiAgRnVuY3Rpb24ucHJvdG90eXBlLmJpbmQgPSBmdW5jdGlvbiAob1RoaXMpIHtcbiAgICBpZiAodHlwZW9mIHRoaXMgIT09ICdmdW5jdGlvbicpIHtcbiAgICAgIC8vIGNsb3Nlc3QgdGhpbmcgcG9zc2libGUgdG8gdGhlIEVDTUFTY3JpcHQgNVxuICAgICAgLy8gaW50ZXJuYWwgSXNDYWxsYWJsZSBmdW5jdGlvblxuICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcihcbiAgICAgICAgJ0Z1bmN0aW9uLnByb3RvdHlwZS5iaW5kIC0gd2hhdCBpcyB0cnlpbmcgdG8gYmUgYm91bmQgaXMgbm90IGNhbGxhYmxlJ1xuICAgICAgKTtcbiAgICB9XG5cbiAgICB2YXIgYUFyZ3MgPSBBcnJheS5wcm90b3R5cGUuc2xpY2UuY2FsbChhcmd1bWVudHMsIDEpLFxuICAgICAgZlRvQmluZCA9IHRoaXMsXG4gICAgICBmTk9QID0gZnVuY3Rpb24gKCkge30sXG4gICAgICBmQm91bmQgPSBmdW5jdGlvbiAoKSB7XG4gICAgICAgIHJldHVybiBmVG9CaW5kLmFwcGx5KHRoaXMgaW5zdGFuY2VvZiBmTk9QICYmIG9UaGlzID9cbiAgICAgICAgICB0aGlzIDpcbiAgICAgICAgICBvVGhpcyxcbiAgICAgICAgICBhQXJncy5jb25jYXQoQXJyYXkucHJvdG90eXBlLnNsaWNlLmNhbGwoYXJndW1lbnRzKSkpO1xuICAgICAgfTtcblxuICAgIGZOT1AucHJvdG90eXBlID0gdGhpcy5wcm90b3R5cGU7XG4gICAgZkJvdW5kLnByb3RvdHlwZSA9IG5ldyBmTk9QKCk7XG5cbiAgICByZXR1cm4gZkJvdW5kO1xuICB9O1xufVxuXG52YXIgRXZlbnRFbWl0dGVyID0gcmVxdWlyZSgnZXZlbnRzJykuRXZlbnRFbWl0dGVyO1xuXG52YXIgaW5oZXJpdHMgPSByZXF1aXJlKCdpbmhlcml0cycpO1xuXG52YXIgcGFja2VycyA9IHJlcXVpcmUoJy4vcGFja2VycycpO1xudmFyIE1hcHBlciA9IHJlcXVpcmUoJy4vTWFwcGVyJyk7XG5cbnZhciBCQVNFX1RJTUVPVVQgPSA1MDAwO1xuXG5mdW5jdGlvbiB1bmlmeVJlc3BvbnNlTWV0aG9kcyhyZXNwb25zZU1ldGhvZHMpIHtcbiAgaWYgKCFyZXNwb25zZU1ldGhvZHMpIHJldHVybiB7fTtcblxuICBmb3IgKHZhciBrZXkgaW4gcmVzcG9uc2VNZXRob2RzKSB7XG4gICAgdmFyIHZhbHVlID0gcmVzcG9uc2VNZXRob2RzW2tleV07XG5cbiAgICBpZiAodHlwZW9mIHZhbHVlID09ICdzdHJpbmcnKVxuICAgICAgcmVzcG9uc2VNZXRob2RzW2tleV0gPSB7XG4gICAgICAgIHJlc3BvbnNlOiB2YWx1ZVxuICAgICAgfVxuICB9O1xuXG4gIHJldHVybiByZXNwb25zZU1ldGhvZHM7XG59O1xuXG5mdW5jdGlvbiB1bmlmeVRyYW5zcG9ydCh0cmFuc3BvcnQpIHtcbiAgaWYgKCF0cmFuc3BvcnQpIHJldHVybjtcblxuICAvLyBUcmFuc3BvcnQgYXMgYSBmdW5jdGlvblxuICBpZiAodHJhbnNwb3J0IGluc3RhbmNlb2YgRnVuY3Rpb24pXG4gICAgcmV0dXJuIHtcbiAgICAgIHNlbmQ6IHRyYW5zcG9ydFxuICAgIH07XG5cbiAgLy8gV2ViU29ja2V0ICYgRGF0YUNoYW5uZWxcbiAgaWYgKHRyYW5zcG9ydC5zZW5kIGluc3RhbmNlb2YgRnVuY3Rpb24pXG4gICAgcmV0dXJuIHRyYW5zcG9ydDtcblxuICAvLyBNZXNzYWdlIEFQSSAoSW50ZXItd2luZG93ICYgV2ViV29ya2VyKVxuICBpZiAodHJhbnNwb3J0LnBvc3RNZXNzYWdlIGluc3RhbmNlb2YgRnVuY3Rpb24pIHtcbiAgICB0cmFuc3BvcnQuc2VuZCA9IHRyYW5zcG9ydC5wb3N0TWVzc2FnZTtcbiAgICByZXR1cm4gdHJhbnNwb3J0O1xuICB9XG5cbiAgLy8gU3RyZWFtIEFQSVxuICBpZiAodHJhbnNwb3J0LndyaXRlIGluc3RhbmNlb2YgRnVuY3Rpb24pIHtcbiAgICB0cmFuc3BvcnQuc2VuZCA9IHRyYW5zcG9ydC53cml0ZTtcbiAgICByZXR1cm4gdHJhbnNwb3J0O1xuICB9XG5cbiAgLy8gVHJhbnNwb3J0cyB0aGF0IG9ubHkgY2FuIHJlY2VpdmUgbWVzc2FnZXMsIGJ1dCBub3Qgc2VuZFxuICBpZiAodHJhbnNwb3J0Lm9ubWVzc2FnZSAhPT0gdW5kZWZpbmVkKSByZXR1cm47XG4gIGlmICh0cmFuc3BvcnQucGF1c2UgaW5zdGFuY2VvZiBGdW5jdGlvbikgcmV0dXJuO1xuXG4gIHRocm93IG5ldyBTeW50YXhFcnJvcihcIlRyYW5zcG9ydCBpcyBub3QgYSBmdW5jdGlvbiBub3IgYSB2YWxpZCBvYmplY3RcIik7XG59O1xuXG4vKipcbiAqIFJlcHJlc2VudGF0aW9uIG9mIGEgUlBDIG5vdGlmaWNhdGlvblxuICpcbiAqIEBjbGFzc1xuICpcbiAqIEBjb25zdHJ1Y3RvclxuICpcbiAqIEBwYXJhbSB7U3RyaW5nfSBtZXRob2QgLW1ldGhvZCBvZiB0aGUgbm90aWZpY2F0aW9uXG4gKiBAcGFyYW0gcGFyYW1zIC0gcGFyYW1ldGVycyBvZiB0aGUgbm90aWZpY2F0aW9uXG4gKi9cbmZ1bmN0aW9uIFJwY05vdGlmaWNhdGlvbihtZXRob2QsIHBhcmFtcykge1xuICBpZiAoZGVmaW5lUHJvcGVydHlfSUU4KSB7XG4gICAgdGhpcy5tZXRob2QgPSBtZXRob2RcbiAgICB0aGlzLnBhcmFtcyA9IHBhcmFtc1xuICB9IGVsc2Uge1xuICAgIE9iamVjdC5kZWZpbmVQcm9wZXJ0eSh0aGlzLCAnbWV0aG9kJywge1xuICAgICAgdmFsdWU6IG1ldGhvZCxcbiAgICAgIGVudW1lcmFibGU6IHRydWVcbiAgICB9KTtcbiAgICBPYmplY3QuZGVmaW5lUHJvcGVydHkodGhpcywgJ3BhcmFtcycsIHtcbiAgICAgIHZhbHVlOiBwYXJhbXMsXG4gICAgICBlbnVtZXJhYmxlOiB0cnVlXG4gICAgfSk7XG4gIH1cbn07XG5cbi8qKlxuICogQGNsYXNzXG4gKlxuICogQGNvbnN0cnVjdG9yXG4gKlxuICogQHBhcmFtIHtvYmplY3R9IHBhY2tlclxuICpcbiAqIEBwYXJhbSB7b2JqZWN0fSBbb3B0aW9uc11cbiAqXG4gKiBAcGFyYW0ge29iamVjdH0gW3RyYW5zcG9ydF1cbiAqXG4gKiBAcGFyYW0ge0Z1bmN0aW9ufSBbb25SZXF1ZXN0XVxuICovXG5mdW5jdGlvbiBScGNCdWlsZGVyKHBhY2tlciwgb3B0aW9ucywgdHJhbnNwb3J0LCBvblJlcXVlc3QpIHtcbiAgdmFyIHNlbGYgPSB0aGlzO1xuXG4gIGlmICghcGFja2VyKVxuICAgIHRocm93IG5ldyBTeW50YXhFcnJvcignUGFja2VyIGlzIG5vdCBkZWZpbmVkJyk7XG5cbiAgaWYgKCFwYWNrZXIucGFjayB8fCAhcGFja2VyLnVucGFjaylcbiAgICB0aHJvdyBuZXcgU3ludGF4RXJyb3IoJ1BhY2tlciBpcyBpbnZhbGlkJyk7XG5cbiAgdmFyIHJlc3BvbnNlTWV0aG9kcyA9IHVuaWZ5UmVzcG9uc2VNZXRob2RzKHBhY2tlci5yZXNwb25zZU1ldGhvZHMpO1xuXG4gIGlmIChvcHRpb25zIGluc3RhbmNlb2YgRnVuY3Rpb24pIHtcbiAgICBpZiAodHJhbnNwb3J0ICE9IHVuZGVmaW5lZClcbiAgICAgIHRocm93IG5ldyBTeW50YXhFcnJvcihcIlRoZXJlIGNhbid0IGJlIHBhcmFtZXRlcnMgYWZ0ZXIgb25SZXF1ZXN0XCIpO1xuXG4gICAgb25SZXF1ZXN0ID0gb3B0aW9ucztcbiAgICB0cmFuc3BvcnQgPSB1bmRlZmluZWQ7XG4gICAgb3B0aW9ucyA9IHVuZGVmaW5lZDtcbiAgfTtcblxuICBpZiAob3B0aW9ucyAmJiBvcHRpb25zLnNlbmQgaW5zdGFuY2VvZiBGdW5jdGlvbikge1xuICAgIGlmICh0cmFuc3BvcnQgJiYgISh0cmFuc3BvcnQgaW5zdGFuY2VvZiBGdW5jdGlvbikpXG4gICAgICB0aHJvdyBuZXcgU3ludGF4RXJyb3IoXCJPbmx5IGEgZnVuY3Rpb24gY2FuIGJlIGFmdGVyIHRyYW5zcG9ydFwiKTtcblxuICAgIG9uUmVxdWVzdCA9IHRyYW5zcG9ydDtcbiAgICB0cmFuc3BvcnQgPSBvcHRpb25zO1xuICAgIG9wdGlvbnMgPSB1bmRlZmluZWQ7XG4gIH07XG5cbiAgaWYgKHRyYW5zcG9ydCBpbnN0YW5jZW9mIEZ1bmN0aW9uKSB7XG4gICAgaWYgKG9uUmVxdWVzdCAhPSB1bmRlZmluZWQpXG4gICAgICB0aHJvdyBuZXcgU3ludGF4RXJyb3IoXCJUaGVyZSBjYW4ndCBiZSBwYXJhbWV0ZXJzIGFmdGVyIG9uUmVxdWVzdFwiKTtcblxuICAgIG9uUmVxdWVzdCA9IHRyYW5zcG9ydDtcbiAgICB0cmFuc3BvcnQgPSB1bmRlZmluZWQ7XG4gIH07XG5cbiAgaWYgKHRyYW5zcG9ydCAmJiB0cmFuc3BvcnQuc2VuZCBpbnN0YW5jZW9mIEZ1bmN0aW9uKVxuICAgIGlmIChvblJlcXVlc3QgJiYgIShvblJlcXVlc3QgaW5zdGFuY2VvZiBGdW5jdGlvbikpXG4gICAgICB0aHJvdyBuZXcgU3ludGF4RXJyb3IoXCJPbmx5IGEgZnVuY3Rpb24gY2FuIGJlIGFmdGVyIHRyYW5zcG9ydFwiKTtcblxuICBvcHRpb25zID0gb3B0aW9ucyB8fCB7fTtcblxuICBFdmVudEVtaXR0ZXIuY2FsbCh0aGlzKTtcblxuICBpZiAob25SZXF1ZXN0KVxuICAgIHRoaXMub24oJ3JlcXVlc3QnLCBvblJlcXVlc3QpO1xuXG4gIGlmIChkZWZpbmVQcm9wZXJ0eV9JRTgpXG4gICAgdGhpcy5wZWVySUQgPSBvcHRpb25zLnBlZXJJRFxuICBlbHNlXG4gICAgT2JqZWN0LmRlZmluZVByb3BlcnR5KHRoaXMsICdwZWVySUQnLCB7XG4gICAgICB2YWx1ZTogb3B0aW9ucy5wZWVySURcbiAgICB9KTtcblxuICB2YXIgbWF4X3JldHJpZXMgPSBvcHRpb25zLm1heF9yZXRyaWVzIHx8IDA7XG5cbiAgZnVuY3Rpb24gdHJhbnNwb3J0TWVzc2FnZShldmVudCkge1xuICAgIHNlbGYuZGVjb2RlKGV2ZW50LmRhdGEgfHwgZXZlbnQudG9TdHJpbmcoKSk7XG4gIH07XG5cbiAgdGhpcy5nZXRUcmFuc3BvcnQgPSBmdW5jdGlvbiAoKSB7XG4gICAgcmV0dXJuIHRyYW5zcG9ydDtcbiAgfVxuICB0aGlzLnNldFRyYW5zcG9ydCA9IGZ1bmN0aW9uICh2YWx1ZSkge1xuICAgIC8vIFJlbW92ZSBsaXN0ZW5lciBmcm9tIG9sZCB0cmFuc3BvcnRcbiAgICBpZiAodHJhbnNwb3J0KSB7XG4gICAgICAvLyBXM0MgdHJhbnNwb3J0c1xuICAgICAgaWYgKHRyYW5zcG9ydC5yZW1vdmVFdmVudExpc3RlbmVyKVxuICAgICAgICB0cmFuc3BvcnQucmVtb3ZlRXZlbnRMaXN0ZW5lcignbWVzc2FnZScsIHRyYW5zcG9ydE1lc3NhZ2UpO1xuXG4gICAgICAvLyBOb2RlLmpzIFN0cmVhbXMgQVBJXG4gICAgICBlbHNlIGlmICh0cmFuc3BvcnQucmVtb3ZlTGlzdGVuZXIpXG4gICAgICAgIHRyYW5zcG9ydC5yZW1vdmVMaXN0ZW5lcignZGF0YScsIHRyYW5zcG9ydE1lc3NhZ2UpO1xuICAgIH07XG5cbiAgICAvLyBTZXQgbGlzdGVuZXIgb24gbmV3IHRyYW5zcG9ydFxuICAgIGlmICh2YWx1ZSkge1xuICAgICAgLy8gVzNDIHRyYW5zcG9ydHNcbiAgICAgIGlmICh2YWx1ZS5hZGRFdmVudExpc3RlbmVyKVxuICAgICAgICB2YWx1ZS5hZGRFdmVudExpc3RlbmVyKCdtZXNzYWdlJywgdHJhbnNwb3J0TWVzc2FnZSk7XG5cbiAgICAgIC8vIE5vZGUuanMgU3RyZWFtcyBBUElcbiAgICAgIGVsc2UgaWYgKHZhbHVlLmFkZExpc3RlbmVyKVxuICAgICAgICB2YWx1ZS5hZGRMaXN0ZW5lcignZGF0YScsIHRyYW5zcG9ydE1lc3NhZ2UpO1xuICAgIH07XG5cbiAgICB0cmFuc3BvcnQgPSB1bmlmeVRyYW5zcG9ydCh2YWx1ZSk7XG4gIH1cblxuICBpZiAoIWRlZmluZVByb3BlcnR5X0lFOClcbiAgICBPYmplY3QuZGVmaW5lUHJvcGVydHkodGhpcywgJ3RyYW5zcG9ydCcsIHtcbiAgICAgIGdldDogdGhpcy5nZXRUcmFuc3BvcnQuYmluZCh0aGlzKSxcbiAgICAgIHNldDogdGhpcy5zZXRUcmFuc3BvcnQuYmluZCh0aGlzKVxuICAgIH0pXG5cbiAgdGhpcy5zZXRUcmFuc3BvcnQodHJhbnNwb3J0KTtcblxuICB2YXIgcmVxdWVzdF90aW1lb3V0ID0gb3B0aW9ucy5yZXF1ZXN0X3RpbWVvdXQgfHwgQkFTRV9USU1FT1VUO1xuICB2YXIgcGluZ19yZXF1ZXN0X3RpbWVvdXQgPSBvcHRpb25zLnBpbmdfcmVxdWVzdF90aW1lb3V0IHx8IHJlcXVlc3RfdGltZW91dDtcbiAgdmFyIHJlc3BvbnNlX3RpbWVvdXQgPSBvcHRpb25zLnJlc3BvbnNlX3RpbWVvdXQgfHwgQkFTRV9USU1FT1VUO1xuICB2YXIgZHVwbGljYXRlc190aW1lb3V0ID0gb3B0aW9ucy5kdXBsaWNhdGVzX3RpbWVvdXQgfHwgQkFTRV9USU1FT1VUO1xuXG4gIHZhciByZXF1ZXN0SUQgPSAwO1xuXG4gIHZhciByZXF1ZXN0cyA9IG5ldyBNYXBwZXIoKTtcbiAgdmFyIHJlc3BvbnNlcyA9IG5ldyBNYXBwZXIoKTtcbiAgdmFyIHByb2Nlc3NlZFJlc3BvbnNlcyA9IG5ldyBNYXBwZXIoKTtcblxuICB2YXIgbWVzc2FnZTJLZXkgPSB7fTtcblxuICAvKipcbiAgICogU3RvcmUgdGhlIHJlc3BvbnNlIHRvIHByZXZlbnQgdG8gcHJvY2VzcyBkdXBsaWNhdGUgcmVxdWVzdCBsYXRlclxuICAgKi9cbiAgZnVuY3Rpb24gc3RvcmVSZXNwb25zZShtZXNzYWdlLCBpZCwgZGVzdCkge1xuICAgIHZhciByZXNwb25zZSA9IHtcbiAgICAgIG1lc3NhZ2U6IG1lc3NhZ2UsXG4gICAgICAvKiogVGltZW91dCB0byBhdXRvLWNsZWFuIG9sZCByZXNwb25zZXMgKi9cbiAgICAgIHRpbWVvdXQ6IHNldFRpbWVvdXQoZnVuY3Rpb24gKCkge1xuICAgICAgICAgIHJlc3BvbnNlcy5yZW1vdmUoaWQsIGRlc3QpO1xuICAgICAgICB9LFxuICAgICAgICByZXNwb25zZV90aW1lb3V0KVxuICAgIH07XG5cbiAgICByZXNwb25zZXMuc2V0KHJlc3BvbnNlLCBpZCwgZGVzdCk7XG4gIH07XG5cbiAgLyoqXG4gICAqIFN0b3JlIHRoZSByZXNwb25zZSB0byBpZ25vcmUgZHVwbGljYXRlZCBtZXNzYWdlcyBsYXRlclxuICAgKi9cbiAgZnVuY3Rpb24gc3RvcmVQcm9jZXNzZWRSZXNwb25zZShhY2ssIGZyb20pIHtcbiAgICB2YXIgdGltZW91dCA9IHNldFRpbWVvdXQoZnVuY3Rpb24gKCkge1xuICAgICAgICBwcm9jZXNzZWRSZXNwb25zZXMucmVtb3ZlKGFjaywgZnJvbSk7XG4gICAgICB9LFxuICAgICAgZHVwbGljYXRlc190aW1lb3V0KTtcblxuICAgIHByb2Nlc3NlZFJlc3BvbnNlcy5zZXQodGltZW91dCwgYWNrLCBmcm9tKTtcbiAgfTtcblxuICAvKipcbiAgICogUmVwcmVzZW50YXRpb24gb2YgYSBSUEMgcmVxdWVzdFxuICAgKlxuICAgKiBAY2xhc3NcbiAgICogQGV4dGVuZHMgUnBjTm90aWZpY2F0aW9uXG4gICAqXG4gICAqIEBjb25zdHJ1Y3RvclxuICAgKlxuICAgKiBAcGFyYW0ge1N0cmluZ30gbWV0aG9kIC1tZXRob2Qgb2YgdGhlIG5vdGlmaWNhdGlvblxuICAgKiBAcGFyYW0gcGFyYW1zIC0gcGFyYW1ldGVycyBvZiB0aGUgbm90aWZpY2F0aW9uXG4gICAqIEBwYXJhbSB7SW50ZWdlcn0gaWQgLSBpZGVudGlmaWVyIG9mIHRoZSByZXF1ZXN0XG4gICAqIEBwYXJhbSBbZnJvbV0gLSBzb3VyY2Ugb2YgdGhlIG5vdGlmaWNhdGlvblxuICAgKi9cbiAgZnVuY3Rpb24gUnBjUmVxdWVzdChtZXRob2QsIHBhcmFtcywgaWQsIGZyb20sIHRyYW5zcG9ydCkge1xuICAgIFJwY05vdGlmaWNhdGlvbi5jYWxsKHRoaXMsIG1ldGhvZCwgcGFyYW1zKTtcblxuICAgIHRoaXMuZ2V0VHJhbnNwb3J0ID0gZnVuY3Rpb24gKCkge1xuICAgICAgcmV0dXJuIHRyYW5zcG9ydDtcbiAgICB9XG4gICAgdGhpcy5zZXRUcmFuc3BvcnQgPSBmdW5jdGlvbiAodmFsdWUpIHtcbiAgICAgIHRyYW5zcG9ydCA9IHVuaWZ5VHJhbnNwb3J0KHZhbHVlKTtcbiAgICB9XG5cbiAgICBpZiAoIWRlZmluZVByb3BlcnR5X0lFOClcbiAgICAgIE9iamVjdC5kZWZpbmVQcm9wZXJ0eSh0aGlzLCAndHJhbnNwb3J0Jywge1xuICAgICAgICBnZXQ6IHRoaXMuZ2V0VHJhbnNwb3J0LmJpbmQodGhpcyksXG4gICAgICAgIHNldDogdGhpcy5zZXRUcmFuc3BvcnQuYmluZCh0aGlzKVxuICAgICAgfSlcblxuICAgIHZhciByZXNwb25zZSA9IHJlc3BvbnNlcy5nZXQoaWQsIGZyb20pO1xuXG4gICAgLyoqXG4gICAgICogQGNvbnN0YW50IHtCb29sZWFufSBkdXBsaWNhdGVkXG4gICAgICovXG4gICAgaWYgKCEodHJhbnNwb3J0IHx8IHNlbGYuZ2V0VHJhbnNwb3J0KCkpKSB7XG4gICAgICBpZiAoZGVmaW5lUHJvcGVydHlfSUU4KVxuICAgICAgICB0aGlzLmR1cGxpY2F0ZWQgPSBCb29sZWFuKHJlc3BvbnNlKVxuICAgICAgZWxzZVxuICAgICAgICBPYmplY3QuZGVmaW5lUHJvcGVydHkodGhpcywgJ2R1cGxpY2F0ZWQnLCB7XG4gICAgICAgICAgdmFsdWU6IEJvb2xlYW4ocmVzcG9uc2UpXG4gICAgICAgIH0pO1xuICAgIH1cblxuICAgIHZhciByZXNwb25zZU1ldGhvZCA9IHJlc3BvbnNlTWV0aG9kc1ttZXRob2RdO1xuXG4gICAgdGhpcy5wYWNrID0gcGFja2VyLnBhY2suYmluZChwYWNrZXIsIHRoaXMsIGlkKVxuXG4gICAgLyoqXG4gICAgICogR2VuZXJhdGUgYSByZXNwb25zZSB0byB0aGlzIHJlcXVlc3RcbiAgICAgKlxuICAgICAqIEBwYXJhbSB7RXJyb3J9IFtlcnJvcl1cbiAgICAgKiBAcGFyYW0geyp9IFtyZXN1bHRdXG4gICAgICpcbiAgICAgKiBAcmV0dXJucyB7c3RyaW5nfVxuICAgICAqL1xuICAgIHRoaXMucmVwbHkgPSBmdW5jdGlvbiAoZXJyb3IsIHJlc3VsdCwgdHJhbnNwb3J0KSB7XG4gICAgICAvLyBGaXggb3B0aW9uYWwgcGFyYW1ldGVyc1xuICAgICAgaWYgKGVycm9yIGluc3RhbmNlb2YgRnVuY3Rpb24gfHwgZXJyb3IgJiYgZXJyb3JcbiAgICAgICAgLnNlbmQgaW5zdGFuY2VvZiBGdW5jdGlvbikge1xuICAgICAgICBpZiAocmVzdWx0ICE9IHVuZGVmaW5lZClcbiAgICAgICAgICB0aHJvdyBuZXcgU3ludGF4RXJyb3IoXCJUaGVyZSBjYW4ndCBiZSBwYXJhbWV0ZXJzIGFmdGVyIGNhbGxiYWNrXCIpO1xuXG4gICAgICAgIHRyYW5zcG9ydCA9IGVycm9yO1xuICAgICAgICByZXN1bHQgPSBudWxsO1xuICAgICAgICBlcnJvciA9IHVuZGVmaW5lZDtcbiAgICAgIH0gZWxzZSBpZiAocmVzdWx0IGluc3RhbmNlb2YgRnVuY3Rpb24gfHxcbiAgICAgICAgcmVzdWx0ICYmIHJlc3VsdC5zZW5kIGluc3RhbmNlb2YgRnVuY3Rpb24pIHtcbiAgICAgICAgaWYgKHRyYW5zcG9ydCAhPSB1bmRlZmluZWQpXG4gICAgICAgICAgdGhyb3cgbmV3IFN5bnRheEVycm9yKFwiVGhlcmUgY2FuJ3QgYmUgcGFyYW1ldGVycyBhZnRlciBjYWxsYmFja1wiKTtcblxuICAgICAgICB0cmFuc3BvcnQgPSByZXN1bHQ7XG4gICAgICAgIHJlc3VsdCA9IG51bGw7XG4gICAgICB9O1xuXG4gICAgICB0cmFuc3BvcnQgPSB1bmlmeVRyYW5zcG9ydCh0cmFuc3BvcnQpO1xuXG4gICAgICAvLyBEdXBsaWNhdGVkIHJlcXVlc3QsIHJlbW92ZSBvbGQgcmVzcG9uc2UgdGltZW91dFxuICAgICAgaWYgKHJlc3BvbnNlKVxuICAgICAgICBjbGVhclRpbWVvdXQocmVzcG9uc2UudGltZW91dCk7XG5cbiAgICAgIGlmIChmcm9tICE9IHVuZGVmaW5lZCkge1xuICAgICAgICBpZiAoZXJyb3IpXG4gICAgICAgICAgZXJyb3IuZGVzdCA9IGZyb207XG5cbiAgICAgICAgaWYgKHJlc3VsdClcbiAgICAgICAgICByZXN1bHQuZGVzdCA9IGZyb207XG4gICAgICB9O1xuXG4gICAgICB2YXIgbWVzc2FnZTtcblxuICAgICAgLy8gTmV3IHJlcXVlc3Qgb3Igb3ZlcnJpZGVuIG9uZSwgY3JlYXRlIG5ldyByZXNwb25zZSB3aXRoIHByb3ZpZGVkIGRhdGFcbiAgICAgIGlmIChlcnJvciB8fCByZXN1bHQgIT0gdW5kZWZpbmVkKSB7XG4gICAgICAgIGlmIChzZWxmLnBlZXJJRCAhPSB1bmRlZmluZWQpIHtcbiAgICAgICAgICBpZiAoZXJyb3IpXG4gICAgICAgICAgICBlcnJvci5mcm9tID0gc2VsZi5wZWVySUQ7XG4gICAgICAgICAgZWxzZVxuICAgICAgICAgICAgcmVzdWx0LmZyb20gPSBzZWxmLnBlZXJJRDtcbiAgICAgICAgfVxuXG4gICAgICAgIC8vIFByb3RvY29sIGluZGljYXRlcyB0aGF0IHJlc3BvbnNlcyBoYXMgb3duIHJlcXVlc3QgbWV0aG9kc1xuICAgICAgICBpZiAocmVzcG9uc2VNZXRob2QpIHtcbiAgICAgICAgICBpZiAocmVzcG9uc2VNZXRob2QuZXJyb3IgPT0gdW5kZWZpbmVkICYmIGVycm9yKVxuICAgICAgICAgICAgbWVzc2FnZSA9IHtcbiAgICAgICAgICAgICAgZXJyb3I6IGVycm9yXG4gICAgICAgICAgICB9O1xuXG4gICAgICAgICAgZWxzZSB7XG4gICAgICAgICAgICB2YXIgbWV0aG9kID0gZXJyb3IgP1xuICAgICAgICAgICAgICByZXNwb25zZU1ldGhvZC5lcnJvciA6XG4gICAgICAgICAgICAgIHJlc3BvbnNlTWV0aG9kLnJlc3BvbnNlO1xuXG4gICAgICAgICAgICBtZXNzYWdlID0ge1xuICAgICAgICAgICAgICBtZXRob2Q6IG1ldGhvZCxcbiAgICAgICAgICAgICAgcGFyYW1zOiBlcnJvciB8fCByZXN1bHRcbiAgICAgICAgICAgIH07XG4gICAgICAgICAgfVxuICAgICAgICB9IGVsc2VcbiAgICAgICAgICBtZXNzYWdlID0ge1xuICAgICAgICAgICAgZXJyb3I6IGVycm9yLFxuICAgICAgICAgICAgcmVzdWx0OiByZXN1bHRcbiAgICAgICAgICB9O1xuXG4gICAgICAgIG1lc3NhZ2UgPSBwYWNrZXIucGFjayhtZXNzYWdlLCBpZCk7XG4gICAgICB9XG5cbiAgICAgIC8vIER1cGxpY2F0ZSAmIG5vdC1vdmVycmlkZW4gcmVxdWVzdCwgcmUtc2VuZCBvbGQgcmVzcG9uc2VcbiAgICAgIGVsc2UgaWYgKHJlc3BvbnNlKVxuICAgICAgICBtZXNzYWdlID0gcmVzcG9uc2UubWVzc2FnZTtcblxuICAgICAgLy8gTmV3IGVtcHR5IHJlcGx5LCByZXNwb25zZSBudWxsIHZhbHVlXG4gICAgICBlbHNlXG4gICAgICAgIG1lc3NhZ2UgPSBwYWNrZXIucGFjayh7XG4gICAgICAgICAgcmVzdWx0OiBudWxsXG4gICAgICAgIH0sIGlkKTtcblxuICAgICAgLy8gU3RvcmUgdGhlIHJlc3BvbnNlIHRvIHByZXZlbnQgdG8gcHJvY2VzcyBhIGR1cGxpY2F0ZWQgcmVxdWVzdCBsYXRlclxuICAgICAgc3RvcmVSZXNwb25zZShtZXNzYWdlLCBpZCwgZnJvbSk7XG5cbiAgICAgIC8vIFJldHVybiB0aGUgc3RvcmVkIHJlc3BvbnNlIHNvIGl0IGNhbiBiZSBkaXJlY3RseSBzZW5kIGJhY2tcbiAgICAgIHRyYW5zcG9ydCA9IHRyYW5zcG9ydCB8fCB0aGlzLmdldFRyYW5zcG9ydCgpIHx8IHNlbGYuZ2V0VHJhbnNwb3J0KCk7XG5cbiAgICAgIGlmICh0cmFuc3BvcnQpXG4gICAgICAgIHJldHVybiB0cmFuc3BvcnQuc2VuZChtZXNzYWdlKTtcblxuICAgICAgcmV0dXJuIG1lc3NhZ2U7XG4gICAgfVxuICB9O1xuICBpbmhlcml0cyhScGNSZXF1ZXN0LCBScGNOb3RpZmljYXRpb24pO1xuXG4gIGZ1bmN0aW9uIGNhbmNlbChtZXNzYWdlKSB7XG4gICAgdmFyIGtleSA9IG1lc3NhZ2UyS2V5W21lc3NhZ2VdO1xuICAgIGlmICgha2V5KSByZXR1cm47XG5cbiAgICBkZWxldGUgbWVzc2FnZTJLZXlbbWVzc2FnZV07XG5cbiAgICB2YXIgcmVxdWVzdCA9IHJlcXVlc3RzLnBvcChrZXkuaWQsIGtleS5kZXN0KTtcbiAgICBpZiAoIXJlcXVlc3QpIHJldHVybjtcblxuICAgIGNsZWFyVGltZW91dChyZXF1ZXN0LnRpbWVvdXQpO1xuXG4gICAgLy8gU3RhcnQgZHVwbGljYXRlZCByZXNwb25zZXMgdGltZW91dFxuICAgIHN0b3JlUHJvY2Vzc2VkUmVzcG9uc2Uoa2V5LmlkLCBrZXkuZGVzdCk7XG4gIH07XG5cbiAgLyoqXG4gICAqIEFsbG93IHRvIGNhbmNlbCBhIHJlcXVlc3QgYW5kIGRvbid0IHdhaXQgZm9yIGEgcmVzcG9uc2VcbiAgICpcbiAgICogSWYgYG1lc3NhZ2VgIGlzIG5vdCBnaXZlbiwgY2FuY2VsIGFsbCB0aGUgcmVxdWVzdFxuICAgKi9cbiAgdGhpcy5jYW5jZWwgPSBmdW5jdGlvbiAobWVzc2FnZSkge1xuICAgIGlmIChtZXNzYWdlKSByZXR1cm4gY2FuY2VsKG1lc3NhZ2UpO1xuXG4gICAgZm9yICh2YXIgbWVzc2FnZSBpbiBtZXNzYWdlMktleSlcbiAgICAgIGNhbmNlbChtZXNzYWdlKTtcbiAgfTtcblxuICB0aGlzLmNsb3NlID0gZnVuY3Rpb24gKCkge1xuICAgIC8vIFByZXZlbnQgdG8gcmVjZWl2ZSBuZXcgbWVzc2FnZXNcbiAgICB2YXIgdHJhbnNwb3J0ID0gdGhpcy5nZXRUcmFuc3BvcnQoKTtcbiAgICBpZiAodHJhbnNwb3J0ICYmIHRyYW5zcG9ydC5jbG9zZSlcbiAgICAgIHRyYW5zcG9ydC5jbG9zZSgpO1xuXG4gICAgLy8gUmVxdWVzdCAmIHByb2Nlc3NlZCByZXNwb25zZXNcbiAgICB0aGlzLmNhbmNlbCgpO1xuXG4gICAgcHJvY2Vzc2VkUmVzcG9uc2VzLmZvckVhY2goY2xlYXJUaW1lb3V0KTtcblxuICAgIC8vIFJlc3BvbnNlc1xuICAgIHJlc3BvbnNlcy5mb3JFYWNoKGZ1bmN0aW9uIChyZXNwb25zZSkge1xuICAgICAgY2xlYXJUaW1lb3V0KHJlc3BvbnNlLnRpbWVvdXQpO1xuICAgIH0pO1xuICB9O1xuXG4gIC8qKlxuICAgKiBHZW5lcmF0ZXMgYW5kIGVuY29kZSBhIEpzb25SUEMgMi4wIG1lc3NhZ2VcbiAgICpcbiAgICogQHBhcmFtIHtTdHJpbmd9IG1ldGhvZCAtbWV0aG9kIG9mIHRoZSBub3RpZmljYXRpb25cbiAgICogQHBhcmFtIHBhcmFtcyAtIHBhcmFtZXRlcnMgb2YgdGhlIG5vdGlmaWNhdGlvblxuICAgKiBAcGFyYW0gW2Rlc3RdIC0gZGVzdGluYXRpb24gb2YgdGhlIG5vdGlmaWNhdGlvblxuICAgKiBAcGFyYW0ge29iamVjdH0gW3RyYW5zcG9ydF0gLSB0cmFuc3BvcnQgd2hlcmUgdG8gc2VuZCB0aGUgbWVzc2FnZVxuICAgKiBAcGFyYW0gW2NhbGxiYWNrXSAtIGZ1bmN0aW9uIGNhbGxlZCB3aGVuIGEgcmVzcG9uc2UgdG8gdGhpcyByZXF1ZXN0IGlzXG4gICAqICAgcmVjZWl2ZWQuIElmIG5vdCBkZWZpbmVkLCBhIG5vdGlmaWNhdGlvbiB3aWxsIGJlIHNlbmQgaW5zdGVhZFxuICAgKlxuICAgKiBAcmV0dXJucyB7c3RyaW5nfSBBIHJhdyBKc29uUlBDIDIuMCByZXF1ZXN0IG9yIG5vdGlmaWNhdGlvbiBzdHJpbmdcbiAgICovXG4gIHRoaXMuZW5jb2RlID0gZnVuY3Rpb24gKG1ldGhvZCwgcGFyYW1zLCBkZXN0LCB0cmFuc3BvcnQsIGNhbGxiYWNrKSB7XG4gICAgLy8gRml4IG9wdGlvbmFsIHBhcmFtZXRlcnNcbiAgICBpZiAocGFyYW1zIGluc3RhbmNlb2YgRnVuY3Rpb24pIHtcbiAgICAgIGlmIChkZXN0ICE9IHVuZGVmaW5lZClcbiAgICAgICAgdGhyb3cgbmV3IFN5bnRheEVycm9yKFwiVGhlcmUgY2FuJ3QgYmUgcGFyYW1ldGVycyBhZnRlciBjYWxsYmFja1wiKTtcblxuICAgICAgY2FsbGJhY2sgPSBwYXJhbXM7XG4gICAgICB0cmFuc3BvcnQgPSB1bmRlZmluZWQ7XG4gICAgICBkZXN0ID0gdW5kZWZpbmVkO1xuICAgICAgcGFyYW1zID0gdW5kZWZpbmVkO1xuICAgIH0gZWxzZSBpZiAoZGVzdCBpbnN0YW5jZW9mIEZ1bmN0aW9uKSB7XG4gICAgICBpZiAodHJhbnNwb3J0ICE9IHVuZGVmaW5lZClcbiAgICAgICAgdGhyb3cgbmV3IFN5bnRheEVycm9yKFwiVGhlcmUgY2FuJ3QgYmUgcGFyYW1ldGVycyBhZnRlciBjYWxsYmFja1wiKTtcblxuICAgICAgY2FsbGJhY2sgPSBkZXN0O1xuICAgICAgdHJhbnNwb3J0ID0gdW5kZWZpbmVkO1xuICAgICAgZGVzdCA9IHVuZGVmaW5lZDtcbiAgICB9IGVsc2UgaWYgKHRyYW5zcG9ydCBpbnN0YW5jZW9mIEZ1bmN0aW9uKSB7XG4gICAgICBpZiAoY2FsbGJhY2sgIT0gdW5kZWZpbmVkKVxuICAgICAgICB0aHJvdyBuZXcgU3ludGF4RXJyb3IoXCJUaGVyZSBjYW4ndCBiZSBwYXJhbWV0ZXJzIGFmdGVyIGNhbGxiYWNrXCIpO1xuXG4gICAgICBjYWxsYmFjayA9IHRyYW5zcG9ydDtcbiAgICAgIHRyYW5zcG9ydCA9IHVuZGVmaW5lZDtcbiAgICB9O1xuXG4gICAgaWYgKHNlbGYucGVlcklEICE9IHVuZGVmaW5lZCkge1xuICAgICAgcGFyYW1zID0gcGFyYW1zIHx8IHt9O1xuXG4gICAgICBwYXJhbXMuZnJvbSA9IHNlbGYucGVlcklEO1xuICAgIH07XG5cbiAgICBpZiAoZGVzdCAhPSB1bmRlZmluZWQpIHtcbiAgICAgIHBhcmFtcyA9IHBhcmFtcyB8fCB7fTtcblxuICAgICAgcGFyYW1zLmRlc3QgPSBkZXN0O1xuICAgIH07XG5cbiAgICAvLyBFbmNvZGUgbWVzc2FnZVxuICAgIHZhciBtZXNzYWdlID0ge1xuICAgICAgbWV0aG9kOiBtZXRob2QsXG4gICAgICBwYXJhbXM6IHBhcmFtc1xuICAgIH07XG5cbiAgICBpZiAoY2FsbGJhY2spIHtcbiAgICAgIHZhciBpZCA9IHJlcXVlc3RJRCsrO1xuICAgICAgdmFyIHJldHJpZWQgPSAwO1xuXG4gICAgICBtZXNzYWdlID0gcGFja2VyLnBhY2sobWVzc2FnZSwgaWQpO1xuXG4gICAgICBmdW5jdGlvbiBkaXNwYXRjaENhbGxiYWNrKGVycm9yLCByZXN1bHQpIHtcbiAgICAgICAgc2VsZi5jYW5jZWwobWVzc2FnZSk7XG5cbiAgICAgICAgY2FsbGJhY2soZXJyb3IsIHJlc3VsdCk7XG4gICAgICB9O1xuXG4gICAgICB2YXIgcmVxdWVzdCA9IHtcbiAgICAgICAgbWVzc2FnZTogbWVzc2FnZSxcbiAgICAgICAgY2FsbGJhY2s6IGRpc3BhdGNoQ2FsbGJhY2ssXG4gICAgICAgIHJlc3BvbnNlTWV0aG9kczogcmVzcG9uc2VNZXRob2RzW21ldGhvZF0gfHwge31cbiAgICAgIH07XG5cbiAgICAgIHZhciBlbmNvZGVfdHJhbnNwb3J0ID0gdW5pZnlUcmFuc3BvcnQodHJhbnNwb3J0KTtcblxuICAgICAgZnVuY3Rpb24gc2VuZFJlcXVlc3QodHJhbnNwb3J0KSB7XG4gICAgICAgIHZhciBydCA9IChtZXRob2QgPT09ICdwaW5nJyA/IHBpbmdfcmVxdWVzdF90aW1lb3V0IDogcmVxdWVzdF90aW1lb3V0KTtcbiAgICAgICAgcmVxdWVzdC50aW1lb3V0ID0gc2V0VGltZW91dCh0aW1lb3V0LCBydCAqIE1hdGgucG93KDIsIHJldHJpZWQrKykpO1xuICAgICAgICBtZXNzYWdlMktleVttZXNzYWdlXSA9IHtcbiAgICAgICAgICBpZDogaWQsXG4gICAgICAgICAgZGVzdDogZGVzdFxuICAgICAgICB9O1xuICAgICAgICByZXF1ZXN0cy5zZXQocmVxdWVzdCwgaWQsIGRlc3QpO1xuXG4gICAgICAgIHRyYW5zcG9ydCA9IHRyYW5zcG9ydCB8fCBlbmNvZGVfdHJhbnNwb3J0IHx8IHNlbGYuZ2V0VHJhbnNwb3J0KCk7XG4gICAgICAgIGlmICh0cmFuc3BvcnQpXG4gICAgICAgICAgcmV0dXJuIHRyYW5zcG9ydC5zZW5kKG1lc3NhZ2UpO1xuXG4gICAgICAgIHJldHVybiBtZXNzYWdlO1xuICAgICAgfTtcblxuICAgICAgZnVuY3Rpb24gcmV0cnkodHJhbnNwb3J0KSB7XG4gICAgICAgIHRyYW5zcG9ydCA9IHVuaWZ5VHJhbnNwb3J0KHRyYW5zcG9ydCk7XG5cbiAgICAgICAgY29uc29sZS53YXJuKHJldHJpZWQgKyAnIHJldHJ5IGZvciByZXF1ZXN0IG1lc3NhZ2U6JywgbWVzc2FnZSk7XG5cbiAgICAgICAgdmFyIHRpbWVvdXQgPSBwcm9jZXNzZWRSZXNwb25zZXMucG9wKGlkLCBkZXN0KTtcbiAgICAgICAgY2xlYXJUaW1lb3V0KHRpbWVvdXQpO1xuXG4gICAgICAgIHJldHVybiBzZW5kUmVxdWVzdCh0cmFuc3BvcnQpO1xuICAgICAgfTtcblxuICAgICAgZnVuY3Rpb24gdGltZW91dCgpIHtcbiAgICAgICAgaWYgKHJldHJpZWQgPCBtYXhfcmV0cmllcylcbiAgICAgICAgICByZXR1cm4gcmV0cnkodHJhbnNwb3J0KTtcblxuICAgICAgICB2YXIgZXJyb3IgPSBuZXcgRXJyb3IoJ1JlcXVlc3QgaGFzIHRpbWVkIG91dCcpO1xuICAgICAgICBlcnJvci5yZXF1ZXN0ID0gbWVzc2FnZTtcblxuICAgICAgICBlcnJvci5yZXRyeSA9IHJldHJ5O1xuXG4gICAgICAgIGRpc3BhdGNoQ2FsbGJhY2soZXJyb3IpXG4gICAgICB9O1xuXG4gICAgICByZXR1cm4gc2VuZFJlcXVlc3QodHJhbnNwb3J0KTtcbiAgICB9O1xuXG4gICAgLy8gUmV0dXJuIHRoZSBwYWNrZWQgbWVzc2FnZVxuICAgIG1lc3NhZ2UgPSBwYWNrZXIucGFjayhtZXNzYWdlKTtcblxuICAgIHRyYW5zcG9ydCA9IHRyYW5zcG9ydCB8fCB0aGlzLmdldFRyYW5zcG9ydCgpO1xuICAgIGlmICh0cmFuc3BvcnQpXG4gICAgICByZXR1cm4gdHJhbnNwb3J0LnNlbmQobWVzc2FnZSk7XG5cbiAgICByZXR1cm4gbWVzc2FnZTtcbiAgfTtcblxuICAvKipcbiAgICogRGVjb2RlIGFuZCBwcm9jZXNzIGEgSnNvblJQQyAyLjAgbWVzc2FnZVxuICAgKlxuICAgKiBAcGFyYW0ge3N0cmluZ30gbWVzc2FnZSAtIHN0cmluZyB3aXRoIHRoZSBjb250ZW50IG9mIHRoZSBtZXNzYWdlXG4gICAqXG4gICAqIEByZXR1cm5zIHtScGNOb3RpZmljYXRpb258UnBjUmVxdWVzdHx1bmRlZmluZWR9IC0gdGhlIHJlcHJlc2VudGF0aW9uIG9mIHRoZVxuICAgKiAgIG5vdGlmaWNhdGlvbiBvciB0aGUgcmVxdWVzdC4gSWYgYSByZXNwb25zZSB3YXMgcHJvY2Vzc2VkLCBpdCB3aWxsIHJldHVyblxuICAgKiAgIGB1bmRlZmluZWRgIHRvIG5vdGlmeSB0aGF0IGl0IHdhcyBwcm9jZXNzZWRcbiAgICpcbiAgICogQHRocm93cyB7VHlwZUVycm9yfSAtIE1lc3NhZ2UgaXMgbm90IGRlZmluZWRcbiAgICovXG4gIHRoaXMuZGVjb2RlID0gZnVuY3Rpb24gKG1lc3NhZ2UsIHRyYW5zcG9ydCkge1xuICAgIGlmICghbWVzc2FnZSlcbiAgICAgIHRocm93IG5ldyBUeXBlRXJyb3IoXCJNZXNzYWdlIGlzIG5vdCBkZWZpbmVkXCIpO1xuXG4gICAgdHJ5IHtcbiAgICAgIG1lc3NhZ2UgPSBwYWNrZXIudW5wYWNrKG1lc3NhZ2UpO1xuICAgIH0gY2F0Y2ggKGUpIHtcbiAgICAgIC8vIElnbm9yZSBpbnZhbGlkIG1lc3NhZ2VzXG4gICAgICByZXR1cm4gY29uc29sZS5kZWJ1ZyhlLCBtZXNzYWdlKTtcbiAgICB9O1xuXG4gICAgdmFyIGlkID0gbWVzc2FnZS5pZDtcbiAgICB2YXIgYWNrID0gbWVzc2FnZS5hY2s7XG4gICAgdmFyIG1ldGhvZCA9IG1lc3NhZ2UubWV0aG9kO1xuICAgIHZhciBwYXJhbXMgPSBtZXNzYWdlLnBhcmFtcyB8fCB7fTtcblxuICAgIHZhciBmcm9tID0gcGFyYW1zLmZyb207XG4gICAgdmFyIGRlc3QgPSBwYXJhbXMuZGVzdDtcblxuICAgIC8vIElnbm9yZSBtZXNzYWdlcyBzZW5kIGJ5IHVzXG4gICAgaWYgKHNlbGYucGVlcklEICE9IHVuZGVmaW5lZCAmJiBmcm9tID09IHNlbGYucGVlcklEKSByZXR1cm47XG5cbiAgICAvLyBOb3RpZmljYXRpb25cbiAgICBpZiAoaWQgPT0gdW5kZWZpbmVkICYmIGFjayA9PSB1bmRlZmluZWQpIHtcbiAgICAgIHZhciBub3RpZmljYXRpb24gPSBuZXcgUnBjTm90aWZpY2F0aW9uKG1ldGhvZCwgcGFyYW1zKTtcblxuICAgICAgaWYgKHNlbGYuZW1pdCgncmVxdWVzdCcsIG5vdGlmaWNhdGlvbikpIHJldHVybjtcbiAgICAgIHJldHVybiBub3RpZmljYXRpb247XG4gICAgfTtcblxuICAgIGZ1bmN0aW9uIHByb2Nlc3NSZXF1ZXN0KCkge1xuICAgICAgLy8gSWYgd2UgaGF2ZSBhIHRyYW5zcG9ydCBhbmQgaXQncyBhIGR1cGxpY2F0ZWQgcmVxdWVzdCwgcmVwbHkgaW5tZWRpYXRseVxuICAgICAgdHJhbnNwb3J0ID0gdW5pZnlUcmFuc3BvcnQodHJhbnNwb3J0KSB8fCBzZWxmLmdldFRyYW5zcG9ydCgpO1xuICAgICAgaWYgKHRyYW5zcG9ydCkge1xuICAgICAgICB2YXIgcmVzcG9uc2UgPSByZXNwb25zZXMuZ2V0KGlkLCBmcm9tKTtcbiAgICAgICAgaWYgKHJlc3BvbnNlKVxuICAgICAgICAgIHJldHVybiB0cmFuc3BvcnQuc2VuZChyZXNwb25zZS5tZXNzYWdlKTtcbiAgICAgIH07XG5cbiAgICAgIHZhciBpZEFjayA9IChpZCAhPSB1bmRlZmluZWQpID8gaWQgOiBhY2s7XG4gICAgICB2YXIgcmVxdWVzdCA9IG5ldyBScGNSZXF1ZXN0KG1ldGhvZCwgcGFyYW1zLCBpZEFjaywgZnJvbSwgdHJhbnNwb3J0KTtcblxuICAgICAgaWYgKHNlbGYuZW1pdCgncmVxdWVzdCcsIHJlcXVlc3QpKSByZXR1cm47XG4gICAgICByZXR1cm4gcmVxdWVzdDtcbiAgICB9O1xuXG4gICAgZnVuY3Rpb24gcHJvY2Vzc1Jlc3BvbnNlKHJlcXVlc3QsIGVycm9yLCByZXN1bHQpIHtcbiAgICAgIHJlcXVlc3QuY2FsbGJhY2soZXJyb3IsIHJlc3VsdCk7XG4gICAgfTtcblxuICAgIGZ1bmN0aW9uIGR1cGxpY2F0ZWRSZXNwb25zZSh0aW1lb3V0KSB7XG4gICAgICBjb25zb2xlLndhcm4oXCJSZXNwb25zZSBhbHJlYWR5IHByb2Nlc3NlZFwiLCBtZXNzYWdlKTtcblxuICAgICAgLy8gVXBkYXRlIGR1cGxpY2F0ZWQgcmVzcG9uc2VzIHRpbWVvdXRcbiAgICAgIGNsZWFyVGltZW91dCh0aW1lb3V0KTtcbiAgICAgIHN0b3JlUHJvY2Vzc2VkUmVzcG9uc2UoYWNrLCBmcm9tKTtcbiAgICB9O1xuXG4gICAgLy8gUmVxdWVzdCwgb3IgcmVzcG9uc2Ugd2l0aCBvd24gbWV0aG9kXG4gICAgaWYgKG1ldGhvZCkge1xuICAgICAgLy8gQ2hlY2sgaWYgaXQncyBhIHJlc3BvbnNlIHdpdGggb3duIG1ldGhvZFxuICAgICAgaWYgKGRlc3QgPT0gdW5kZWZpbmVkIHx8IGRlc3QgPT0gc2VsZi5wZWVySUQpIHtcbiAgICAgICAgdmFyIHJlcXVlc3QgPSByZXF1ZXN0cy5nZXQoYWNrLCBmcm9tKTtcbiAgICAgICAgaWYgKHJlcXVlc3QpIHtcbiAgICAgICAgICB2YXIgcmVzcG9uc2VNZXRob2RzID0gcmVxdWVzdC5yZXNwb25zZU1ldGhvZHM7XG5cbiAgICAgICAgICBpZiAobWV0aG9kID09IHJlc3BvbnNlTWV0aG9kcy5lcnJvcilcbiAgICAgICAgICAgIHJldHVybiBwcm9jZXNzUmVzcG9uc2UocmVxdWVzdCwgcGFyYW1zKTtcblxuICAgICAgICAgIGlmIChtZXRob2QgPT0gcmVzcG9uc2VNZXRob2RzLnJlc3BvbnNlKVxuICAgICAgICAgICAgcmV0dXJuIHByb2Nlc3NSZXNwb25zZShyZXF1ZXN0LCBudWxsLCBwYXJhbXMpO1xuXG4gICAgICAgICAgcmV0dXJuIHByb2Nlc3NSZXF1ZXN0KCk7XG4gICAgICAgIH1cblxuICAgICAgICB2YXIgcHJvY2Vzc2VkID0gcHJvY2Vzc2VkUmVzcG9uc2VzLmdldChhY2ssIGZyb20pO1xuICAgICAgICBpZiAocHJvY2Vzc2VkKVxuICAgICAgICAgIHJldHVybiBkdXBsaWNhdGVkUmVzcG9uc2UocHJvY2Vzc2VkKTtcbiAgICAgIH1cblxuICAgICAgLy8gUmVxdWVzdFxuICAgICAgcmV0dXJuIHByb2Nlc3NSZXF1ZXN0KCk7XG4gICAgfTtcblxuICAgIHZhciBlcnJvciA9IG1lc3NhZ2UuZXJyb3I7XG4gICAgdmFyIHJlc3VsdCA9IG1lc3NhZ2UucmVzdWx0O1xuXG4gICAgLy8gSWdub3JlIHJlc3BvbnNlcyBub3Qgc2VuZCB0byB1c1xuICAgIGlmIChlcnJvciAmJiBlcnJvci5kZXN0ICYmIGVycm9yLmRlc3QgIT0gc2VsZi5wZWVySUQpIHJldHVybjtcbiAgICBpZiAocmVzdWx0ICYmIHJlc3VsdC5kZXN0ICYmIHJlc3VsdC5kZXN0ICE9IHNlbGYucGVlcklEKSByZXR1cm47XG5cbiAgICAvLyBSZXNwb25zZVxuICAgIHZhciByZXF1ZXN0ID0gcmVxdWVzdHMuZ2V0KGFjaywgZnJvbSk7XG4gICAgaWYgKCFyZXF1ZXN0KSB7XG4gICAgICB2YXIgcHJvY2Vzc2VkID0gcHJvY2Vzc2VkUmVzcG9uc2VzLmdldChhY2ssIGZyb20pO1xuICAgICAgaWYgKHByb2Nlc3NlZClcbiAgICAgICAgcmV0dXJuIGR1cGxpY2F0ZWRSZXNwb25zZShwcm9jZXNzZWQpO1xuXG4gICAgICByZXR1cm4gY29uc29sZS53YXJuKFwiTm8gY2FsbGJhY2sgd2FzIGRlZmluZWQgZm9yIHRoaXMgbWVzc2FnZVwiLFxuICAgICAgICBtZXNzYWdlKTtcbiAgICB9O1xuXG4gICAgLy8gUHJvY2VzcyByZXNwb25zZVxuICAgIHByb2Nlc3NSZXNwb25zZShyZXF1ZXN0LCBlcnJvciwgcmVzdWx0KTtcbiAgfTtcbn07XG5pbmhlcml0cyhScGNCdWlsZGVyLCBFdmVudEVtaXR0ZXIpO1xuXG5ScGNCdWlsZGVyLlJwY05vdGlmaWNhdGlvbiA9IFJwY05vdGlmaWNhdGlvbjtcblxubW9kdWxlLmV4cG9ydHMgPSBScGNCdWlsZGVyO1xuXG52YXIgY2xpZW50cyA9IHJlcXVpcmUoJy4vY2xpZW50cycpO1xudmFyIHRyYW5zcG9ydHMgPSByZXF1aXJlKCcuL2NsaWVudHMvdHJhbnNwb3J0cycpO1xuXG5ScGNCdWlsZGVyLmNsaWVudHMgPSBjbGllbnRzO1xuUnBjQnVpbGRlci5jbGllbnRzLnRyYW5zcG9ydHMgPSB0cmFuc3BvcnRzO1xuUnBjQnVpbGRlci5wYWNrZXJzID0gcGFja2VycztcbiIsIi8qKlxuICogSnNvblJQQyAyLjAgcGFja2VyXG4gKi9cblxuLyoqXG4gKiBQYWNrIGEgSnNvblJQQyAyLjAgbWVzc2FnZVxuICpcbiAqIEBwYXJhbSB7T2JqZWN0fSBtZXNzYWdlIC0gb2JqZWN0IHRvIGJlIHBhY2thZ2VkLiBJdCByZXF1aXJlcyB0byBoYXZlIGFsbCB0aGVcbiAqICAgZmllbGRzIG5lZWRlZCBieSB0aGUgSnNvblJQQyAyLjAgbWVzc2FnZSB0aGF0IGl0J3MgZ29pbmcgdG8gYmUgZ2VuZXJhdGVkXG4gKlxuICogQHJldHVybiB7U3RyaW5nfSAtIHRoZSBzdHJpbmdpZmllZCBKc29uUlBDIDIuMCBtZXNzYWdlXG4gKi9cbmZ1bmN0aW9uIHBhY2sobWVzc2FnZSwgaWQpIHtcbiAgdmFyIHJlc3VsdCA9IHtcbiAgICBqc29ucnBjOiBcIjIuMFwiXG4gIH07XG5cbiAgLy8gUmVxdWVzdFxuICBpZiAobWVzc2FnZS5tZXRob2QpIHtcbiAgICByZXN1bHQubWV0aG9kID0gbWVzc2FnZS5tZXRob2Q7XG5cbiAgICBpZiAobWVzc2FnZS5wYXJhbXMpXG4gICAgICByZXN1bHQucGFyYW1zID0gbWVzc2FnZS5wYXJhbXM7XG5cbiAgICAvLyBSZXF1ZXN0IGlzIGEgbm90aWZpY2F0aW9uXG4gICAgaWYgKGlkICE9IHVuZGVmaW5lZClcbiAgICAgIHJlc3VsdC5pZCA9IGlkO1xuICB9XG5cbiAgLy8gUmVzcG9uc2VcbiAgZWxzZSBpZiAoaWQgIT0gdW5kZWZpbmVkKSB7XG4gICAgaWYgKG1lc3NhZ2UuZXJyb3IpIHtcbiAgICAgIGlmIChtZXNzYWdlLnJlc3VsdCAhPT0gdW5kZWZpbmVkKVxuICAgICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKFwiQm90aCByZXN1bHQgYW5kIGVycm9yIGFyZSBkZWZpbmVkXCIpO1xuXG4gICAgICByZXN1bHQuZXJyb3IgPSBtZXNzYWdlLmVycm9yO1xuICAgIH0gZWxzZSBpZiAobWVzc2FnZS5yZXN1bHQgIT09IHVuZGVmaW5lZClcbiAgICAgIHJlc3VsdC5yZXN1bHQgPSBtZXNzYWdlLnJlc3VsdDtcbiAgICBlbHNlXG4gICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKFwiTm8gcmVzdWx0IG9yIGVycm9yIGlzIGRlZmluZWRcIik7XG5cbiAgICByZXN1bHQuaWQgPSBpZDtcbiAgfTtcblxuICByZXR1cm4gSlNPTi5zdHJpbmdpZnkocmVzdWx0KTtcbn07XG5cbi8qKlxuICogVW5wYWNrIGEgSnNvblJQQyAyLjAgbWVzc2FnZVxuICpcbiAqIEBwYXJhbSB7U3RyaW5nfSBtZXNzYWdlIC0gc3RyaW5nIHdpdGggdGhlIGNvbnRlbnQgb2YgdGhlIEpzb25SUEMgMi4wIG1lc3NhZ2VcbiAqXG4gKiBAdGhyb3dzIHtUeXBlRXJyb3J9IC0gSW52YWxpZCBKc29uUlBDIHZlcnNpb25cbiAqXG4gKiBAcmV0dXJuIHtPYmplY3R9IC0gb2JqZWN0IGZpbGxlZCB3aXRoIHRoZSBKc29uUlBDIDIuMCBtZXNzYWdlIGNvbnRlbnRcbiAqL1xuZnVuY3Rpb24gdW5wYWNrKG1lc3NhZ2UpIHtcbiAgdmFyIHJlc3VsdCA9IG1lc3NhZ2U7XG5cbiAgaWYgKHR5cGVvZiBtZXNzYWdlID09PSAnc3RyaW5nJyB8fCBtZXNzYWdlIGluc3RhbmNlb2YgU3RyaW5nKSB7XG4gICAgcmVzdWx0ID0gSlNPTi5wYXJzZShtZXNzYWdlKTtcbiAgfVxuXG4gIC8vIENoZWNrIGlmIGl0J3MgYSB2YWxpZCBtZXNzYWdlXG5cbiAgdmFyIHZlcnNpb24gPSByZXN1bHQuanNvbnJwYztcbiAgaWYgKHZlcnNpb24gIT09ICcyLjAnKVxuICAgIHRocm93IG5ldyBUeXBlRXJyb3IoXCJJbnZhbGlkIEpzb25SUEMgdmVyc2lvbiAnXCIgKyB2ZXJzaW9uICsgXCInOiBcIiArXG4gICAgICBtZXNzYWdlKTtcblxuICAvLyBSZXNwb25zZVxuICBpZiAocmVzdWx0Lm1ldGhvZCA9PSB1bmRlZmluZWQpIHtcbiAgICBpZiAocmVzdWx0LmlkID09IHVuZGVmaW5lZClcbiAgICAgIHRocm93IG5ldyBUeXBlRXJyb3IoXCJJbnZhbGlkIG1lc3NhZ2U6IFwiICsgbWVzc2FnZSk7XG5cbiAgICB2YXIgcmVzdWx0X2RlZmluZWQgPSByZXN1bHQucmVzdWx0ICE9PSB1bmRlZmluZWQ7XG4gICAgdmFyIGVycm9yX2RlZmluZWQgPSByZXN1bHQuZXJyb3IgIT09IHVuZGVmaW5lZDtcblxuICAgIC8vIENoZWNrIG9ubHkgcmVzdWx0IG9yIGVycm9yIGlzIGRlZmluZWQsIG5vdCBib3RoIG9yIG5vbmVcbiAgICBpZiAocmVzdWx0X2RlZmluZWQgJiYgZXJyb3JfZGVmaW5lZClcbiAgICAgIHRocm93IG5ldyBUeXBlRXJyb3IoXCJCb3RoIHJlc3VsdCBhbmQgZXJyb3IgYXJlIGRlZmluZWQ6IFwiICsgbWVzc2FnZSk7XG5cbiAgICBpZiAoIXJlc3VsdF9kZWZpbmVkICYmICFlcnJvcl9kZWZpbmVkKVxuICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcihcIk5vIHJlc3VsdCBvciBlcnJvciBpcyBkZWZpbmVkOiBcIiArIG1lc3NhZ2UpO1xuXG4gICAgcmVzdWx0LmFjayA9IHJlc3VsdC5pZDtcbiAgICBkZWxldGUgcmVzdWx0LmlkO1xuICB9XG5cbiAgLy8gUmV0dXJuIHVucGFja2VkIG1lc3NhZ2VcbiAgcmV0dXJuIHJlc3VsdDtcbn07XG5cbmV4cG9ydHMucGFjayA9IHBhY2s7XG5leHBvcnRzLnVucGFjayA9IHVucGFjaztcbiIsImZ1bmN0aW9uIHBhY2sobWVzc2FnZSkge1xuICB0aHJvdyBuZXcgVHlwZUVycm9yKFwiTm90IHlldCBpbXBsZW1lbnRlZFwiKTtcbn07XG5cbmZ1bmN0aW9uIHVucGFjayhtZXNzYWdlKSB7XG4gIHRocm93IG5ldyBUeXBlRXJyb3IoXCJOb3QgeWV0IGltcGxlbWVudGVkXCIpO1xufTtcblxuZXhwb3J0cy5wYWNrID0gcGFjaztcbmV4cG9ydHMudW5wYWNrID0gdW5wYWNrO1xuIiwidmFyIEpzb25SUEMgPSByZXF1aXJlKCcuL0pzb25SUEMnKTtcbnZhciBYbWxSUEMgPSByZXF1aXJlKCcuL1htbFJQQycpO1xuXG5leHBvcnRzLkpzb25SUEMgPSBKc29uUlBDO1xuZXhwb3J0cy5YbWxSUEMgPSBYbWxSUEM7XG4iLCIvLyBDb3B5cmlnaHQgSm95ZW50LCBJbmMuIGFuZCBvdGhlciBOb2RlIGNvbnRyaWJ1dG9ycy5cbi8vXG4vLyBQZXJtaXNzaW9uIGlzIGhlcmVieSBncmFudGVkLCBmcmVlIG9mIGNoYXJnZSwgdG8gYW55IHBlcnNvbiBvYnRhaW5pbmcgYVxuLy8gY29weSBvZiB0aGlzIHNvZnR3YXJlIGFuZCBhc3NvY2lhdGVkIGRvY3VtZW50YXRpb24gZmlsZXMgKHRoZVxuLy8gXCJTb2Z0d2FyZVwiKSwgdG8gZGVhbCBpbiB0aGUgU29mdHdhcmUgd2l0aG91dCByZXN0cmljdGlvbiwgaW5jbHVkaW5nXG4vLyB3aXRob3V0IGxpbWl0YXRpb24gdGhlIHJpZ2h0cyB0byB1c2UsIGNvcHksIG1vZGlmeSwgbWVyZ2UsIHB1Ymxpc2gsXG4vLyBkaXN0cmlidXRlLCBzdWJsaWNlbnNlLCBhbmQvb3Igc2VsbCBjb3BpZXMgb2YgdGhlIFNvZnR3YXJlLCBhbmQgdG8gcGVybWl0XG4vLyBwZXJzb25zIHRvIHdob20gdGhlIFNvZnR3YXJlIGlzIGZ1cm5pc2hlZCB0byBkbyBzbywgc3ViamVjdCB0byB0aGVcbi8vIGZvbGxvd2luZyBjb25kaXRpb25zOlxuLy9cbi8vIFRoZSBhYm92ZSBjb3B5cmlnaHQgbm90aWNlIGFuZCB0aGlzIHBlcm1pc3Npb24gbm90aWNlIHNoYWxsIGJlIGluY2x1ZGVkXG4vLyBpbiBhbGwgY29waWVzIG9yIHN1YnN0YW50aWFsIHBvcnRpb25zIG9mIHRoZSBTb2Z0d2FyZS5cbi8vXG4vLyBUSEUgU09GVFdBUkUgSVMgUFJPVklERUQgXCJBUyBJU1wiLCBXSVRIT1VUIFdBUlJBTlRZIE9GIEFOWSBLSU5ELCBFWFBSRVNTXG4vLyBPUiBJTVBMSUVELCBJTkNMVURJTkcgQlVUIE5PVCBMSU1JVEVEIFRPIFRIRSBXQVJSQU5USUVTIE9GXG4vLyBNRVJDSEFOVEFCSUxJVFksIEZJVE5FU1MgRk9SIEEgUEFSVElDVUxBUiBQVVJQT1NFIEFORCBOT05JTkZSSU5HRU1FTlQuIElOXG4vLyBOTyBFVkVOVCBTSEFMTCBUSEUgQVVUSE9SUyBPUiBDT1BZUklHSFQgSE9MREVSUyBCRSBMSUFCTEUgRk9SIEFOWSBDTEFJTSxcbi8vIERBTUFHRVMgT1IgT1RIRVIgTElBQklMSVRZLCBXSEVUSEVSIElOIEFOIEFDVElPTiBPRiBDT05UUkFDVCwgVE9SVCBPUlxuLy8gT1RIRVJXSVNFLCBBUklTSU5HIEZST00sIE9VVCBPRiBPUiBJTiBDT05ORUNUSU9OIFdJVEggVEhFIFNPRlRXQVJFIE9SIFRIRVxuLy8gVVNFIE9SIE9USEVSIERFQUxJTkdTIElOIFRIRSBTT0ZUV0FSRS5cblxudmFyIG9iamVjdENyZWF0ZSA9IE9iamVjdC5jcmVhdGUgfHwgb2JqZWN0Q3JlYXRlUG9seWZpbGxcbnZhciBvYmplY3RLZXlzID0gT2JqZWN0LmtleXMgfHwgb2JqZWN0S2V5c1BvbHlmaWxsXG52YXIgYmluZCA9IEZ1bmN0aW9uLnByb3RvdHlwZS5iaW5kIHx8IGZ1bmN0aW9uQmluZFBvbHlmaWxsXG5cbmZ1bmN0aW9uIEV2ZW50RW1pdHRlcigpIHtcbiAgaWYgKCF0aGlzLl9ldmVudHMgfHwgIU9iamVjdC5wcm90b3R5cGUuaGFzT3duUHJvcGVydHkuY2FsbCh0aGlzLCAnX2V2ZW50cycpKSB7XG4gICAgdGhpcy5fZXZlbnRzID0gb2JqZWN0Q3JlYXRlKG51bGwpO1xuICAgIHRoaXMuX2V2ZW50c0NvdW50ID0gMDtcbiAgfVxuXG4gIHRoaXMuX21heExpc3RlbmVycyA9IHRoaXMuX21heExpc3RlbmVycyB8fCB1bmRlZmluZWQ7XG59XG5tb2R1bGUuZXhwb3J0cyA9IEV2ZW50RW1pdHRlcjtcblxuLy8gQmFja3dhcmRzLWNvbXBhdCB3aXRoIG5vZGUgMC4xMC54XG5FdmVudEVtaXR0ZXIuRXZlbnRFbWl0dGVyID0gRXZlbnRFbWl0dGVyO1xuXG5FdmVudEVtaXR0ZXIucHJvdG90eXBlLl9ldmVudHMgPSB1bmRlZmluZWQ7XG5FdmVudEVtaXR0ZXIucHJvdG90eXBlLl9tYXhMaXN0ZW5lcnMgPSB1bmRlZmluZWQ7XG5cbi8vIEJ5IGRlZmF1bHQgRXZlbnRFbWl0dGVycyB3aWxsIHByaW50IGEgd2FybmluZyBpZiBtb3JlIHRoYW4gMTAgbGlzdGVuZXJzIGFyZVxuLy8gYWRkZWQgdG8gaXQuIFRoaXMgaXMgYSB1c2VmdWwgZGVmYXVsdCB3aGljaCBoZWxwcyBmaW5kaW5nIG1lbW9yeSBsZWFrcy5cbnZhciBkZWZhdWx0TWF4TGlzdGVuZXJzID0gMTA7XG5cbnZhciBoYXNEZWZpbmVQcm9wZXJ0eTtcbnRyeSB7XG4gIHZhciBvID0ge307XG4gIGlmIChPYmplY3QuZGVmaW5lUHJvcGVydHkpIE9iamVjdC5kZWZpbmVQcm9wZXJ0eShvLCAneCcsIHsgdmFsdWU6IDAgfSk7XG4gIGhhc0RlZmluZVByb3BlcnR5ID0gby54ID09PSAwO1xufSBjYXRjaCAoZXJyKSB7IGhhc0RlZmluZVByb3BlcnR5ID0gZmFsc2UgfVxuaWYgKGhhc0RlZmluZVByb3BlcnR5KSB7XG4gIE9iamVjdC5kZWZpbmVQcm9wZXJ0eShFdmVudEVtaXR0ZXIsICdkZWZhdWx0TWF4TGlzdGVuZXJzJywge1xuICAgIGVudW1lcmFibGU6IHRydWUsXG4gICAgZ2V0OiBmdW5jdGlvbigpIHtcbiAgICAgIHJldHVybiBkZWZhdWx0TWF4TGlzdGVuZXJzO1xuICAgIH0sXG4gICAgc2V0OiBmdW5jdGlvbihhcmcpIHtcbiAgICAgIC8vIGNoZWNrIHdoZXRoZXIgdGhlIGlucHV0IGlzIGEgcG9zaXRpdmUgbnVtYmVyICh3aG9zZSB2YWx1ZSBpcyB6ZXJvIG9yXG4gICAgICAvLyBncmVhdGVyIGFuZCBub3QgYSBOYU4pLlxuICAgICAgaWYgKHR5cGVvZiBhcmcgIT09ICdudW1iZXInIHx8IGFyZyA8IDAgfHwgYXJnICE9PSBhcmcpXG4gICAgICAgIHRocm93IG5ldyBUeXBlRXJyb3IoJ1wiZGVmYXVsdE1heExpc3RlbmVyc1wiIG11c3QgYmUgYSBwb3NpdGl2ZSBudW1iZXInKTtcbiAgICAgIGRlZmF1bHRNYXhMaXN0ZW5lcnMgPSBhcmc7XG4gICAgfVxuICB9KTtcbn0gZWxzZSB7XG4gIEV2ZW50RW1pdHRlci5kZWZhdWx0TWF4TGlzdGVuZXJzID0gZGVmYXVsdE1heExpc3RlbmVycztcbn1cblxuLy8gT2J2aW91c2x5IG5vdCBhbGwgRW1pdHRlcnMgc2hvdWxkIGJlIGxpbWl0ZWQgdG8gMTAuIFRoaXMgZnVuY3Rpb24gYWxsb3dzXG4vLyB0aGF0IHRvIGJlIGluY3JlYXNlZC4gU2V0IHRvIHplcm8gZm9yIHVubGltaXRlZC5cbkV2ZW50RW1pdHRlci5wcm90b3R5cGUuc2V0TWF4TGlzdGVuZXJzID0gZnVuY3Rpb24gc2V0TWF4TGlzdGVuZXJzKG4pIHtcbiAgaWYgKHR5cGVvZiBuICE9PSAnbnVtYmVyJyB8fCBuIDwgMCB8fCBpc05hTihuKSlcbiAgICB0aHJvdyBuZXcgVHlwZUVycm9yKCdcIm5cIiBhcmd1bWVudCBtdXN0IGJlIGEgcG9zaXRpdmUgbnVtYmVyJyk7XG4gIHRoaXMuX21heExpc3RlbmVycyA9IG47XG4gIHJldHVybiB0aGlzO1xufTtcblxuZnVuY3Rpb24gJGdldE1heExpc3RlbmVycyh0aGF0KSB7XG4gIGlmICh0aGF0Ll9tYXhMaXN0ZW5lcnMgPT09IHVuZGVmaW5lZClcbiAgICByZXR1cm4gRXZlbnRFbWl0dGVyLmRlZmF1bHRNYXhMaXN0ZW5lcnM7XG4gIHJldHVybiB0aGF0Ll9tYXhMaXN0ZW5lcnM7XG59XG5cbkV2ZW50RW1pdHRlci5wcm90b3R5cGUuZ2V0TWF4TGlzdGVuZXJzID0gZnVuY3Rpb24gZ2V0TWF4TGlzdGVuZXJzKCkge1xuICByZXR1cm4gJGdldE1heExpc3RlbmVycyh0aGlzKTtcbn07XG5cbi8vIFRoZXNlIHN0YW5kYWxvbmUgZW1pdCogZnVuY3Rpb25zIGFyZSB1c2VkIHRvIG9wdGltaXplIGNhbGxpbmcgb2YgZXZlbnRcbi8vIGhhbmRsZXJzIGZvciBmYXN0IGNhc2VzIGJlY2F1c2UgZW1pdCgpIGl0c2VsZiBvZnRlbiBoYXMgYSB2YXJpYWJsZSBudW1iZXIgb2Zcbi8vIGFyZ3VtZW50cyBhbmQgY2FuIGJlIGRlb3B0aW1pemVkIGJlY2F1c2Ugb2YgdGhhdC4gVGhlc2UgZnVuY3Rpb25zIGFsd2F5cyBoYXZlXG4vLyB0aGUgc2FtZSBudW1iZXIgb2YgYXJndW1lbnRzIGFuZCB0aHVzIGRvIG5vdCBnZXQgZGVvcHRpbWl6ZWQsIHNvIHRoZSBjb2RlXG4vLyBpbnNpZGUgdGhlbSBjYW4gZXhlY3V0ZSBmYXN0ZXIuXG5mdW5jdGlvbiBlbWl0Tm9uZShoYW5kbGVyLCBpc0ZuLCBzZWxmKSB7XG4gIGlmIChpc0ZuKVxuICAgIGhhbmRsZXIuY2FsbChzZWxmKTtcbiAgZWxzZSB7XG4gICAgdmFyIGxlbiA9IGhhbmRsZXIubGVuZ3RoO1xuICAgIHZhciBsaXN0ZW5lcnMgPSBhcnJheUNsb25lKGhhbmRsZXIsIGxlbik7XG4gICAgZm9yICh2YXIgaSA9IDA7IGkgPCBsZW47ICsraSlcbiAgICAgIGxpc3RlbmVyc1tpXS5jYWxsKHNlbGYpO1xuICB9XG59XG5mdW5jdGlvbiBlbWl0T25lKGhhbmRsZXIsIGlzRm4sIHNlbGYsIGFyZzEpIHtcbiAgaWYgKGlzRm4pXG4gICAgaGFuZGxlci5jYWxsKHNlbGYsIGFyZzEpO1xuICBlbHNlIHtcbiAgICB2YXIgbGVuID0gaGFuZGxlci5sZW5ndGg7XG4gICAgdmFyIGxpc3RlbmVycyA9IGFycmF5Q2xvbmUoaGFuZGxlciwgbGVuKTtcbiAgICBmb3IgKHZhciBpID0gMDsgaSA8IGxlbjsgKytpKVxuICAgICAgbGlzdGVuZXJzW2ldLmNhbGwoc2VsZiwgYXJnMSk7XG4gIH1cbn1cbmZ1bmN0aW9uIGVtaXRUd28oaGFuZGxlciwgaXNGbiwgc2VsZiwgYXJnMSwgYXJnMikge1xuICBpZiAoaXNGbilcbiAgICBoYW5kbGVyLmNhbGwoc2VsZiwgYXJnMSwgYXJnMik7XG4gIGVsc2Uge1xuICAgIHZhciBsZW4gPSBoYW5kbGVyLmxlbmd0aDtcbiAgICB2YXIgbGlzdGVuZXJzID0gYXJyYXlDbG9uZShoYW5kbGVyLCBsZW4pO1xuICAgIGZvciAodmFyIGkgPSAwOyBpIDwgbGVuOyArK2kpXG4gICAgICBsaXN0ZW5lcnNbaV0uY2FsbChzZWxmLCBhcmcxLCBhcmcyKTtcbiAgfVxufVxuZnVuY3Rpb24gZW1pdFRocmVlKGhhbmRsZXIsIGlzRm4sIHNlbGYsIGFyZzEsIGFyZzIsIGFyZzMpIHtcbiAgaWYgKGlzRm4pXG4gICAgaGFuZGxlci5jYWxsKHNlbGYsIGFyZzEsIGFyZzIsIGFyZzMpO1xuICBlbHNlIHtcbiAgICB2YXIgbGVuID0gaGFuZGxlci5sZW5ndGg7XG4gICAgdmFyIGxpc3RlbmVycyA9IGFycmF5Q2xvbmUoaGFuZGxlciwgbGVuKTtcbiAgICBmb3IgKHZhciBpID0gMDsgaSA8IGxlbjsgKytpKVxuICAgICAgbGlzdGVuZXJzW2ldLmNhbGwoc2VsZiwgYXJnMSwgYXJnMiwgYXJnMyk7XG4gIH1cbn1cblxuZnVuY3Rpb24gZW1pdE1hbnkoaGFuZGxlciwgaXNGbiwgc2VsZiwgYXJncykge1xuICBpZiAoaXNGbilcbiAgICBoYW5kbGVyLmFwcGx5KHNlbGYsIGFyZ3MpO1xuICBlbHNlIHtcbiAgICB2YXIgbGVuID0gaGFuZGxlci5sZW5ndGg7XG4gICAgdmFyIGxpc3RlbmVycyA9IGFycmF5Q2xvbmUoaGFuZGxlciwgbGVuKTtcbiAgICBmb3IgKHZhciBpID0gMDsgaSA8IGxlbjsgKytpKVxuICAgICAgbGlzdGVuZXJzW2ldLmFwcGx5KHNlbGYsIGFyZ3MpO1xuICB9XG59XG5cbkV2ZW50RW1pdHRlci5wcm90b3R5cGUuZW1pdCA9IGZ1bmN0aW9uIGVtaXQodHlwZSkge1xuICB2YXIgZXIsIGhhbmRsZXIsIGxlbiwgYXJncywgaSwgZXZlbnRzO1xuICB2YXIgZG9FcnJvciA9ICh0eXBlID09PSAnZXJyb3InKTtcblxuICBldmVudHMgPSB0aGlzLl9ldmVudHM7XG4gIGlmIChldmVudHMpXG4gICAgZG9FcnJvciA9IChkb0Vycm9yICYmIGV2ZW50cy5lcnJvciA9PSBudWxsKTtcbiAgZWxzZSBpZiAoIWRvRXJyb3IpXG4gICAgcmV0dXJuIGZhbHNlO1xuXG4gIC8vIElmIHRoZXJlIGlzIG5vICdlcnJvcicgZXZlbnQgbGlzdGVuZXIgdGhlbiB0aHJvdy5cbiAgaWYgKGRvRXJyb3IpIHtcbiAgICBpZiAoYXJndW1lbnRzLmxlbmd0aCA+IDEpXG4gICAgICBlciA9IGFyZ3VtZW50c1sxXTtcbiAgICBpZiAoZXIgaW5zdGFuY2VvZiBFcnJvcikge1xuICAgICAgdGhyb3cgZXI7IC8vIFVuaGFuZGxlZCAnZXJyb3InIGV2ZW50XG4gICAgfSBlbHNlIHtcbiAgICAgIC8vIEF0IGxlYXN0IGdpdmUgc29tZSBraW5kIG9mIGNvbnRleHQgdG8gdGhlIHVzZXJcbiAgICAgIHZhciBlcnIgPSBuZXcgRXJyb3IoJ1VuaGFuZGxlZCBcImVycm9yXCIgZXZlbnQuICgnICsgZXIgKyAnKScpO1xuICAgICAgZXJyLmNvbnRleHQgPSBlcjtcbiAgICAgIHRocm93IGVycjtcbiAgICB9XG4gICAgcmV0dXJuIGZhbHNlO1xuICB9XG5cbiAgaGFuZGxlciA9IGV2ZW50c1t0eXBlXTtcblxuICBpZiAoIWhhbmRsZXIpXG4gICAgcmV0dXJuIGZhbHNlO1xuXG4gIHZhciBpc0ZuID0gdHlwZW9mIGhhbmRsZXIgPT09ICdmdW5jdGlvbic7XG4gIGxlbiA9IGFyZ3VtZW50cy5sZW5ndGg7XG4gIHN3aXRjaCAobGVuKSB7XG4gICAgICAvLyBmYXN0IGNhc2VzXG4gICAgY2FzZSAxOlxuICAgICAgZW1pdE5vbmUoaGFuZGxlciwgaXNGbiwgdGhpcyk7XG4gICAgICBicmVhaztcbiAgICBjYXNlIDI6XG4gICAgICBlbWl0T25lKGhhbmRsZXIsIGlzRm4sIHRoaXMsIGFyZ3VtZW50c1sxXSk7XG4gICAgICBicmVhaztcbiAgICBjYXNlIDM6XG4gICAgICBlbWl0VHdvKGhhbmRsZXIsIGlzRm4sIHRoaXMsIGFyZ3VtZW50c1sxXSwgYXJndW1lbnRzWzJdKTtcbiAgICAgIGJyZWFrO1xuICAgIGNhc2UgNDpcbiAgICAgIGVtaXRUaHJlZShoYW5kbGVyLCBpc0ZuLCB0aGlzLCBhcmd1bWVudHNbMV0sIGFyZ3VtZW50c1syXSwgYXJndW1lbnRzWzNdKTtcbiAgICAgIGJyZWFrO1xuICAgICAgLy8gc2xvd2VyXG4gICAgZGVmYXVsdDpcbiAgICAgIGFyZ3MgPSBuZXcgQXJyYXkobGVuIC0gMSk7XG4gICAgICBmb3IgKGkgPSAxOyBpIDwgbGVuOyBpKyspXG4gICAgICAgIGFyZ3NbaSAtIDFdID0gYXJndW1lbnRzW2ldO1xuICAgICAgZW1pdE1hbnkoaGFuZGxlciwgaXNGbiwgdGhpcywgYXJncyk7XG4gIH1cblxuICByZXR1cm4gdHJ1ZTtcbn07XG5cbmZ1bmN0aW9uIF9hZGRMaXN0ZW5lcih0YXJnZXQsIHR5cGUsIGxpc3RlbmVyLCBwcmVwZW5kKSB7XG4gIHZhciBtO1xuICB2YXIgZXZlbnRzO1xuICB2YXIgZXhpc3Rpbmc7XG5cbiAgaWYgKHR5cGVvZiBsaXN0ZW5lciAhPT0gJ2Z1bmN0aW9uJylcbiAgICB0aHJvdyBuZXcgVHlwZUVycm9yKCdcImxpc3RlbmVyXCIgYXJndW1lbnQgbXVzdCBiZSBhIGZ1bmN0aW9uJyk7XG5cbiAgZXZlbnRzID0gdGFyZ2V0Ll9ldmVudHM7XG4gIGlmICghZXZlbnRzKSB7XG4gICAgZXZlbnRzID0gdGFyZ2V0Ll9ldmVudHMgPSBvYmplY3RDcmVhdGUobnVsbCk7XG4gICAgdGFyZ2V0Ll9ldmVudHNDb3VudCA9IDA7XG4gIH0gZWxzZSB7XG4gICAgLy8gVG8gYXZvaWQgcmVjdXJzaW9uIGluIHRoZSBjYXNlIHRoYXQgdHlwZSA9PT0gXCJuZXdMaXN0ZW5lclwiISBCZWZvcmVcbiAgICAvLyBhZGRpbmcgaXQgdG8gdGhlIGxpc3RlbmVycywgZmlyc3QgZW1pdCBcIm5ld0xpc3RlbmVyXCIuXG4gICAgaWYgKGV2ZW50cy5uZXdMaXN0ZW5lcikge1xuICAgICAgdGFyZ2V0LmVtaXQoJ25ld0xpc3RlbmVyJywgdHlwZSxcbiAgICAgICAgICBsaXN0ZW5lci5saXN0ZW5lciA/IGxpc3RlbmVyLmxpc3RlbmVyIDogbGlzdGVuZXIpO1xuXG4gICAgICAvLyBSZS1hc3NpZ24gYGV2ZW50c2AgYmVjYXVzZSBhIG5ld0xpc3RlbmVyIGhhbmRsZXIgY291bGQgaGF2ZSBjYXVzZWQgdGhlXG4gICAgICAvLyB0aGlzLl9ldmVudHMgdG8gYmUgYXNzaWduZWQgdG8gYSBuZXcgb2JqZWN0XG4gICAgICBldmVudHMgPSB0YXJnZXQuX2V2ZW50cztcbiAgICB9XG4gICAgZXhpc3RpbmcgPSBldmVudHNbdHlwZV07XG4gIH1cblxuICBpZiAoIWV4aXN0aW5nKSB7XG4gICAgLy8gT3B0aW1pemUgdGhlIGNhc2Ugb2Ygb25lIGxpc3RlbmVyLiBEb24ndCBuZWVkIHRoZSBleHRyYSBhcnJheSBvYmplY3QuXG4gICAgZXhpc3RpbmcgPSBldmVudHNbdHlwZV0gPSBsaXN0ZW5lcjtcbiAgICArK3RhcmdldC5fZXZlbnRzQ291bnQ7XG4gIH0gZWxzZSB7XG4gICAgaWYgKHR5cGVvZiBleGlzdGluZyA9PT0gJ2Z1bmN0aW9uJykge1xuICAgICAgLy8gQWRkaW5nIHRoZSBzZWNvbmQgZWxlbWVudCwgbmVlZCB0byBjaGFuZ2UgdG8gYXJyYXkuXG4gICAgICBleGlzdGluZyA9IGV2ZW50c1t0eXBlXSA9XG4gICAgICAgICAgcHJlcGVuZCA/IFtsaXN0ZW5lciwgZXhpc3RpbmddIDogW2V4aXN0aW5nLCBsaXN0ZW5lcl07XG4gICAgfSBlbHNlIHtcbiAgICAgIC8vIElmIHdlJ3ZlIGFscmVhZHkgZ290IGFuIGFycmF5LCBqdXN0IGFwcGVuZC5cbiAgICAgIGlmIChwcmVwZW5kKSB7XG4gICAgICAgIGV4aXN0aW5nLnVuc2hpZnQobGlzdGVuZXIpO1xuICAgICAgfSBlbHNlIHtcbiAgICAgICAgZXhpc3RpbmcucHVzaChsaXN0ZW5lcik7XG4gICAgICB9XG4gICAgfVxuXG4gICAgLy8gQ2hlY2sgZm9yIGxpc3RlbmVyIGxlYWtcbiAgICBpZiAoIWV4aXN0aW5nLndhcm5lZCkge1xuICAgICAgbSA9ICRnZXRNYXhMaXN0ZW5lcnModGFyZ2V0KTtcbiAgICAgIGlmIChtICYmIG0gPiAwICYmIGV4aXN0aW5nLmxlbmd0aCA+IG0pIHtcbiAgICAgICAgZXhpc3Rpbmcud2FybmVkID0gdHJ1ZTtcbiAgICAgICAgdmFyIHcgPSBuZXcgRXJyb3IoJ1Bvc3NpYmxlIEV2ZW50RW1pdHRlciBtZW1vcnkgbGVhayBkZXRlY3RlZC4gJyArXG4gICAgICAgICAgICBleGlzdGluZy5sZW5ndGggKyAnIFwiJyArIFN0cmluZyh0eXBlKSArICdcIiBsaXN0ZW5lcnMgJyArXG4gICAgICAgICAgICAnYWRkZWQuIFVzZSBlbWl0dGVyLnNldE1heExpc3RlbmVycygpIHRvICcgK1xuICAgICAgICAgICAgJ2luY3JlYXNlIGxpbWl0LicpO1xuICAgICAgICB3Lm5hbWUgPSAnTWF4TGlzdGVuZXJzRXhjZWVkZWRXYXJuaW5nJztcbiAgICAgICAgdy5lbWl0dGVyID0gdGFyZ2V0O1xuICAgICAgICB3LnR5cGUgPSB0eXBlO1xuICAgICAgICB3LmNvdW50ID0gZXhpc3RpbmcubGVuZ3RoO1xuICAgICAgICBpZiAodHlwZW9mIGNvbnNvbGUgPT09ICdvYmplY3QnICYmIGNvbnNvbGUud2Fybikge1xuICAgICAgICAgIGNvbnNvbGUud2FybignJXM6ICVzJywgdy5uYW1lLCB3Lm1lc3NhZ2UpO1xuICAgICAgICB9XG4gICAgICB9XG4gICAgfVxuICB9XG5cbiAgcmV0dXJuIHRhcmdldDtcbn1cblxuRXZlbnRFbWl0dGVyLnByb3RvdHlwZS5hZGRMaXN0ZW5lciA9IGZ1bmN0aW9uIGFkZExpc3RlbmVyKHR5cGUsIGxpc3RlbmVyKSB7XG4gIHJldHVybiBfYWRkTGlzdGVuZXIodGhpcywgdHlwZSwgbGlzdGVuZXIsIGZhbHNlKTtcbn07XG5cbkV2ZW50RW1pdHRlci5wcm90b3R5cGUub24gPSBFdmVudEVtaXR0ZXIucHJvdG90eXBlLmFkZExpc3RlbmVyO1xuXG5FdmVudEVtaXR0ZXIucHJvdG90eXBlLnByZXBlbmRMaXN0ZW5lciA9XG4gICAgZnVuY3Rpb24gcHJlcGVuZExpc3RlbmVyKHR5cGUsIGxpc3RlbmVyKSB7XG4gICAgICByZXR1cm4gX2FkZExpc3RlbmVyKHRoaXMsIHR5cGUsIGxpc3RlbmVyLCB0cnVlKTtcbiAgICB9O1xuXG5mdW5jdGlvbiBvbmNlV3JhcHBlcigpIHtcbiAgaWYgKCF0aGlzLmZpcmVkKSB7XG4gICAgdGhpcy50YXJnZXQucmVtb3ZlTGlzdGVuZXIodGhpcy50eXBlLCB0aGlzLndyYXBGbik7XG4gICAgdGhpcy5maXJlZCA9IHRydWU7XG4gICAgc3dpdGNoIChhcmd1bWVudHMubGVuZ3RoKSB7XG4gICAgICBjYXNlIDA6XG4gICAgICAgIHJldHVybiB0aGlzLmxpc3RlbmVyLmNhbGwodGhpcy50YXJnZXQpO1xuICAgICAgY2FzZSAxOlxuICAgICAgICByZXR1cm4gdGhpcy5saXN0ZW5lci5jYWxsKHRoaXMudGFyZ2V0LCBhcmd1bWVudHNbMF0pO1xuICAgICAgY2FzZSAyOlxuICAgICAgICByZXR1cm4gdGhpcy5saXN0ZW5lci5jYWxsKHRoaXMudGFyZ2V0LCBhcmd1bWVudHNbMF0sIGFyZ3VtZW50c1sxXSk7XG4gICAgICBjYXNlIDM6XG4gICAgICAgIHJldHVybiB0aGlzLmxpc3RlbmVyLmNhbGwodGhpcy50YXJnZXQsIGFyZ3VtZW50c1swXSwgYXJndW1lbnRzWzFdLFxuICAgICAgICAgICAgYXJndW1lbnRzWzJdKTtcbiAgICAgIGRlZmF1bHQ6XG4gICAgICAgIHZhciBhcmdzID0gbmV3IEFycmF5KGFyZ3VtZW50cy5sZW5ndGgpO1xuICAgICAgICBmb3IgKHZhciBpID0gMDsgaSA8IGFyZ3MubGVuZ3RoOyArK2kpXG4gICAgICAgICAgYXJnc1tpXSA9IGFyZ3VtZW50c1tpXTtcbiAgICAgICAgdGhpcy5saXN0ZW5lci5hcHBseSh0aGlzLnRhcmdldCwgYXJncyk7XG4gICAgfVxuICB9XG59XG5cbmZ1bmN0aW9uIF9vbmNlV3JhcCh0YXJnZXQsIHR5cGUsIGxpc3RlbmVyKSB7XG4gIHZhciBzdGF0ZSA9IHsgZmlyZWQ6IGZhbHNlLCB3cmFwRm46IHVuZGVmaW5lZCwgdGFyZ2V0OiB0YXJnZXQsIHR5cGU6IHR5cGUsIGxpc3RlbmVyOiBsaXN0ZW5lciB9O1xuICB2YXIgd3JhcHBlZCA9IGJpbmQuY2FsbChvbmNlV3JhcHBlciwgc3RhdGUpO1xuICB3cmFwcGVkLmxpc3RlbmVyID0gbGlzdGVuZXI7XG4gIHN0YXRlLndyYXBGbiA9IHdyYXBwZWQ7XG4gIHJldHVybiB3cmFwcGVkO1xufVxuXG5FdmVudEVtaXR0ZXIucHJvdG90eXBlLm9uY2UgPSBmdW5jdGlvbiBvbmNlKHR5cGUsIGxpc3RlbmVyKSB7XG4gIGlmICh0eXBlb2YgbGlzdGVuZXIgIT09ICdmdW5jdGlvbicpXG4gICAgdGhyb3cgbmV3IFR5cGVFcnJvcignXCJsaXN0ZW5lclwiIGFyZ3VtZW50IG11c3QgYmUgYSBmdW5jdGlvbicpO1xuICB0aGlzLm9uKHR5cGUsIF9vbmNlV3JhcCh0aGlzLCB0eXBlLCBsaXN0ZW5lcikpO1xuICByZXR1cm4gdGhpcztcbn07XG5cbkV2ZW50RW1pdHRlci5wcm90b3R5cGUucHJlcGVuZE9uY2VMaXN0ZW5lciA9XG4gICAgZnVuY3Rpb24gcHJlcGVuZE9uY2VMaXN0ZW5lcih0eXBlLCBsaXN0ZW5lcikge1xuICAgICAgaWYgKHR5cGVvZiBsaXN0ZW5lciAhPT0gJ2Z1bmN0aW9uJylcbiAgICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcignXCJsaXN0ZW5lclwiIGFyZ3VtZW50IG11c3QgYmUgYSBmdW5jdGlvbicpO1xuICAgICAgdGhpcy5wcmVwZW5kTGlzdGVuZXIodHlwZSwgX29uY2VXcmFwKHRoaXMsIHR5cGUsIGxpc3RlbmVyKSk7XG4gICAgICByZXR1cm4gdGhpcztcbiAgICB9O1xuXG4vLyBFbWl0cyBhICdyZW1vdmVMaXN0ZW5lcicgZXZlbnQgaWYgYW5kIG9ubHkgaWYgdGhlIGxpc3RlbmVyIHdhcyByZW1vdmVkLlxuRXZlbnRFbWl0dGVyLnByb3RvdHlwZS5yZW1vdmVMaXN0ZW5lciA9XG4gICAgZnVuY3Rpb24gcmVtb3ZlTGlzdGVuZXIodHlwZSwgbGlzdGVuZXIpIHtcbiAgICAgIHZhciBsaXN0LCBldmVudHMsIHBvc2l0aW9uLCBpLCBvcmlnaW5hbExpc3RlbmVyO1xuXG4gICAgICBpZiAodHlwZW9mIGxpc3RlbmVyICE9PSAnZnVuY3Rpb24nKVxuICAgICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKCdcImxpc3RlbmVyXCIgYXJndW1lbnQgbXVzdCBiZSBhIGZ1bmN0aW9uJyk7XG5cbiAgICAgIGV2ZW50cyA9IHRoaXMuX2V2ZW50cztcbiAgICAgIGlmICghZXZlbnRzKVxuICAgICAgICByZXR1cm4gdGhpcztcblxuICAgICAgbGlzdCA9IGV2ZW50c1t0eXBlXTtcbiAgICAgIGlmICghbGlzdClcbiAgICAgICAgcmV0dXJuIHRoaXM7XG5cbiAgICAgIGlmIChsaXN0ID09PSBsaXN0ZW5lciB8fCBsaXN0Lmxpc3RlbmVyID09PSBsaXN0ZW5lcikge1xuICAgICAgICBpZiAoLS10aGlzLl9ldmVudHNDb3VudCA9PT0gMClcbiAgICAgICAgICB0aGlzLl9ldmVudHMgPSBvYmplY3RDcmVhdGUobnVsbCk7XG4gICAgICAgIGVsc2Uge1xuICAgICAgICAgIGRlbGV0ZSBldmVudHNbdHlwZV07XG4gICAgICAgICAgaWYgKGV2ZW50cy5yZW1vdmVMaXN0ZW5lcilcbiAgICAgICAgICAgIHRoaXMuZW1pdCgncmVtb3ZlTGlzdGVuZXInLCB0eXBlLCBsaXN0Lmxpc3RlbmVyIHx8IGxpc3RlbmVyKTtcbiAgICAgICAgfVxuICAgICAgfSBlbHNlIGlmICh0eXBlb2YgbGlzdCAhPT0gJ2Z1bmN0aW9uJykge1xuICAgICAgICBwb3NpdGlvbiA9IC0xO1xuXG4gICAgICAgIGZvciAoaSA9IGxpc3QubGVuZ3RoIC0gMTsgaSA+PSAwOyBpLS0pIHtcbiAgICAgICAgICBpZiAobGlzdFtpXSA9PT0gbGlzdGVuZXIgfHwgbGlzdFtpXS5saXN0ZW5lciA9PT0gbGlzdGVuZXIpIHtcbiAgICAgICAgICAgIG9yaWdpbmFsTGlzdGVuZXIgPSBsaXN0W2ldLmxpc3RlbmVyO1xuICAgICAgICAgICAgcG9zaXRpb24gPSBpO1xuICAgICAgICAgICAgYnJlYWs7XG4gICAgICAgICAgfVxuICAgICAgICB9XG5cbiAgICAgICAgaWYgKHBvc2l0aW9uIDwgMClcbiAgICAgICAgICByZXR1cm4gdGhpcztcblxuICAgICAgICBpZiAocG9zaXRpb24gPT09IDApXG4gICAgICAgICAgbGlzdC5zaGlmdCgpO1xuICAgICAgICBlbHNlXG4gICAgICAgICAgc3BsaWNlT25lKGxpc3QsIHBvc2l0aW9uKTtcblxuICAgICAgICBpZiAobGlzdC5sZW5ndGggPT09IDEpXG4gICAgICAgICAgZXZlbnRzW3R5cGVdID0gbGlzdFswXTtcblxuICAgICAgICBpZiAoZXZlbnRzLnJlbW92ZUxpc3RlbmVyKVxuICAgICAgICAgIHRoaXMuZW1pdCgncmVtb3ZlTGlzdGVuZXInLCB0eXBlLCBvcmlnaW5hbExpc3RlbmVyIHx8IGxpc3RlbmVyKTtcbiAgICAgIH1cblxuICAgICAgcmV0dXJuIHRoaXM7XG4gICAgfTtcblxuRXZlbnRFbWl0dGVyLnByb3RvdHlwZS5yZW1vdmVBbGxMaXN0ZW5lcnMgPVxuICAgIGZ1bmN0aW9uIHJlbW92ZUFsbExpc3RlbmVycyh0eXBlKSB7XG4gICAgICB2YXIgbGlzdGVuZXJzLCBldmVudHMsIGk7XG5cbiAgICAgIGV2ZW50cyA9IHRoaXMuX2V2ZW50cztcbiAgICAgIGlmICghZXZlbnRzKVxuICAgICAgICByZXR1cm4gdGhpcztcblxuICAgICAgLy8gbm90IGxpc3RlbmluZyBmb3IgcmVtb3ZlTGlzdGVuZXIsIG5vIG5lZWQgdG8gZW1pdFxuICAgICAgaWYgKCFldmVudHMucmVtb3ZlTGlzdGVuZXIpIHtcbiAgICAgICAgaWYgKGFyZ3VtZW50cy5sZW5ndGggPT09IDApIHtcbiAgICAgICAgICB0aGlzLl9ldmVudHMgPSBvYmplY3RDcmVhdGUobnVsbCk7XG4gICAgICAgICAgdGhpcy5fZXZlbnRzQ291bnQgPSAwO1xuICAgICAgICB9IGVsc2UgaWYgKGV2ZW50c1t0eXBlXSkge1xuICAgICAgICAgIGlmICgtLXRoaXMuX2V2ZW50c0NvdW50ID09PSAwKVxuICAgICAgICAgICAgdGhpcy5fZXZlbnRzID0gb2JqZWN0Q3JlYXRlKG51bGwpO1xuICAgICAgICAgIGVsc2VcbiAgICAgICAgICAgIGRlbGV0ZSBldmVudHNbdHlwZV07XG4gICAgICAgIH1cbiAgICAgICAgcmV0dXJuIHRoaXM7XG4gICAgICB9XG5cbiAgICAgIC8vIGVtaXQgcmVtb3ZlTGlzdGVuZXIgZm9yIGFsbCBsaXN0ZW5lcnMgb24gYWxsIGV2ZW50c1xuICAgICAgaWYgKGFyZ3VtZW50cy5sZW5ndGggPT09IDApIHtcbiAgICAgICAgdmFyIGtleXMgPSBvYmplY3RLZXlzKGV2ZW50cyk7XG4gICAgICAgIHZhciBrZXk7XG4gICAgICAgIGZvciAoaSA9IDA7IGkgPCBrZXlzLmxlbmd0aDsgKytpKSB7XG4gICAgICAgICAga2V5ID0ga2V5c1tpXTtcbiAgICAgICAgICBpZiAoa2V5ID09PSAncmVtb3ZlTGlzdGVuZXInKSBjb250aW51ZTtcbiAgICAgICAgICB0aGlzLnJlbW92ZUFsbExpc3RlbmVycyhrZXkpO1xuICAgICAgICB9XG4gICAgICAgIHRoaXMucmVtb3ZlQWxsTGlzdGVuZXJzKCdyZW1vdmVMaXN0ZW5lcicpO1xuICAgICAgICB0aGlzLl9ldmVudHMgPSBvYmplY3RDcmVhdGUobnVsbCk7XG4gICAgICAgIHRoaXMuX2V2ZW50c0NvdW50ID0gMDtcbiAgICAgICAgcmV0dXJuIHRoaXM7XG4gICAgICB9XG5cbiAgICAgIGxpc3RlbmVycyA9IGV2ZW50c1t0eXBlXTtcblxuICAgICAgaWYgKHR5cGVvZiBsaXN0ZW5lcnMgPT09ICdmdW5jdGlvbicpIHtcbiAgICAgICAgdGhpcy5yZW1vdmVMaXN0ZW5lcih0eXBlLCBsaXN0ZW5lcnMpO1xuICAgICAgfSBlbHNlIGlmIChsaXN0ZW5lcnMpIHtcbiAgICAgICAgLy8gTElGTyBvcmRlclxuICAgICAgICBmb3IgKGkgPSBsaXN0ZW5lcnMubGVuZ3RoIC0gMTsgaSA+PSAwOyBpLS0pIHtcbiAgICAgICAgICB0aGlzLnJlbW92ZUxpc3RlbmVyKHR5cGUsIGxpc3RlbmVyc1tpXSk7XG4gICAgICAgIH1cbiAgICAgIH1cblxuICAgICAgcmV0dXJuIHRoaXM7XG4gICAgfTtcblxuZnVuY3Rpb24gX2xpc3RlbmVycyh0YXJnZXQsIHR5cGUsIHVud3JhcCkge1xuICB2YXIgZXZlbnRzID0gdGFyZ2V0Ll9ldmVudHM7XG5cbiAgaWYgKCFldmVudHMpXG4gICAgcmV0dXJuIFtdO1xuXG4gIHZhciBldmxpc3RlbmVyID0gZXZlbnRzW3R5cGVdO1xuICBpZiAoIWV2bGlzdGVuZXIpXG4gICAgcmV0dXJuIFtdO1xuXG4gIGlmICh0eXBlb2YgZXZsaXN0ZW5lciA9PT0gJ2Z1bmN0aW9uJylcbiAgICByZXR1cm4gdW53cmFwID8gW2V2bGlzdGVuZXIubGlzdGVuZXIgfHwgZXZsaXN0ZW5lcl0gOiBbZXZsaXN0ZW5lcl07XG5cbiAgcmV0dXJuIHVud3JhcCA/IHVud3JhcExpc3RlbmVycyhldmxpc3RlbmVyKSA6IGFycmF5Q2xvbmUoZXZsaXN0ZW5lciwgZXZsaXN0ZW5lci5sZW5ndGgpO1xufVxuXG5FdmVudEVtaXR0ZXIucHJvdG90eXBlLmxpc3RlbmVycyA9IGZ1bmN0aW9uIGxpc3RlbmVycyh0eXBlKSB7XG4gIHJldHVybiBfbGlzdGVuZXJzKHRoaXMsIHR5cGUsIHRydWUpO1xufTtcblxuRXZlbnRFbWl0dGVyLnByb3RvdHlwZS5yYXdMaXN0ZW5lcnMgPSBmdW5jdGlvbiByYXdMaXN0ZW5lcnModHlwZSkge1xuICByZXR1cm4gX2xpc3RlbmVycyh0aGlzLCB0eXBlLCBmYWxzZSk7XG59O1xuXG5FdmVudEVtaXR0ZXIubGlzdGVuZXJDb3VudCA9IGZ1bmN0aW9uKGVtaXR0ZXIsIHR5cGUpIHtcbiAgaWYgKHR5cGVvZiBlbWl0dGVyLmxpc3RlbmVyQ291bnQgPT09ICdmdW5jdGlvbicpIHtcbiAgICByZXR1cm4gZW1pdHRlci5saXN0ZW5lckNvdW50KHR5cGUpO1xuICB9IGVsc2Uge1xuICAgIHJldHVybiBsaXN0ZW5lckNvdW50LmNhbGwoZW1pdHRlciwgdHlwZSk7XG4gIH1cbn07XG5cbkV2ZW50RW1pdHRlci5wcm90b3R5cGUubGlzdGVuZXJDb3VudCA9IGxpc3RlbmVyQ291bnQ7XG5mdW5jdGlvbiBsaXN0ZW5lckNvdW50KHR5cGUpIHtcbiAgdmFyIGV2ZW50cyA9IHRoaXMuX2V2ZW50cztcblxuICBpZiAoZXZlbnRzKSB7XG4gICAgdmFyIGV2bGlzdGVuZXIgPSBldmVudHNbdHlwZV07XG5cbiAgICBpZiAodHlwZW9mIGV2bGlzdGVuZXIgPT09ICdmdW5jdGlvbicpIHtcbiAgICAgIHJldHVybiAxO1xuICAgIH0gZWxzZSBpZiAoZXZsaXN0ZW5lcikge1xuICAgICAgcmV0dXJuIGV2bGlzdGVuZXIubGVuZ3RoO1xuICAgIH1cbiAgfVxuXG4gIHJldHVybiAwO1xufVxuXG5FdmVudEVtaXR0ZXIucHJvdG90eXBlLmV2ZW50TmFtZXMgPSBmdW5jdGlvbiBldmVudE5hbWVzKCkge1xuICByZXR1cm4gdGhpcy5fZXZlbnRzQ291bnQgPiAwID8gUmVmbGVjdC5vd25LZXlzKHRoaXMuX2V2ZW50cykgOiBbXTtcbn07XG5cbi8vIEFib3V0IDEuNXggZmFzdGVyIHRoYW4gdGhlIHR3by1hcmcgdmVyc2lvbiBvZiBBcnJheSNzcGxpY2UoKS5cbmZ1bmN0aW9uIHNwbGljZU9uZShsaXN0LCBpbmRleCkge1xuICBmb3IgKHZhciBpID0gaW5kZXgsIGsgPSBpICsgMSwgbiA9IGxpc3QubGVuZ3RoOyBrIDwgbjsgaSArPSAxLCBrICs9IDEpXG4gICAgbGlzdFtpXSA9IGxpc3Rba107XG4gIGxpc3QucG9wKCk7XG59XG5cbmZ1bmN0aW9uIGFycmF5Q2xvbmUoYXJyLCBuKSB7XG4gIHZhciBjb3B5ID0gbmV3IEFycmF5KG4pO1xuICBmb3IgKHZhciBpID0gMDsgaSA8IG47ICsraSlcbiAgICBjb3B5W2ldID0gYXJyW2ldO1xuICByZXR1cm4gY29weTtcbn1cblxuZnVuY3Rpb24gdW53cmFwTGlzdGVuZXJzKGFycikge1xuICB2YXIgcmV0ID0gbmV3IEFycmF5KGFyci5sZW5ndGgpO1xuICBmb3IgKHZhciBpID0gMDsgaSA8IHJldC5sZW5ndGg7ICsraSkge1xuICAgIHJldFtpXSA9IGFycltpXS5saXN0ZW5lciB8fCBhcnJbaV07XG4gIH1cbiAgcmV0dXJuIHJldDtcbn1cblxuZnVuY3Rpb24gb2JqZWN0Q3JlYXRlUG9seWZpbGwocHJvdG8pIHtcbiAgdmFyIEYgPSBmdW5jdGlvbigpIHt9O1xuICBGLnByb3RvdHlwZSA9IHByb3RvO1xuICByZXR1cm4gbmV3IEY7XG59XG5mdW5jdGlvbiBvYmplY3RLZXlzUG9seWZpbGwob2JqKSB7XG4gIHZhciBrZXlzID0gW107XG4gIGZvciAodmFyIGsgaW4gb2JqKSBpZiAoT2JqZWN0LnByb3RvdHlwZS5oYXNPd25Qcm9wZXJ0eS5jYWxsKG9iaiwgaykpIHtcbiAgICBrZXlzLnB1c2goayk7XG4gIH1cbiAgcmV0dXJuIGs7XG59XG5mdW5jdGlvbiBmdW5jdGlvbkJpbmRQb2x5ZmlsbChjb250ZXh0KSB7XG4gIHZhciBmbiA9IHRoaXM7XG4gIHJldHVybiBmdW5jdGlvbiAoKSB7XG4gICAgcmV0dXJuIGZuLmFwcGx5KGNvbnRleHQsIGFyZ3VtZW50cyk7XG4gIH07XG59XG4iLCJpZiAodHlwZW9mIE9iamVjdC5jcmVhdGUgPT09ICdmdW5jdGlvbicpIHtcbiAgLy8gaW1wbGVtZW50YXRpb24gZnJvbSBzdGFuZGFyZCBub2RlLmpzICd1dGlsJyBtb2R1bGVcbiAgbW9kdWxlLmV4cG9ydHMgPSBmdW5jdGlvbiBpbmhlcml0cyhjdG9yLCBzdXBlckN0b3IpIHtcbiAgICBpZiAoc3VwZXJDdG9yKSB7XG4gICAgICBjdG9yLnN1cGVyXyA9IHN1cGVyQ3RvclxuICAgICAgY3Rvci5wcm90b3R5cGUgPSBPYmplY3QuY3JlYXRlKHN1cGVyQ3Rvci5wcm90b3R5cGUsIHtcbiAgICAgICAgY29uc3RydWN0b3I6IHtcbiAgICAgICAgICB2YWx1ZTogY3RvcixcbiAgICAgICAgICBlbnVtZXJhYmxlOiBmYWxzZSxcbiAgICAgICAgICB3cml0YWJsZTogdHJ1ZSxcbiAgICAgICAgICBjb25maWd1cmFibGU6IHRydWVcbiAgICAgICAgfVxuICAgICAgfSlcbiAgICB9XG4gIH07XG59IGVsc2Uge1xuICAvLyBvbGQgc2Nob29sIHNoaW0gZm9yIG9sZCBicm93c2Vyc1xuICBtb2R1bGUuZXhwb3J0cyA9IGZ1bmN0aW9uIGluaGVyaXRzKGN0b3IsIHN1cGVyQ3Rvcikge1xuICAgIGlmIChzdXBlckN0b3IpIHtcbiAgICAgIGN0b3Iuc3VwZXJfID0gc3VwZXJDdG9yXG4gICAgICB2YXIgVGVtcEN0b3IgPSBmdW5jdGlvbiAoKSB7fVxuICAgICAgVGVtcEN0b3IucHJvdG90eXBlID0gc3VwZXJDdG9yLnByb3RvdHlwZVxuICAgICAgY3Rvci5wcm90b3R5cGUgPSBuZXcgVGVtcEN0b3IoKVxuICAgICAgY3Rvci5wcm90b3R5cGUuY29uc3RydWN0b3IgPSBjdG9yXG4gICAgfVxuICB9XG59XG4iXX0=
