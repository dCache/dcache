let dCacheEventSource = (function () {
    'use strict';
    //Adapted Jake Archibald code - https://github.com/jakearchibald/sse-fetcher
    class dCacheEventSource extends EventTarget
    {
        constructor(url, init)
        {
            super();
            this._chunk = '';
            this._data = '';
            this._eventType = '';
            this._auth = init.auth ? init.auth : '';
            this._lastId = init.lastID ? init.lastID : '';
            this._lastIdBuffer = '';
            this._aborted = false;
            this._url = url;
            this._withCredentials = !!init.withCredentials;
            this._reconnectionDelay = init.reconnectionDelay || 2000;
            this._open();
        }

        _error(error)
        {
            this.dispatchEvent(new CustomEvent('error', {detail : {message: error}}));
        }

        async _readLine(reader)
        {
            while (true) {
                const re = /\r?\n/.exec(this._chunk);
                if (re) {
                    const line = this._chunk.slice(0, re.index);
                    this._chunk = this._chunk.slice(re.index + re[0].length);
                    return line;
                }
                const {done, value} = await reader.read();
                if (done)
                    throw Error('Connection terminated');
                const textValue = this._decoder.decode(value, {stream: true});
                this._chunk += textValue;
            }
        }

        _dispatch()
        {
            this._lastId = this._lastIdBuffer;
            if (this._data === '') {
                this._eventType = '';
                return;
            }
            if (this._data.slice(-1) === '\n')
                this._data = this._data.slice(0, -1);

            this.dispatchEvent(
                new CustomEvent(this._eventType ? this._eventType : 'message', {detail : {data: this._data}}));
            this._data = '';
            this._eventType = '';
        }

        _process(field, value)
        {
            switch (field) {
                case 'event':
                    this._eventType = value;
                    break;
                case 'data':
                    this._data += value + '\n';
                    break;
                case 'id':
                    this._lastIdBuffer = value;
                    break;
                case 'retry':
                    const num = Number(value);
                    if (num)
                        this._reconnectionDelay = num;
                    break;
            }
        }

        _open()
        {
            this._connect().catch(e => {
                this._error(e);
                setTimeout(this._open(), this._reconnectionDelay)
            })
        }

        async _connect()
        {
            const headers = new Headers({
                'Accept': 'text/event-stream',
                "content-type": "application/json",
                "suppress-www-authenticate": "Suppress"
            });
            if (this._lastId) {
                headers.set('Last-Event-ID', this._lastId);
            }
            if (this._auth) {
                headers.set('Authorization', this._auth);
            }
            const response = await fetch(this._url, {
                headers,
                credentials: this._withCredentials ? 'include' : 'same-origin',
                cache: 'no-store',
            });

            if (response.status !== 200) {
                this._error(Error(`Bad status - ${response.status}`));
                await response.body.cancel();
                return;
            }
            const streamReader = response.body.getReader();
            this._decoder = new TextDecoder();
            this._chunk = '';
            this._eventType = '';
            this._data = '';
            this._lastIdBuffer = '';

            while (true) {
                if (this._aborted) {
                    await streamReader.cancel();
                    return;
                }
                const decodedLine = await this._readLine(streamReader);
                if (decodedLine === '') {
                    this._dispatch();
                } else if (decodedLine[0] === ':') {
                } else if (decodedLine.includes(':')) {
                    const index = decodedLine.indexOf(':');
                    const field = decodedLine.slice(0, index);
                    let value = decodedLine.slice(index + 1);
                    if (value[0] === ' ')
                        value = value.slice(1);
                    this._process(field, value);
                } else {
                    this._process(decodedLine, '');
                }
            }
        }

        close() {
            this._aborted = true;
            this._error(new DOMException('', 'AbortError'));
        }
    }
    return dCacheEventSource;
}());