let MoveOpInotifyTaskManager = (function () {
    'use strict';

    class MoveOpInotifyTaskManager
    {
        constructor(time=200)
        {
            this._taskList = {};
            this.time = time;
        }
        create(funct, event)
        {
            this._taskList[`${event["cookie"]}`] = setTimeout(funct, this.time, event);
        }
        cancel(id)
        {
            clearTimeout(this._taskList[`${id}`]);
            delete this._taskList[`${id}`];
        }
        cancelAll()
        {
            const cookies = Object.keys(this._taskList);
            for (const cookie of cookies) {
                this.cancel(cookie)
            }
        }
    }
    return MoveOpInotifyTaskManager;
}());