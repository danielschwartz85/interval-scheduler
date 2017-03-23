class Common {

    static get env() {
        return process.env.ENVIRONMENT || 'local';
    }

    static isLocal() {
        return ('local' === process.env.ENVIRONMENT) || !process.env.ENVIRONMENT;
    }

    // Overwrites slaveObj's values with masterObj's and adds masterObj's if non existent in slaveObj
    static mergeObjects(slaveObj ={}, masterObj = {}) {
        var obj3 = {};
        for (var attrname in slaveObj) { obj3[attrname] = slaveObj[attrname]; }
        for (var attrname in masterObj) { obj3[attrname] = masterObj[attrname]; }
        return obj3;
    }

    static assertKeys(obj, ...expected) {
        if (!obj) {
            throw new Error(`missing parameters object`);
        }
        expected.forEach(k => {
          if (obj[k] === undefined) {
              throw new Error(`missing '${k}' parameter`);
          }
        });
        return this;
    }

    // From: http://stackoverflow.com/questions/6274339/how-can-i-shuffle-an-array
    static shuffle(array) {
        let counter = array.length;

        // While there are elements in the array
        while (counter > 0) {
            // Pick a random index
            let index = Math.floor(Math.random() * counter);

            // Decrease counter by 1
            counter--;

            // And swap the last element with it
            let temp = array[counter];
            array[counter] = array[index];
            array[index] = temp;
        }

        return array;
    }

    static objGet(obj, path) {
        if ('' === path) {
            return obj;
        }
        let result = obj;
        path.split('/').forEach(key => {
            result = result && result[key];
        });
        return result;
    }
}

module.exports = Common;