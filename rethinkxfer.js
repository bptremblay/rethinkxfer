/**
 * @module rethinkxfer
 * @requires rethinkdb
 * @requires fs
 * @requires path
 * @requires wrench
 */
var r = require('rethinkdb');
var _fs = require('fs');
var _path = require('path');
var _wrench = require('wrench');
var csvConverter = require('json-2-csv');
var csvConverter2 = require('json-csv');
var Promise = require('bluebird');
//Stub out a standard config object.
var config = {};
var _rdbConn = null;
var JSON_FOLDER = 'seed_data';
//List of file/table names used when reading from JSON. TODO: make this dynamic
var tableFileList = [];
//DB to copy from
var source = '';
//DB to write to
var destination = '';
//Set this to true or false to either serialize or deserialize data.
var DUMP_TO_FILES = false;

var JSON_FILE_SUFFIX = '';
//Experimental. Handle very large tables. WIP.
var MAX_TABLE_LENGTH = 16384;

//Halt operations?
var halted = false;

//Load the configuration.
var configText = readFile('rethinkxfer-config.json');
if (configText.length) {
    config = JSON.parse(configText);
    source = config.rethinkdbCopy.db;
    destination = config.rethinkdbCreate.db;
    DUMP_TO_FILES = config.DUMP_TO_FILES;
    //print process.argv
    process.argv.forEach(function(val, index, array) {
        //console.log(index + ': ' + val);
        if (val === '-backup') {
            DUMP_TO_FILES = true;
        } else if (val === '-restore') {
            DUMP_TO_FILES = false;
        }
        if (DUMP_TO_FILES) {
            config.rethinkdb = config.rethinkdbCopy;
        } else {
            config.rethinkdb = config.rethinkdbCreate;
        }
        if (val.indexOf('=') !== -1) {
            var splitter = val.trim().split('=');
            var key = splitter[0];
            var value = splitter[1];
            console.log('Setting config.rethinkdb.' + key + ' to "' + value + '".');
            config.rethinkdb[key] = value;
        }
        if (val === '-help' || val === '-h' || val === '-?' || val === '?') {
            console.log('rethinkxfer [-backup | -restore] [key]=[value] where values are assigned to config[key] properties.');
            halted = true;
        }
    });
} else {
    console.log('Could not find file rethinkxfer-config.json. I hope you pass in enough args!');
    config.rethinkdb = {};
    //print process.argv
    process.argv.forEach(function(val, index, array) {
        //console.log(index + ': ' + val);
        if (val === '-backup') {
            DUMP_TO_FILES = true;
        } else if (val === '-restore') {
            DUMP_TO_FILES = false;
        }
        if (val.indexOf('=') !== -1) {
            var splitter = val.trim().split('=');
            var key = splitter[0];
            var value = splitter[1];
            console.log('Setting config.rethinkdb.' + key + ' to "' + value + '".');
            config.rethinkdb[key] = value;
        }
        if (val === '-help' || val === '-h' || val === '-?' || val === '?') {
            console.log('rethinkxfer [-backup | -restore] [key]=[value] where values are assigned to config[key] properties.');
            halted = true;
        }
    });
}



if (halted) {
    return;
}
//Load the list of tables to work with. ONLY APPLIES if DUMP_TO_FILES = false.
var rawTables = readFile(JSON_FOLDER + '/tables.json');
if (rawTables.length) {
    tableFileList = JSON.parse(rawTables);
}

/**
 * Create/populate tables from a source database.
 *
 * @param r
 * @param conn
 * @param cb
 */
function createTablesFromDB(r, conn, cb) {
    var t;
    console.log('createTables 0');
    // List tables.
    r.db(source).tableList().run(conn, function(err, res) {
        if (err) {
            throw err;
        }
        console.log('createTables: Create these tables:\n', res);
        var completionCount = 0;
        for (t = 0; t < res.length; t++) {
            var func = function(tableName) {
                console.log('createTables: Creating "' + tableName + '".');
                r.db(destination).tableDrop(tableName).run(conn, function() {
                    // might fail... does it
                    // matter?
                    r.db(destination).tableCreate(tableName).run(conn, function(err, newTable) {
                        if (err) {
                            console.warn(err.message);
                        }
                        console.log('createTables: Created "' + newTable.config_changes[0].new_val.name + '".');
                        r.db(destination).table(tableName).insert(r.db(source).table(tableName)).run(conn, function(err, rez) {
                            if (err) {
                                throw err;
                            }
                            console.log('Inserted data for ' + tableName);
                            completionCount++;
                            if (completionCount === res.length) {
                                cb(done);
                            }
                        });
                    });
                });
            };
            // Closure for table name variable.
            func(res[t]);
        }
    });
}
/**
 * Create JSON files from tables.
 *
 * @param r
 * @param conn
 * @param cb
 */
function createJSONFromTables(r, conn, cb) {
    var t;
    console.log('createJSONFromTables 0');
    // List tables.
    r.db(source).tableList().run(conn, function(err, res) {
        if (err) {
            throw err;
        }
        console.log('createJSONFromTables: Create JSON from these tables:\n', res);
        writeFile(JSON_FOLDER + '/tables.json', JSON.stringify(res, null, 2));
        var completionCount = 0;
        for (t = 0; t < res.length; t++) {
            var func = function(tableName) {
                var jsonFileName = tableName + JSON_FILE_SUFFIX + '.json';
                // console.log('createJSONFromTables: Create "' + jsonFileName + '".');
                r.db(source).table(tableName).indexList().run(conn, function(error, indexCursor) {
                    console.log('Secondary indices for table "' + tableName + '": ', indexCursor);

                    console.log('createJSONFromTables: >>>>>>>> getting table data for "' + tableName + '".');
                    r.db(source).table(tableName).run(conn, {
                        timeFormat: 'raw'
                    }, function(err, cursor) {
                        console.log('createJSONFromTables: Creating "' + jsonFileName + '".');
                        if (err) {
                            console.error(err.message);
                            throw err;
                        }
                        cursor.toArray(function(err, result) {
                            if (err) {
                                console.error(err.message);
                                throw err;
                            }
                            //toCSV(JSON_FOLDER + '/' + tableName, result);
                            var output = {};
                            output.secondaryIndices = indexCursor;
                            output.rows = result;
                            var jsonOutput = JSON.stringify(output, null, 2);
                            writeFile(JSON_FOLDER + '/' + jsonFileName, jsonOutput);
                            completionCount++;
                            console.log('createJSONFromTables: Created "' + jsonFileName + '". (' + completionCount + '/' + res.length + ')');

                            if (completionCount === res.length) {
                                cb(done);
                            }
                        });
                    });
                });

            };
            // Closure for table name variable.
            func(res[t]);
        }
    });
}
var remainingBuffer = {};
/**
 * Create tables from JSON files.
 *
 * @param res
 * @param r
 * @param conn
 * @param cb
 */
function createTablesFromJSON(res, r, conn, cb) {
    console.log('createTablesFromJSON 0');
    console.log('createTablesFromJSON: Create these tables:\n', res);
    var completionCount = 0;
    for (var t = 0; t < res.length; t++) {
        /**
         * Func.
         *
         * @param tableName
         */
        var func = function(tableName) {
            console.log('createTablesFromJSON: Creating "' + tableName + '".');
            console.log('createTablesFromJSON: dropping "' + tableName + '".');
            r.db(destination).tableDrop(tableName).run(conn, function() {
                // might fail... does it matter?
                console.log('createTablesFromJSON: dropped, so we will create a new table "' + tableName + '".');
                r.db(destination).tableCreate(tableName).run(conn, function(err, newTable) {
                    if (err) {
                        console.warn('tableCreate ', err.message);
                    }
                    console.log('createTablesFromJSON: Created "' + newTable.config_changes[0].new_val.name + '".');
                    var newData = readFile(JSON_FOLDER + '/' + tableName + JSON_FILE_SUFFIX + '.json');
                    var newJSON = {};
                    var rows = [];
                    var secondaryIndices = [];
                    try {
                        newJSON = JSON.parse(newData);
                        rows = newJSON.rows;
                        secondaryIndices = newJSON.secondaryIndices;
                    } catch (parseErr) {
                        console.log('createTablesFromJSON: JSON.parse "' + newTable.config_changes[0].new_val.name + '": ', parseErr.message);
                    }
                    if (rows.length > MAX_TABLE_LENGTH) {
                        remainingBuffer[tableName] = [];
                        remainingBuffer[tableName] = rows.slice(MAX_TABLE_LENGTH);
                        console.warn('Truncated a huge table!!!');
                        rows = rows.slice(0, MAX_TABLE_LENGTH);
                    }
                    console.log('Inserting ' + rows.length + ' records for table "' + tableName + '".');
                    r.db(destination).table(tableName).insert(rows).run(conn, function(err, rez) {
                        if (err) {
                            console.warn('insert ', err.message);
                            completionCount++;
                            if (completionCount === res.length) {
                                cb(done);
                            }
                            return;
                        }
                        console.log('Inserted data for table "' + tableName + '".');
                        addIndices(conn, destination, tableName, secondaryIndices).then(function() {
                            completionCount++;
                            console.log('ADDED all secondary indices ', completionCount);
                            if (completionCount === res.length) {
                                cb(done);
                            }
                        });
                    });
                });
            });
        };
        // Closure for table name variable.
        func(res[t]);
    }
}

/**
 *
 * @param conn
 * @param destination
 * @param tableName
 * @param secondaryIndex
 * @returns {Promise}
 */
function addIndex(conn, destination, tableName, secondaryIndex) {
    var laterDude = new Promise(function(resolve, reject) {
        console.log('Insert secondary index "' + secondaryIndex + '" for table "' + tableName + '".');
        r.db(destination).table(tableName).indexCreate(secondaryIndex).run(conn, function(err, completion) {
            if (err) {
                reject(err);
            } else {
                console.log('wait for secondary index "' + secondaryIndex + '" for table "' + tableName + '".');
                r.db(destination).table(tableName).indexWait(secondaryIndex).run(conn, function(error, result) {
                    if (error) {
                        reject(error);
                    } else {
                        console.log('created secondary index "' + secondaryIndex + '" for table "' + tableName + '".');
                        resolve(completion);
                    }
                });
            }
        });
    });
    return laterDude;
}

/**
 *
 * @param conn
 * @param destination
 * @param tableName
 * @param secondaryIndices
 * @returns {Promise}
 */
function addIndices(conn, destination, tableName, secondaryIndices) {
    var myPledge = new Promise(function(resolve, reject) {
        if (secondaryIndices.length) {
            addIndex(conn, destination, tableName, secondaryIndices.shift()).then(function() {
                if (secondaryIndices.length === 0) {
                    //console.log('DONE because addIndex finished with secondaryIndices = []');
                    resolve(secondaryIndices);
                } else {
                    addIndices(conn, destination, tableName, secondaryIndices).then(function(remaining) {
                        if (secondaryIndices.length === 0) {
                            //console.log('DONE because addIndices finished with secondaryIndices = []');
                            resolve(secondaryIndices);
                        }
                    });
                }
            });
        } else {
            //console.log('DONE because addIndices started with secondaryIndices = []');
            resolve(secondaryIndices);
        }
    });
    return myPledge;
}

/**
 *
 * @param conn
 * @param r
 * @param cb
 */
function finishLargeTables(conn, r, cb) {
    var res = Object.keys(remainingBuffer);
    if (res.length) {
        console.warn('Unfinished business.', res);
        for (var t = 0; t < res.length; t++) {
            var func = function(tableName) {
                console.log('finishLargeTables: Inserting more data into "' + tableName + '".');
                // fixme
                var newJSON = remainingBuffer[tableName];
                r.db(destination).table(tableName).insert(newJSON).run(conn, function(err, rez) {
                    if (err) {
                        console.warn('insert ', err.message);
                        //            completionCount++;
                        //            if (completionCount === res.length) {
                        //              cb(done);
                        //            }
                        return;
                    }
                    console.log('Inserted data for table "' + tableName + '".');
                    //          completionCount++;
                    //          if (completionCount === res.length) {
                    //            cb(done);
                    //          }
                    cb();
                });
            };
            // Closure for table name variable.
            func(res[t]);
        }
    }
}
/**
 * Done.
 */
function done() {
    console.log('Are we done?');
    if (Object.keys(remainingBuffer).length) {
        finishLargeTables(_rdbConn, r, completelyDone);
    } else {
        completelyDone();
    }
}
/**
 * Really, completely, totally done.
 */
function completelyDone() {
    _rdbConn.close();
    console.log("ALL DONE");
}
/**
 * Main.
 */
function main() {
    //config.rethinkdb.db = 'perfhub_ben';

    r.connect(config.rethinkdb, function(err, conn) {
        if (err) {
            throw err;
        }
        _rdbConn = conn;
        if (DUMP_TO_FILES) {
            createJSONFromTables(r, conn, done);
        } else {
            createTablesFromJSON(tableFileList, r, conn, done);
        }
    });
}
/////////////////////// UTILITY METHODS ////////////////////////
/**
 * Read file.
 *
 * @name readFile
 * @method readFile
 * @param filePathName
 * @todo Please describe the return type of this method.
 * @return {String}
 */
function readFile(filePathName) {
    var _fs = require('fs');
    var _path = require('path');
    var FILE_ENCODING = 'utf8';
    filePathName = _path.normalize(filePathName);
    var source = '';
    try {
        source = _fs.readFileSync(filePathName, FILE_ENCODING);
    } catch (er) {
        // logger.error(er.message);
        source = '';
    }
    return source;
}
/**
 * Safe create file dir.
 *
 * @name safeCreateFileDir
 * @method safeCreateFileDir
 * @param path
 */
function safeCreateFileDir(path) {
    var dir = _path.dirname(path);
    if (!_fs.existsSync(dir)) {
        // // // logger.log("does not exist");
        _wrench.mkdirSyncRecursive(dir);
    }
}
/**
 * Safe create dir.
 *
 * @name safeCreateDir
 * @method safeCreateDir
 * @param dir
 */
function safeCreateDir(dir) {
    if (!_fs.existsSync(dir)) {
        // // // logger.log("does not exist");
        _wrench.mkdirSyncRecursive(dir);
    }
}
/**
 * Write file.
 *
 * @name writeFile
 * @method writeFile
 * @param filePathName
 * @param source
 */
function writeFile(filePathName, source) {
    filePathName = _path.normalize(filePathName);
    safeCreateFileDir(filePathName);
    _fs.writeFileSync(filePathName, source);
}

function toCSVNew(tableName, data) {
    var json2csvCallback = function(err, csv) {
        if (err) {
            console.error(err);
        }
        writeFile(tableName + '.csv', csv);
        console.log('Exported table ' + tableName + ' to CSV.');
    };
    csvConverter2.json2csv(data, json2csvCallback);
}

function getColumnNames(jsonIn) {
    var colCount = 0;
    var cols = [];
    if (jsonIn instanceof Array) {
        for (var index = 0; index < jsonIn.length; index++) {
            var row = jsonIn[index];
            var tempCols = [];
            for (var p in row) {
                if (row.hasOwnProperty(p)) {
                    tempCols.push(p);
                }
            }
            if (tempCols.length > cols.length) {
                cols = tempCols;
            }
        }
    }
    return cols;
}

function toCSV(tableName, data) {
    var cols = getColumnNames(data);
    console.warn('Column Names: ' + cols);
    var bufferedCallback = function(error, result) {
        console.warn('>>>>>>>>>>>>>>>>>>>>>>> BUFFERED');
        //console.warn(arguments);
        if (error) {
            console.error(err);
        }
        writeFile(tableName + '_BUFFED.csv', result);
        console.log('Exported table ' + tableName + ' to CSV.');
    };
    var json2csvCallback = function(err, csv) {
        if (!csv || csv.length === 0) {
            console.warn('toCSV: ' + tableName + ': empty file');
        } else if (csv.indexOf('[object Object]') !== -1) {
            console.warn(tableName + ': UH OH, not serializing JS objects in columns....');
            var fields = [];
            for (var index = 0; index < cols.length; index++) {
                fields.push({
                    name: cols[index],
                    label: cols[index],
                    filter: function(value) {
                        if (value.toString().indexOf('[object Object]') !== -1) {
                            // console.warn(tableName + ': STILL, not serializing JS objects in columns, forcing...');
                            return JSON.stringify(value); // no pretty-printing here
                        }
                        return value;
                    }
                });
            }
            // console.warn(fields);
            csvConverter2.csvBuffered(data, {
                fields: fields
            }, bufferedCallback);
        }
        if (err) {
            if (err.message.indexOf('Not all documents have the same schema') !== -1) {
                // do over with hard-coded column names
                var options = {
                    KEYS: cols
                };
                csvConverter.json2csv(data, json2csvCallback, options);
                return;
            } else {
                console.error(err);
            }
        }
        writeFile(tableName + '.csv', csv);
        console.log('Exported table ' + tableName + ' to CSV.');
    };
    csvConverter.json2csv(data, json2csvCallback);
}

function jsonCSVTest() {
    var jsoncsv = require('json-csv');
    var items = [{
        contact: {
            company: 'Widgets, LLC',
            name: 'John Doe',
            email: 'john@widgets.somewhere'
        },
        registration: {
            year: 2013,
            level: 3
        }
    }, {
        contact: {
            company: 'Sprockets, LLC',
            name: 'Jane Doe',
            email: 'jane@sprockets.somewhere'
        },
        registration: {
            year: 2013,
            level: 2
        }
    }];
    jsoncsv.csvBuffered(items, {
        fields: [{
            name: 'contact.company',
            label: 'Company'
        }, {
            name: 'contact.name',
            label: 'Name'
        }, {
            name: 'contact.email',
            label: 'Email'
        }, {
            name: 'registration.year',
            label: 'Year'
        }, {
            name: 'registration.level',
            label: 'Level'
            //                ,
            //                filter : function(value) {
            //                switch(value) {
            //                case 1 : return 'Test 1'
            //                case 2 : return 'Test 2'
            //                default : return 'Unknown'
            //                }
            //                }
        }]
    }, function(err, csv) {
        console.log(csv);
    });
}
/////////////////////////// LASTLY, CALL MAIN //////////////////////////
main();