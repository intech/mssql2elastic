/**
 * Created by ivan on 25.07.15.
 */
var sql = require('mssql'),
    elasticsearch = require('elasticsearch');

var client = new elasticsearch.Client({
    host: 'localhost:9200',
    log: 'error'
});

sql.connect({
    user: 'sa',
    password: 'nhfrnjh',
    server: '122.233.190.188',
    port: 31337,
    database: 'Warehouse_Semena',
    stream: true,
    options: {}
}, function(err) {
    if(err) throw err;

    var request = new sql.Request();
    request.stream = true;
    request.query('SELECT * FROM ТОВАР');

    var rows = [], bulk = 0;

    request.on('recordset', function(columns) {
        // Emitted once for each recordset in a query
    });

    request.on('row', function(row) {
        // Emitted for each row in a recordset
        rows.push({ index:  { _index: 'semena', _type: 'products' }});
        rows.push(row);
        if(rows.length >= 1000) {
            //console.log('rows', rows.length);
            client.bulk({
                body: rows
            }, function (err, resp) {
                if (err) throw err;
                console.log('bulk', bulk++);
            });
            rows = [];
        }
    });

    request.on('error', function(err) {
        // May be emitted multiple times
        console.log('error', err);
    });

    request.on('done', function(returnValue) {
        // Always emitted as the last one
        //console.log('rows', rows.length);
        if(rows.length) {
            client.bulk({
                body: rows
            }, function (err, resp) {
                if (err) throw err;
                console.log('done', rows.length);
            });
            rows = [];
        }
    });

});