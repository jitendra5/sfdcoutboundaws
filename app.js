var express        = require( 'express' );
var http           = require( 'http' );
var jsforce        = require('jsforce');
var bodyParser = require('body-parser');
var each = require('async-each');
const AWS = require('aws-sdk');
var app            = express();
var log4js = require('log4js');
let tStamp =Date.now();
log4js.configure({ // configure to use all types in different files.
    appenders: {
        cheeseLogs: { type: 'file', base: 'logs/', filename: 'logs/debugLogs-'+tStamp+'.log' },
        console: { type: 'console' },
      },
     categories: {
        another: { appenders: ['console'], level: 'debug' },
        default: { appenders: [ 'console','cheeseLogs'], level: 'debug' }
    }
});

const log4js_extend = require("log4js-extend");

log4js_extend(log4js, {
    path: __dirname,
    format: "at @name (@file:@line:@column)"
  });

  var logger = log4js.getLogger('debug');
let conn;
app.set( 'port', process.env.PORT || 5000 );
var jsonParser = bodyParser.json();
var urlencodedParser = bodyParser.urlencoded({ extended: false })
app.use(bodyParser.json({ type: 'application/json' }));
app.post('/api1.0/cloudbyz/sfdcObjects',urlencodedParser, function (req, res) {
    //logger.debug(req);
    logger.debug(req.body.objects);
    let sfdcObjects =req.body.objects;

  //We are trying to connect to any dynamoDB on any AWS instance.
  AWS.config.update({
    region: req.body.region,
    endpoint: req.body.endpoint,
    // accessKeyId default can be used while using the downloadable version of DynamoDB.
    // For security reasons, do not store AWS Credentials in your files. Use Amazon Cognito/ Http request.
    accessKeyId: req.body.accessKeyId,
    // secretAccessKey default can be used while using the downloadable version of DynamoDB.
    // For security reasons, do not store AWS Credentials in your files. Use Amazon Cognito/ Http request.
    secretAccessKey: req.body.secretAccessKey
});

//Now that we are authenticated with AWS, lets create an insatnce of Dynamodb to perform required operations.

var dynamodb = new AWS.DynamoDB({apiVersion: '2012-08-10'});
var counter=0;
let tableCounter =0;
let batchWriteCheck=0;

function getFirstHundredTables() {
    // Setting URL and headers for request
            var params={
                'Limit':100,
            };
            logger.debug(params);
    // Return new promise 
    return new Promise(function(resolve, reject) {
    	// Do async job by calling the DynamoDB.
        dynamodb.listTables(params, function(err, data) {
            if (err) {
                logger.debug(err);
                counter++;
                reject(err);
            }
            else {
                var tables= data.TableNames;
                //logger.debug(tables);
                logger.debug('Length: '+tables.length);
                counter++;
                resolve(data);
            }
        });
    })
}
function getNextHundredTables(lastTableName) {
    // Setting URL and headers for request
    var params;
    if(lastTableName === ''){
        params={
            'Limit':100
        };
    }
    else{
        params={
            'Limit':100,
            'ExclusiveStartTableName' : lastTableName
        };
    }
            logger.debug(params);
    // Return new promise 
    return new Promise(function(resolve, reject) {
    	// Do async job by calling the DynamoDB.
        dynamodb.listTables(params, function(err, data) {
            if (err) {
                logger.debug(err);
               //counter++;
                reject(err);
            }
            else {
                var tables= data.TableNames;
                //logger.debug(tables);
                logger.debug('Length: '+tables.length);
                //counter++;
                resolve(data);
            }
        });
    })
}
let sfdcConnFn =function callJSForce(tables){
    logger.debug('Calling JSFORCE now.!!!');
    return new Promise(function(resolve, reject) {
        var conn = new jsforce.Connection({
            // you can change loginUrl to connect to sandbox or prerelease env.
            loginUrl : 'https://login.salesforce.com'
            });
            conn.login('amazon.ctms@cloudbyz.com', 'Summer@2019TbASAaMIfY4B1UmkccpwZzp5', function(err, userInfo) {
            if (err) { 
                var resp={
                    con :'error',
                    status:'400'
                };
                reject(resp);
                console.error(err); 
            }
            else{
                //logger.debug(conn.instanceUrl);
                logger.debug("User ID: " + userInfo.id);
                logger.debug("Org ID: " + userInfo.organizationId);
                var resp={
                    con :conn,
                    status:'200',
                    tables:tables
                };
                resolve(resp);
            }//sucess conn else
            });//conn login fn.
    
    })
}
let tableStatus = function verifyTables(sfdcObjects,dynamoTables){
    let objs= sfdcObjects.split(',');
    logger.debug(sfdcObjects);
    logger.debug(objs);
    var sfdcObjStatus=[];
    return new Promise((resolve,reject)=>{
        for(var i=0;i<objs.length;i++){
            let jsObj ={};
            if(dynamoTables.includes(objs[i])){
                jsObj[objs[i]] = 'TableExists';
                sfdcObjStatus.push(jsObj);
            }
            else{
                jsObj[objs[i]] = 'NoTable';
                sfdcObjStatus.push(jsObj);
            }
        }
        resolve(sfdcObjStatus);
    })
}
let tableAvailable = function isTableAvailable(tableName, avilStr){
    return new Promise((resolve,reject)=>{
        var params = {
            TableName: tableName 
          };
          dynamodb.waitFor(avilStr, params, function(err, data) {
            if (err) {
                logger.debug(err, err.stack); 
                //reject(err);
                resolve('Error');
            }
            else  {
                //logger.debug(data);
                resolve(data);
            }       
          });
    })
}
let objectOps = function(finalRes,con,db,backupRecId){
    return new Promise((resolve,reject)=>{
        let objectNames='';
    for(let i of finalRes){
        objectNames+=Object.keys(i)[0]+',';
    }
    //logger.debug('1. objectNames: '+ objectNames);
    if(objectNames.length>0){
        objectNames = objectNames.substring(0,objectNames.length-1);
    }
    //logger.debug('2. objectNames: '+ objectNames);
    con.sobject("BackupSelection__c").update([
        { Id : backupRecId, Backup_Status__c : 'Success',Objects_backedup__c: objectNames},
      ],
      function(err, rets) {
        if (err) { 
            //return console.error(err);
            resolve(err);
         }
        for (var i=0; i < rets.length; i++) {
          if (rets[i].success) {
            logger.debug("Backup Section Object Updated Successfully : " + rets[i].id);
          }
        }
        resolve('Object updated!!');
      });
    })
}
/**
 * 
 * @param {The name of table to create} tableName 
 * The function creates a DynamoDb table.
 */
let crtTable = function createTable(tableName){
   // logger.debug('creating table for: '+ tableName);
    var params = {
        TableName : tableName,
        KeySchema: [
            {
                AttributeName: "Id", 
                KeyType: "HASH"
            }
        ],
        AttributeDefinitions: [
            {
                AttributeName: "Id", 
                AttributeType: "S"
            }
        ],
        ProvisionedThroughput: {
            ReadCapacityUnits: 100,
            WriteCapacityUnits: 100
        }
    };
    return new Promise((resolve,reject)=>{
        dynamodb.createTable(params, function(err, data) {
            if (err) {
                logger.debug(err, err.stack); 
                resolve({[tableName] : 'NotCreated'});
            }
            else {
                let isTableAvail =tableAvailable(tableName,'tableExists');
                isTableAvail.then(()=>{
                    resolve({[tableName] : 'created'});
                })
                
            }
    })
    
})
}
/**
 * 
 * @param {* The name of table to delete } tableName 
 * The function deletes a table from Dynamodb.
 */
let deleteOps =function delTable(tableName){
    var params = { 
        TableName : tableName
    };
    return new Promise((resolve,reject)=>{
    dynamodb.deleteTable(params, function(err, data) {
        if (err) {
            logger.debug("Unable to delete table. Error JSON:", JSON.stringify(err, null, 2));
            resolve({[tableName] : 'NotDeleted','error':[err]});
        } else {
            logger.debug("Deleted table. Table description JSON:", JSON.stringify(data, null, 2));
            let isTableAvail =tableAvailable(tableName,'tableNotExists');
                isTableAvail.then((res)=>{
                    //resolve({[tableName] : 'Deleted'});
                    logger.debug(res);
                    if(res != 'Error'){
                        logger.debug(tableName+ ' Deleted Successfully!!');
                        return crtTable(tableName);
                    }
                    
                })
                .then((res)=>{
                    resolve(res);
                })
                
            }
    });
})
}
let updateCapacity = function updateThroughput(tableName,con,db){
    logger.debug('Updating Throughput: '+ tableName);
    var params ={
        ProvisionedThroughput: {
            ReadCapacityUnits: 10, 
            WriteCapacityUnits: 10
           }, 
           TableName: tableName
    };
    return new Promise((resolve,reject)=>{
        db.updateTable(params, function(err, data) {
            if (err) {
                logger.debug(tableName+':'+'NotReducedThroughput');
                resolve({[tableName] : 'NotReducedThroughput'});
            } 
            else   {
                logger.debug(tableName+':'+'ReducedThroughput'); 
                resolve({[tableName] : 'ReducedThroughput'});
            }          
          });
    })    

}
let bckTable = function backupTable(tableName){
    logger.debug('starting backup for table: '+ tableName);
    var params = {
        BackupName: tableName+'-backup', /* required */
        TableName: tableName /* required */
      };
      
    return new Promise((resolve,reject)=>{
        dynamodb.createBackup(params, function(err, data) {
            if (err) {
                logger.debug(tableName+':'+'NotBackedup');
                logger.debug(err);
                resolve({[tableName] : 'NotBackedup'});
            } 
            else   {
                logger.debug(tableName+':'+'Backedup'); 
                resolve({[tableName] : 'Backedup'});
            }          
          });
    })
}
let crtBckTable = function createOrBackupTable(tableObj){
    return new Promise((resolve,reject)=>{
        logger.debug(tableObj);
        logger.debug(typeof tableObj);
        logger.debug(Object.values(tableObj));
        if(Object.values(tableObj).includes('TableExists')){
            let backupTable = bckTable(Object.keys(tableObj)[0]);
            backupTable.then((backupRes)=>{
                resolve(backupRes);
            })
        }
        else if(Object.values(tableObj).includes('NoTable')){
            let createTable = crtTable(Object.keys(tableObj)[0]);
            createTable.then((createRes)=>{
                resolve(createRes);
            })
        }
    })
}

let reduceCapacity = function handleInsertData(tableObj,con,db){
    return new Promise((resolve,reject)=>{
        //logger.debug(tableObj);
        let updateTable = updateCapacity(Object.keys(tableObj)[0],con,db);
        updateTable.then((updteDataRes)=>{
            resolve(updteDataRes);
        })
    })
}

let chooseCreateOrDeleteCreate = function handleInsertData(tableObj,con,db){
    return new Promise((resolve,reject)=>{
        logger.debug(tableObj);
        //logger.debug(typeof tableObj);
        logger.debug(Object.values(tableObj));
        if(Object.values(tableObj).includes('created')){
            let insertData = processData(Object.keys(tableObj)[0],con,db);
            insertData.then((insDataRes)=>{
                resolve(insDataRes);
            })
        }
        else if(Object.values(tableObj).includes('Backedup')){
            let deleteTable = deleteOps(Object.keys(tableObj)[0]);
            deleteTable.then((deleteRes)=>{
                let insertData = processData(Object.keys(tableObj)[0],con,db);
                insertData.then((insDataRes)=>{
                    resolve(insDataRes);
                })
            })
        }
    })

}
let tableOps = function createOrBackupObjects(tables){
    logger.debug('---TABLE OPS----');
      let createBackupTables = tables.map((table) =>crtBckTable(table));
      return Promise.all(createBackupTables);
}

let CRUDOps = function processDataObjects(tables,con,db){
    logger.debug('---CRUD OPS----');
      let crudTables = tables.map((table) =>chooseCreateOrDeleteCreate(table,con,db));
      return Promise.all(crudTables);
}
let throughputOps = function(tables,con,db){
    logger.debug('---Throughput OPS----');
    let capacityUnits = tables.map((table) =>reduceCapacity(table,con,db));
      return Promise.all(capacityUnits);
}

let sfdcFields = function getFieldsOfObject(tableName,con){
    logger.debug('272: Getting fields for: '+tableName );
    return new Promise((resolve,reject)=>{
        var fullNames = [tableName];

        con.describe(tableName, function(err, meta) {
            if (err) { 
                var emptyArr=[];
                console.error(err); 
                resolve(emptyArr);
            }
            else{
                //logger.debug('283: Object : ' + meta.label);
                var fields=[];
                for(var i=0;i<meta.fields.length;i++){
                fields.push(meta.fields[i].name);
                }
                logger.debug('288: count of fields: '+ fields.length);
                //logger.debug(fields);
                resolve(fields);
            }
          });
   })
}
let recursiveCheck =0;
let batchOps = function runBatch(dynamodb,params,objectName){
    //logger.debug(dynamodb);
    logger.debug(params);
    let parameters =params[objectName];
    logger.debug(parameters);
    for(let para in parameters){
        logger.debug(para);
        logger.debug(para['PutRequest']);
        logger.debug(para['Item']);
    }
    return new Promise((resolve,reject)=>{
        dynamodb.batchWriteItem(params, function(err, data) {
            if (err) {
                logger.debug(err);
                recursiveCheck++;
                if(recursiveCheck <3){
                    batchOps(dynamodb,params,objectName);
                }
                resolve('DataInserted');
            }
            else {
                //logger.debug(data); 
                var unProcessParam = {};
                unProcessParam.RequestItems = data.UnprocessedItems;
                if(Object.keys(unProcessParam.RequestItems).length != 0) {
                    logger.debug(unProcessParam); 
                    batchOps(dynamodb,unProcessParam,objectName);
                }
                resolve('DataInserted');
            }    
        });
    })
}

let batchWriteAwsIterator = function insertBatch(objectName,start,end,dataLength,totalData,dynamodb){
    
    return new Promise((resolve,reject)=>{
        logger.debug('Start of batch, dataLength :'+ dataLength);
                var params = {
                    RequestItems: {
                    }
                };
                params.RequestItems[objectName] =[];
                for(var i=start;i<totalData.records.length && i< end;i++){
                    var recordKeys =[];
                    for(var k in totalData.records[i]) 
                        recordKeys.push(k);
                    var pRequest={};
                    var PutRequest={};
                    var Item ={};
                    for(var j=0;j<recordKeys.length;j++){
                        var field=recordKeys[j];
                        var value= totalData.records[i][field];
                        var valObj={};
                        if(value === undefined || value == null){
                            valObj['S']='NULL';
                        }
                        else{
                            valObj['S']=value.toString();
                        }
                        Item[field] =valObj;
                    }
                    PutRequest['Item']=Item;
                    pRequest['PutRequest'] =PutRequest;
                    //logger.debug(pRequest);
                    params.RequestItems[objectName].push(pRequest);
                    }
                    var batchOpsCall = batchOps(dynamodb,params,objectName);
                    var x = batchOpsCall.then((res)=>{
                       // logger.debug('batch ops for '+ objectName+ ' : '+res);
                        if(dataLength - 25 > 0){
                            logger.debug('After batch ran, dataLength :'+ dataLength-end);
                            //setTimeout(()=>{
                                return batchWriteAwsIterator(objectName,end,end+25,dataLength-25,totalData,dynamodb);
                            //},1000*);
                            
                           
                        } 
                        else{
                            logger.debug('RESOLVED IN THE ITERATOR');
                            logger.debug(objectName);
                             //setTimeout(()=>{
                                return {[objectName] : 'DataInserted'};
                                //},1000);
                        }
                       
                    });
                    resolve(x);
    })
}

let batchWriteAWS = function writeToAWS(tableName,data,con,dynamodb){
    return new Promise((resolve,reject)=>{
        var count=0; var check25=25;
        if(data.records.length>0){
            //let backoffVar=0;
            let batchCall =batchWriteAwsIterator(tableName,count,check25,data.records.length,data,dynamodb);
            batchCall.then((batchResult)=>{
                logger.debug(batchResult);
                //return batchResult;
                resolve(batchResult);
            });
            //resolve(xx);
        }
    })
}
function handleQueryMore(tableName,result,conn,dynamodb) {
    logger.debug('Inside handleQueryMore method---'+result);
    //logger.debug(conn);
    return new Promise((resolve,reject)=>{
        conn.queryMore(result, function(err, resultMore) {
        if (err) {
            logger.debug(err);
            return({[tableName]: 'FailedDataInsert'});
        }
        //do stuff with result
        else {
            //logger.debug('result: '+ result.records.length);
            logger.debug('resultMore: '+ resultMore.records.length);
            logger.debug('resultMore done: '+ resultMore.done);
            logger.debug('Next resultMore Record: '+ resultMore.records[0].Id);
            if(resultMore.records.length){
                var batchWriteAWSCall = new batchWriteAWS(tableName,resultMore,conn,dynamodb);  
                let yy = batchWriteAWSCall.then((res)=>{
                    logger.debug('Resolved batchWriteAWS---in handleQueryMore');
                    if (!resultMore.done) //didn't pull all records
                    {
                    logger.debug('Next Result Record: '+ resultMore.records[0].Id);
                    logger.debug('next url: '+ resultMore.nextRecordsUrl);
                    return handleQueryMore(tableName,resultMore.nextRecordsUrl,conn,dynamodb);
                    }
                    else{
                        //return 'completed ';
                        return({[tableName]: 'DataInserted'});
                    }
                });
                resolve(yy);
            }
            }
        });
    })
    
  }

let getData = function getDataForFields(tableName,con,dynamodb){
    
    return new Promise((resolve,reject)=>{
        logger.debug('Getting data for : '+ tableName);
        var fieldsForObject =[];
        let getFields =sfdcFields(tableName,con);
        getFields.then((fields)=>{
            fieldsForObject =fields;
            //logger.debug(fieldsForObject);
            if(fieldsForObject.length==0){
                resolve(tableName+ ':'+'NoDataInserted');
            }
            else{
                var soql='';
                for(var i=0;i<fieldsForObject.length;i++){
                    soql+=fieldsForObject[i]+',';
                }
                soql = soql.substring(0, soql.length - 1);
                soql = 'Select '+ soql+' From '+ tableName;
                logger.debug('soql: '+ soql);
                var records = [];
                con.query(soql, function(err, result) {
                if (err) { 
                    console.error(err);
                 }
                 else{
                    logger.debug(tableName+ ' result: '+ result.records.length);
                    logger.debug(tableName+' resultMore: '+ result.records.length);
                    logger.debug(tableName+' resultMore done: '+ result.done);
                    
                    var nextData =result.done;
                    if(result.done){
                        if(result.records.length >0){
                            var batchWriteAWSCall = new batchWriteAWS(tableName,result,con,dynamodb);  
                            batchWriteAWSCall.then((response)=>{
                                logger.debug('Resolved batchWriteAWS---in getData');
                                logger.debug(response);
                                resolve(response);
                            })
                        }
                    }
                    else{
                        logger.debug(tableName+' next url: '+ result.nextRecordsUrl);
                        if(result.records.length >0){
                            var batchWriteAWSCall = new batchWriteAWS(tableName,result,con,dynamodb);  
                            batchWriteAWSCall.then((response)=>{
                                logger.debug('Resolved batchWriteAWS---in getData');
                                logger.debug(response);
                                return handleQueryMore(tableName,result.nextRecordsUrl,con,dynamodb);
                            })
                            .then((handleQueryMoreRes)=>{
                                logger.debug(handleQueryMoreRes);
                                resolve(handleQueryMoreRes);
                            })
                        }
                    }
                     
                 }//else
                });
            }
        })
    })
}

let processData = function formatDataForBatchWriteOps(table,con,dynamodb){
    logger.debug('PROCESS DATA FOR CREATED TABLE.....: '+table);
    return new Promise((resolve,reject)=>{
            let getDataCall = getData(table,con,dynamodb);
            getDataCall.then((getDataCallResp)=>{
                logger.debug('Data Processes completed for: '+ table);
                resolve(getDataCallResp);
            })
    })
}
var userDetails=[];

let getExistingDynamoDbTables = function getTables(lastTableName){
    return new Promise((resolve,reject)=>{
        let first100Tables =getNextHundredTables();
        first100Tables.then((result)=>{
            if(result !== undefined && result.hasOwnProperty('TableNames')){
                logger.debug("Fetched "+result.TableNames.length+" tables..");
                userDetails = userDetails.concat(result.TableNames);
                //logger.debug( result);
                logger.debug("result.LastEvaluatedTableName: "+ result.LastEvaluatedTableName);
                logger.debug("result.TableNames.length: "+ result.TableNames.length);
                if(result.LastEvaluatedTableName !='' && result.TableNames.length==100){
                    return getNextHundredTables(result.LastEvaluatedTableName);
                }
                else{
                    resolve(userDetails);
                }
            }
            else{
                resolve(userDetails);
            }
        })
        .then((result)=>{
            //next hundered tables
            if(result !== undefined && result.hasOwnProperty('TableNames')){
                logger.debug("Fetched "+result.TableNames.length+" tables..");
                userDetails = userDetails.concat(result.TableNames);
                logger.debug("result.LastEvaluatedTableName: "+ result.LastEvaluatedTableName);
                logger.debug("result.TableNames.length: "+ result.TableNames.length);
                if(result.LastEvaluatedTableName !='' && result.TableNames.length==100){
                    return getNextHundredTables(result.LastEvaluatedTableName);
                }
                else{
                    resolve(userDetails);
                }

            }
            else{
                resolve(userDetails);
            }
            
        })
        .then((result)=>{
            //next 56 tables. As DynamoDb has max. 256 tables.
            if(result!==undefined && result.hasOwnProperty('TableNames')){
                logger.debug("Fetched "+result.TableNames.length+" tables..");
                userDetails = userDetails.concat(result.TableNames);
                logger.debug("result.LastEvaluatedTableName: "+ result.LastEvaluatedTableName);
                logger.debug("result.TableNames.length: "+ result.TableNames.length);
                resolve(userDetails);
            }
            else{
                resolve(userDetails);
            }
            
        })
        .catch((error)=>{
            logger.debug(error);
        })

    });
}

function main() {
    var con;
    var db =dynamodb;
    let getTables = getExistingDynamoDbTables();
    var totalTables=[];
    getTables.then((result)=>{
        logger.debug("#####Finally total fetched Tables: "+result.length);
        //logger.debug(result);
        res.send({'Status': 'In Progress' ,'statusCode':'200'});
        return sfdcConnFn(result);
    })
    .then((result)=>{
        //logger.debug(result);
        logger.debug('####SFDC con status: '+result.status);
        if(result.status !='400'){
            con = result.con;
            totalTables = totalTables.concat(result.tables);
            return tableStatus(sfdcObjects,result.tables);
        }
    })
    .then((tableStatus)=>{
        logger.debug('Selected objects will classified wheather they have a table in Dynamodb or not..');
        logger.debug(tableStatus);
        return tableOps(tableStatus);
    })
    .then((tableOpsResult)=>{
        logger.debug('Tables will be created if doesnt exist or will be Backedup if present...');
        logger.debug(tableOpsResult);
       return CRUDOps(tableOpsResult,con,db);

    })
    .then((result)=>{
        //logger.debug('LAST...');
        logger.debug(result);
        //res.end(result);
        return throughputOps(result,con,db);
    })
    .then((finalRes)=>{
        logger.debug(finalRes);
        let backupRecId =req.body.recId;
        return objectOps(finalRes,con,db,backupRecId);
    })
    .then((objectOpsRes)=>{
        logger.debug(objectOpsRes);
        logger.debug('------DONE------');
        res.status(200).end();
    })
    .catch((error)=>{
        logger.debug(error);
        res.status(404).end();
    })

}

//starting the Event loop execution.
main();


});
app.get('/',urlencodedParser, function (req, res) {
    res.send(JSON.stringify({'Status': 'SFDC-DynamoDB REST-API Running in AWS','Response':'200'}));
});
app.get('/api1.0/cloudbyz/test',urlencodedParser, function (req, res) {
    res.send(JSON.stringify({'Status': 'SFDC-DynamoDB REST-API Running in AWS','Response':'200'}));
});
app.post('/api1.0/cloudbyz/verifyAws',urlencodedParser, function (req, res) {
    logger.debug('l599: '+req.body.objects);
    AWS.config.update({
        region: req.body.region,
        endpoint: req.body.endpoint,
        // accessKeyId default can be used while using the downloadable version of DynamoDB.
        // For security reasons, do not store AWS Credentials in your files. Use Amazon Cognito/ Http request.
        accessKeyId: req.body.accessKeyId,
        // secretAccessKey default can be used while using the downloadable version of DynamoDB.
        // For security reasons, do not store AWS Credentials in your files. Use Amazon Cognito/ Http request.
        secretAccessKey: req.body.secretAccessKey
    });
    var dynamodb = new AWS.DynamoDB({apiVersion: '2012-08-10',maxRetries: 15, retryDelayOptions: {base: 500}});
    var request = dynamodb.listTables();
    request.send();
    request.on('success', function(response) {
    console.log('l599: '+"Success!");
    res.send(JSON.stringify({'Status': 'AWS credentials are Verified.','statusCode':'200'}));
  }).on('error', function(error, response) {
    console.log('l617: '+error);
    res.send(JSON.stringify({'Status': error.message ,'statusCode':error.statusCode}));
  });
    
});
app.post('/api1.0/cloudbyz/verifysfdc',urlencodedParser, function (req, res) {
    logger.debug('uName '+req.body.uName);
    logger.debug('PWD: '+req.body.pwd);
    logger.debug('Token: '+req.body.rToken);
    logger.debug('url: '+req.body.url);
    let pwdComb =req.body.pwd +req.body.rToken;
    let resp={};
        var conn = new jsforce.Connection({
            // you can change loginUrl to connect to sandbox or prerelease env.
            loginUrl : req.body.url
            });
            conn.login(req.body.uName, pwdComb, function(err, userInfo) {
            if (err) { 
                resp={
                    con :'error',
                    status:'400'
                };
                res.send({'Status': err.message ,'statusCode':'404'});
            }
            else{
                logger.debug("User ID: " + userInfo.id);
                logger.debug("Org ID: " + userInfo.organizationId);
                conn =conn;
                resp={
                    con :conn,
                    status:'200'
                };
                res.send({'Status': 'Success' ,'statusCode':'200'});
            }//sucess conn else
            });//conn login fn.
});
app.post('/api1.0/cloudbyz/getRecordCount',urlencodedParser, function (req, res) {
    logger.debug(req.body.objNames);
    let connectTosfdc = function sfdcConnect(req){
        logger.debug('uName '+req.body.uName);
        logger.debug('PWD: '+req.body.pwd);
        logger.debug('Token: '+req.body.rToken);
        logger.debug('url: '+req.body.url);
        let pwdComb =req.body.pwd +req.body.rToken;
        return new Promise((resolve,reject)=>{
            var conn = new jsforce.Connection({
                // you can change loginUrl to connect to sandbox or prerelease env.
                loginUrl : req.body.url
                });
                conn.login(req.body.uName, pwdComb, function(err, userInfo) {
                if (err) { 
                    resp={
                        con :'error',
                        status:'400'
                    };
                    //res.send({'Status': err.message ,'statusCode':'404'});
                    reject({status:'error', con: 'error'});
                }
                else{
                    logger.debug("User ID: " + userInfo.id);
                    logger.debug("Org ID: " + userInfo.organizationId);
                    conn =conn;
                    resp={
                        con :conn,
                        status:'200'
                    };
                    //res.send({'Status': 'Success' ,'statusCode':'200'});
                    resolve({status:'success', con: conn});
                }//sucess conn else
                });//conn login fn.

        })

    }
    let queryCount = function queryRecordCount(objectName,conn){
        let soqlQuery = 'SELECT count(Id) FROM '+objectName;
        return new Promise((resolve,reject)=>{
            conn.query(soqlQuery, function(err, result) {
                if (err) { 
                    console.error(err);
                    resolve({[objectName]:'NotQueryable'});
                 }
                 else{
                    console.log("fetched : ");
                    console.log( result.records[0].expr0);
                    resolve({[objectName]:result.records[0].expr0})
                 }
              });
        });
    }
    let countOPS = function countRecs(sfdcObjects,conn){
        let recordCount = sfdcObjects.map((obj)=>queryCount(obj,conn));
        return Promise.all(recordCount);
    }
    function main(){
        if(req.body.objNames.length >0){
            //let createBackupTables = tables.map((table) =>crtBckTable(table));
            let connect = connectTosfdc(req);
            connect.then((result)=>{
                logger.debug(result);
                if(result.status =='success'){
                    logger.debug(req.body.objNames);
                    let objNamesArr =req.body.objNames.split(',');
                    logger.debug('objNamesArr: '+ objNamesArr);
                    return countOPS(objNamesArr,result.con);
                }
            })
            .then((result)=>{
                logger.debug(result);
                //res.json(result);
                res.send(JSON.stringify({'recordCount': result,'Response':'200'}));
            })
            .catch(err =>{
                console.log(err);
                res.json({status:'400', message:'Error'});
            });

        }
        else{
            res.send(JSON.stringify({'Status': 'Select Objects to get reocord count','Response':'400'}));
        }
        
    }
    main();
    //res.send(JSON.stringify({'Status': 'SFDC-DynamoDB REST-API Running in AWS','Response':'200'}));
});
http.createServer( app ).listen( app.get( 'port' ), function (){
  logger.debug( '######Cloudbyz Express server listening on port: ' + app.get( 'port' ));
});

