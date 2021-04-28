var express = require('express');
var router = express.Router();
const MongoClient = require('mongodb').MongoClient;
const config = require('../config.json');
const XLSX = require('xlsx');
const fs = require('fs');
const moment = require('moment-timezone');
const log4js = require('log4js');
const logger = log4js.getLogger("Samplecheck");
const path = require('path');
const _ = require('lodash');
const __ = require('lodash-contrib');

/* GET users listing. */
router.post('/', async function(req, res, next) {
    // Connection URL
    const url = req.body.url;
    const backupUnit = req.body.backupUnit;
    const backupValue = req.body.backupValue;

    res.send(config.collection);
    // Use connect method to connect to the server
    await MongoClient.connect(url, async function(err, client) {

        console.log('connect');
        logger.info("Connected successfully to server");

        const db = client.db(config.db);
        const collection = db.collection(config.collection);
        await collection.createIndex({ dataset_id: 1});

        await db.createCollection('samplehistory').then(value => {}).catch(error => {});
        const collectionHistory = db.collection('samplehistory');

        await db.createCollection('sampleresult_history').then(value => {}).catch(error => {});
        const collectionResHistory = db.collection('sampleresult_history');
        await collectionResHistory.createIndex({ date: 1});
        await collectionResHistory.createIndex({ dataset_id: 1});
        await collectionResHistory.createIndex({ result: 1});


        const todayCheck = moment().tz('Asia/Seoul').format("YYYY-MM-DD");
        const backupDay = moment(todayCheck).subtract(backupValue, backupUnit).tz('Asia/Seoul').format("YYYYMMDDhhmmss");
        const backupDay2 = moment(todayCheck).subtract(backupValue, backupUnit).tz('Asia/Seoul').format("YYYYMMDDhhmmss");
        const todayTime = moment().tz('Asia/Seoul').format("YYYYMMDDhhmmss");

        let findResult = await collection.find({asset_type:'table', status:'검토완료',  $or: [ { sampleCheckDate : null } , { sampleCheckDate: { $ne: todayCheck }} ]}).limit(config.poolSize);
        let findList = await findResult.toArray();

        await collectionHistory.insertOne(   {date: todayTime, 'total_count' : await findResult.count() } );
        const history_id = await collectionHistory.find().sort( { "_id": -1 } ).limit(1).toArray();

        logger.info('sample fail history parent_id: ' + history_id[0]);
        logger.info('Backup day: ' + backupDay);
        logger.info('Backup day2: ' + backupDay2);
        logger.info('Total count: ' + findResult.count());

        const fileRoot = config.root;

        while( findList.length > 0 ) {
            let datasetList = [];
            let bulk = [];
            let bulk_resHistory = [];

            let sum1=0, sum2=0, sum3=0, sum4=0;

            for(let i=0; i<findList.length; i++) {
                let item = findList[i];
                await datasetList.push({ "dataset_id" : item.dataset_id});

                let col_t_desc_t_cnt = await collection.find({dataset_id : item.dataset_id, asset_type: 'column'}).count();
                let col_t_desc_s_cnt = await collection.find({dataset_id : item.dataset_id, asset_type: 'column' , "desc":{$exists:true}, $nor:[{desc:""},{desc:null}]}).count();
                let col_t_desc_e_cnt = col_t_desc_t_cnt - col_t_desc_s_cnt;
                let private_cnt = await collection.find({dataset_id : item.dataset_id, asset_type: 'column', privateYn :'Y'}).count();
                let confidential_cnt = await collection.find({dataset_id : item.dataset_id, asset_type: 'column', confidentialYn :'Y'}).count();

                sum1 = sum1 + col_t_desc_s_cnt;
                sum2 = sum2 + col_t_desc_e_cnt;
                sum3 = sum3 + private_cnt;
                sum4 = sum4 + confidential_cnt;

                let dataset_name_ck =  await collection.find({dataset_id : item.dataset_id, asset_type: 'table' , $nor:[{dataset_name:""},{dataset_name:null}]}).count();
                let desc_ck =await collection.find({dataset_id : item.dataset_id, asset_type: 'table', $nor:[{desc:""},{desc:null}]}).count();
                let tags_ck =await collection.find({dataset_id : item.dataset_id, asset_type: 'table', $nor:[{tags:""},{tags:null}]}).count();

                await bulk.push({updateOne : {filter: {asset_type:'table', "dataset_id" : item.dataset_id}, update: { $set: { col_t_desc_s_cnt: col_t_desc_s_cnt, col_t_desc_e_cnt : col_t_desc_e_cnt,privateCnt:private_cnt, confidentialCnt:confidential_cnt,
                                dataset_name_ck:dataset_name_ck, desc_ck:desc_ck, tags_ck:tags_ck }} } });
                await bulk_resHistory.push({insertOne :  { dataset_id : item.dataset_id, col_t_desc_s_cnt: col_t_desc_s_cnt, col_t_desc_e_cnt : col_t_desc_e_cnt,privateCnt:private_cnt, confidentialCnt:confidential_cnt,
                            server_id:item.server_id, server_name:item.server_name, instance_id:item.instance_id, instance_name:item.instance_name} });
            }//for
            await collectionHistory.updateOne( {date: todayTime},  { $inc: { col_t_desc_s_cnt: sum1, col_t_desc_e_cnt : sum2, privateCnt:sum3, confidentialCnt:sum4 }} , {upsert: true} );

            let match_filter = { $and: [{asset_type:'column'},{ $or: datasetList}]};
            let findList_result = await collection.aggregate([
                {$match : match_filter }
                ,{$project: {"_id":0, "name":1, "nullable":1, "data_type":1, "dataset_id":1, "desc":1}}
                // ,{$group: {_id:"$dataset_id", name: {$addToSet: "$name"}, nullable: {$addToSet: "$nullable"}, data_type:{$addToSet:"$data_type"}}}
                ]).toArray();

            let f_count = 0, s_count = 0, w_count = 0, e_count = 0;
            let workBook;

            logger.info('Sample validate start');
            for(let i=0; i<datasetList.length; i++) {
                let item = datasetList[i];
                let dirPath = item.dataset_id.split('.');

                const _file =  dirPath[0] + '.' + dirPath[1]  + '.' + dirPath[3] ;
                const fileCheck1 = path.join(fileRoot, dirPath[0], dirPath[1], item.dataset_id + ".xlsx");
                const fileCheck2 = path.join(fileRoot, dirPath[0], dirPath[1], _file + '.xlsx');

                if (await fs.existsSync(fileCheck1) ){
                    workBook = await XLSX.readFile(fileCheck1, {type : 'buffer', cellDates : true , cellNF :  false , cellText : true});
                    logger.info('샘플 파일 경로: ' + fileCheck1);
                }else if (await fs.existsSync(fileCheck2)) {
                    workBook = await XLSX.readFile(fileCheck2, {type : 'buffer', cellDates : true , cellNF :  false , cellText : true});
                    logger.info('샘플 파일 경로: ' + fileCheck2);
                }else{
                    f_count++;
                    logger.error('샘플 파일 존재하지 않습니다: ' + item.dataset_id);
                    await bulk.push({updateOne : {filter: {asset_type:'table', "dataset_id" : item.dataset_id}, update: { $set: { sampleYn: 'N', sampleCheckDate : todayCheck }} } });
                    await bulk_resHistory.push({updateOne : {filter: {"dataset_id" : item.dataset_id}, update :  { $set: {date: todayTime, 'dataset_id' : item.dataset_id, result: 'F', columns : [{column : "" , SampleChecks : [ {returnCode : '7', returnMsg : config.code['7']} ] }]  }}}});
                    continue;
                }

                let grouped = await groupBy(findList_result, colList => colList.dataset_id);
                let result = await sampleValidation(workBook, item.dataset_id, grouped.get(item.dataset_id));


                await bulk.push({updateOne : {filter: {asset_type:'table', "dataset_id" : result[0]}, update: { $set: { sampleYn: 'Y', sampleCheckDate : todayCheck }} } });
                if (result[1]=='0' ){
                    s_count++;
                    await bulk_resHistory.push({updateOne : {filter: {"dataset_id" : item.dataset_id}, update : { $set: {date: todayTime, 'dataset_id' : result[0], result: 'Y', columns : [{column : "" , SampleChecks : [{ returnCode : result[1], returnMsg : config.code[result[1]] }] }]  }}}});
                }else if (result[1]=='1' || result[1]=='2') {
                    e_count++;
                    await bulk_resHistory.push({updateOne : {filter: {"dataset_id" : item.dataset_id}, update :  { $set:{date: todayTime, 'dataset_id' : result[0], result: 'N', columns : [{column : "" , SampleChecks : [{ returnCode : result[1], returnMsg : config.code[result[1]] }] }]  }}}});
                }else if (result[1]=='3') {
                    e_count++;
                    await bulk_resHistory.push({updateOne : {filter: {"dataset_id" : item.dataset_id}, update : { $set: {date: todayTime, 'dataset_id' : result[0], result: 'N', columns : [{column : "" , SampleChecks : [{ returnCode : result[1], returnMsg : result[2] + config.code[result[1]] }] }]  }}}});
                }else {
                    if(result[1] =='W')
                        w_count++;
                    else
                        e_count++;
                    await bulk_resHistory.push({updateOne : {filter: {"dataset_id" : item.dataset_id}, update : { $set: {date: todayTime, 'dataset_id' : result[0], result: result[1], columns: result[2]}  }}});
                }


            }//for

            logger.info('Sample validate done');

            let bulkcount1 = await collection.bulkWrite(bulk);
            let bulkcount2 = await collectionResHistory.bulkWrite(bulk_resHistory);
            logger.info('collection.bulk: ' + bulkcount1.matchedCount);
            logger.info('collectionResHistory.bulk: ' + bulkcount2.insertedCount);

            await collectionHistory.updateOne( {date: todayTime},  { $inc : {'exec_count' : findList.length, 'success_count': s_count, 'error_count':e_count, 'file_notfound':f_count, 'warn_count': w_count} } , {upsert: true} );
            logger.info('Sample check history update!');

            findList = await collection.find({asset_type:'table', status:'검토완료' , $or: [ { sampleCheckDate : null } , { sampleCheckDate: { $ne: todayCheck }} ]}).limit(config.poolSize).toArray();
            logger.info('One cycle done[pool size / findList length]');

            console.log('one cycle');

        }//while

        await collectionHistory.deleteMany( { date : {$lt: backupDay } });
        await collectionResHistory.deleteMany( { date : {$lt: backupDay2 } });

        logger.info('Sample validate done!');
        console.log('DONE!')
        client.close();
    });

});

async function groupBy(list, keyGetter) {
    const map = new Map();
    list.forEach((item) => {
        const key = keyGetter(item);
        const collection = map.get(key);
        if (!collection) {
            map.set(key, [item]);
        } else {
            collection.push(item);
        }
    });
    return map;
}


async function sampleValidation(workBook, dataset_id, col_list) {

    let sheet = workBook.SheetNames[0]; // 배열이므로 .length를 사용하여 갯수 확인가능
    let worksheet = XLSX.utils.sheet_to_json(workBook.Sheets[sheet]);

    if ( worksheet.length == 0 ){
        logger.error('파일이 비어있습니다: ' + dataset_id);
        return [dataset_id, '1'];
    }else{
        let compareCheck = 0;
        let compareVal = '';
        let result = new Array();
        let headers;
        let warnCheck = 'W';
        headers = await get_header_row(workBook.Sheets[sheet]);

        if(headers.length != col_list.length){
            logger.error('영문 컬럼 개수가 다릅니다: ' + dataset_id);
            return [dataset_id, '2'];
        }

        for (let j = 0; j < col_list.length; j++) {
            let itemCol = col_list[j];

            let check = false;
            for (let jj = 0; jj < headers.length; jj++) {
                let colName = headers[jj];

                let SampleCheck = new Array();
                let columns = new Object();

                if (colName.eng == itemCol.name){
                    compareCheck++;
                    check = true;
                    let output = await col_check( workBook.Sheets[sheet], jj, itemCol.nullable, itemCol.data_type );

                    if(!output[0]){
                        SampleCheck.push( { returnCode : '4', returnMsg : 'null 값이 존재합니다'});
                        warnCheck = 'N';
                    }
                    if(!output[1]){
                        SampleCheck.push( { returnCode : '5', returnMsg : '데이터타입이 다릅니다' });
                        warnCheck = 'N';
                    }
                    if(colName.kor == null) {
                        // result.push( { column:colName.eng, returnCode : '6', returnMsg : '한글 컬럼이 존재하지 않습니다', dataset_id:dataset_id });
                        SampleCheck.push({returnCode: '6', returnMsg: '한글 컬럼이 존재하지 않습니다'});
                    }else if(itemCol.desc == null){
                        SampleCheck.push( { returnCode : '8', returnMsg : "한글 컬럼명이 다름" });
                    }else if( itemCol.desc!=null &&  colName.kor!=null && ( colName.kor.replace(" ", "") != itemCol.desc.replace(" ","") ) ){
                        // result.push( { column:colName.eng, returnCode : '6', returnMsg : '한글 컬럼이 존재하지 않습니다', dataset_id:dataset_id });
                        SampleCheck.push( { returnCode : '8', returnMsg : "한글 컬럼명이 다름" });
                    }

                    if(SampleCheck.length > 0){
                        columns.column = colName.eng;
                        columns.SampleChecks = SampleCheck;
                        result.push(columns);
                    }
                    continue;
                }
            }//for jj

            if (compareVal == '' && !check){
                compareVal = '[ ' + itemCol.name + ' ] ';
            }
        }//for j

        if (col_list.length == compareCheck) {
            if(result.length > 0){
                return [dataset_id, warnCheck, result];
            }else{
                logger.info('Sample validate success: ' + dataset_id );
                return [dataset_id, '0'];
            }
        }else {
            logger.error('Col different: ' + dataset_id );
            return [dataset_id, '3' , compareVal];
            // return [{ returnCode : '3', returnMsg : '영문 컬럼명이 다릅니다', dataset_id:dataset_id }];
        }
    }
}

async function col_check(sheet, j, nullable, type) {

    var range = XLSX.utils.decode_range(sheet['!ref']);

    let cell;
    let typeCheck = true, nullCheck = true;

    for( let C = 2; C <= range.e.r; ++C) {
        cell = sheet[XLSX.utils.encode_cell({c: j, r: C})] /* eng row */
        if ( !(cell && cell.t)) { //null
            if(nullable =='N')
                nullCheck = false;
        } else {

            if(!typeCheck)
                continue;

            let v = cell.v.toString();
            let val = _.toNumber(v.replace(/,/g,'')); // str -> number


            if(v == 'null' || v=='NULL')
                continue;

            /////data type check
            // if (type == 'float' || type == 'real') {
            //   typeCheck = __.isFloat(val);
            //   if(isNaN(val)) typeCheck = false;
            if (type == 'numeric' || type == 'NUMERIC' || type == 'decimal'|| type == 'float' || type == 'real') {
                typeCheck = __.isNumeric(val);
                if(isNaN(val)) typeCheck = false;
            }else if ( type == 'tinyint') {
                typeCheck = (val >= 0 && val <= 255) ? true : false;
                if(isNaN(val)) typeCheck = false;
            }else if ( type == 'bigint') {
                typeCheck = (val >= -9223372036854775808 && val <= 9223372036854775807) ? true : false;
                if(isNaN(val)) typeCheck = false;
            }else if ( type == 'smallint') {
                typeCheck = (val >= -32768 && val <= 32767) ? true : false;
                if(isNaN(val)) typeCheck = false;
            }else if ( type == 'int') {
                typeCheck = (val >= -2147483648 && val <= 2147483647) ? true : false;
                if(isNaN(val)) typeCheck = false;
            }else if ( type == 'bit') {
                typeCheck = (val == 0 || val == 1) ? true : false;
                if (isNaN(val)) typeCheck = false;
            }
        }//else

        if(nullCheck ==false && typeCheck==false)
            return [false, false];

    }//for

    return [nullCheck, typeCheck];
}

async function get_header_row(sheet) {

    var range = XLSX.utils.decode_range(sheet['!ref']);
    //var C, R = range.s.r; /* start in the first row */
    /* walk every column in the range */

    let colArr=new Array();
    let C, cellKor,cellEng;
    for( C = range.s.c; C <= range.e.c; ++C) {
        cellEng = sheet[XLSX.utils.encode_cell({c:C, r:1})] /* eng row */
        cellKor = sheet[XLSX.utils.encode_cell({c:C, r:0})] /* kor row */

        let colJson = new Object;
        colJson.eng = (cellEng && cellEng.t)?cellEng.v : null;
        colJson.kor = (cellKor && cellKor.t)?cellKor.v : null;

        if (colJson.eng != null)
            colArr.push(colJson);
    }

    return colArr;
}

module.exports = router;