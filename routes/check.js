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

        await db.createCollection('samplefail_history').then(value => {}).catch(error => {});
        const collectionFailHistory = db.collection('samplefail_history');

        const todayCheck = moment().tz('Asia/Seoul').format("YYYY-MM-DD");
        const backupDay = moment(todayCheck).subtract(backupValue, backupUnit).tz('Asia/Seoul').format("YYYY-MM-DD");
        const todayTime = moment().tz('Asia/Seoul').format("YYYYMMDDhhmmss");

        collection.updateMany( {}, {$rename:{"sampleYn":"SampleYn"}})
        let findResult = await collection.find({asset_type:'table', status:'검토완료',  $or: [ { SampleCheckDate : null } , { SampleCheckDate: { $ne: todayCheck }} ]}).limit(config.poolSize);
        let findList = await findResult.toArray();

        await collectionHistory.insertOne(   {date: todayTime, 'total_count' : await findResult.count() } );
        const history_id = await collectionHistory.find().sort( { "_id": -1 } ).limit(1).toArray();


        logger.info('Backup day: ' + backupDay);
        logger.info('Total count: ' + findResult.count());

        while( findList.length > 0 ) {
            let datasetList = [];
            for(let i=0; i<findList.length; i++) {
                let item = findList[i];
                await datasetList.push({ "dataset_id" : item.dataset_id});
            }

            let match_filter = { $and: [{asset_type:'column'},{ $or: datasetList}]};

            let findList_result = await collection.aggregate([
                {$match : match_filter }
                ,{$project: {"_id":0, "name":1, "nullable":1, "data_type":1, "dataset_id":1}}
                // ,{$group: {_id:"$dataset_id", name: {$addToSet: "$name"}, nullable: {$addToSet: "$nullable"}, data_type:{$addToSet:"$data_type"}}}
                ]).toArray();

            let f_count = 0, s_count = 0;
            let bulk = [];
            let bulk_failHistory = [];
            let workBook;

            logger.info('Sample validate start');
            for(let i=0; i<datasetList.length; i++) {
                let item = datasetList[i];
                let dirPath = item.dataset_id.split('.');

                const _file =  dirPath[0] + '.' + dirPath[1]  + '.' + dirPath[3] ;
                const fileCheck1 = path.join('C:\\sample\\', dirPath[0], dirPath[1], item.dataset_id + ".xlsx");
                const fileCheck2 = path.join('C:\\sample\\', dirPath[0], dirPath[1], _file + '.xlsx');

                if (await fs.existsSync(fileCheck1) ){
                    workBook = await XLSX.readFile(fileCheck1, {type : 'buffer', cellDates : true , cellNF :  false , cellText : true});
                    logger.info('샘플 파일 경로: ' + fileCheck1);
                }else if (await fs.existsSync(fileCheck2)) {
                    workBook = await XLSX.readFile(fileCheck2, {type : 'buffer', cellDates : true , cellNF :  false , cellText : true});
                    logger.info('샘플 파일 경로: ' + fileCheck2);
                }else{
                    f_count++;
                    logger.error('샘플 파일 존재하지 않습니다: ' + item.dataset_id);
                    await bulk.push({updateOne : {filter: {asset_type:'table', "dataset_id" : item.dataset_id}, update: { $set: { SampleYn: 'N', SampleCheckDate : todayCheck }} } });
                    await bulk_failHistory.push({insertOne : {date: todayTime, parent_id: history_id[0]._id, 'dataset_id' : item.dataset_id, SampleCheckCode : '7' ,SampleCheckMsg : '샘플 파일 존재하지 않습니다'}  });
                    continue;
                }

                let grouped = await groupBy(findList_result, colList => colList.dataset_id);
                let result = await sampleValidation(workBook, item.dataset_id, grouped.get(item.dataset_id));

                if (result[0].returnCode=='0' ){
                    s_count++;
                    await bulk.push({updateOne : {filter: {asset_type:'table', "dataset_id" : result[0].dataset_id}, update: { $set: { SampleYn: 'Y', SampleCheckDate : todayCheck }} } });
                    await bulk_failHistory.push({insertOne : {date: todayTime, parent_id: history_id[0]._id, 'dataset_id' : result[0].dataset_id, SampleCheckCode : result[0].returnCode ,SampleCheckMsg : config.code[result[0].returnCode]}  });
                }else{
                    f_count++;
                    await bulk.push({updateOne : {filter: {asset_type:'table', "dataset_id" : result[0].dataset_id}, update: { $set: { SampleYn: 'N', SampleCheckDate : todayCheck }} } });

                    if (result[0].returnCode=='1' || result[0].returnCode=='2' || result[0].returnCode=='3'){
                        await bulk_failHistory.push({insertOne : {date: todayTime, parent_id: history_id[0]._id, 'dataset_id' : result[0].dataset_id, SampleCheckCode : result[0].returnCode ,SampleCheckMsg : config.code[result[0].returnCode]}  });
                    }else{
                        for(let jj = 0; jj< result.length; jj++){
                            let colResult = result[jj];
                            await bulk_failHistory.push({insertOne : {date: todayTime, parent_id: history_id[0]._id, 'dataset_id' : colResult.dataset_id, column: colResult.column, SampleCheckCode : colResult.returnCode ,SampleCheckMsg : config.code[colResult.returnCode]}  });
                        }
                    }
                }

            }//for

            logger.info('Sample validate done');

            await collection.bulkWrite(bulk);
            await collectionFailHistory.bulkWrite(bulk_failHistory);
            logger.info('bulk done');

            await collectionHistory.updateOne( {date: todayTime},  { $inc : {'exec_count' : findList.length, 'success_count': s_count, 'fail_count':f_count} } , {upsert: true} );
            logger.info('Sample check history update!');

            findList = await collection.find({asset_type:'table', status:'검토완료' , $or: [ { SampleCheckDate : null } , { SampleCheckDate: { $ne: todayCheck }} ]}).limit(config.poolSize).toArray();
            logger.info('One cycle done[pool size / findList length]');

        }//while

        await collectionHistory.deleteMany( { date : {$lt: backupDay } });
        await collectionFailHistory.deleteMany( { parent_id : {$ne: history_id } });


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
        return [{ returnCode : '1', returnMsg : '파일이 비어있습니다', dataset_id:dataset_id }];
    }else{
        if( Object.keys(worksheet[0]).length != col_list.length) {
            logger.error('영문 컬럼 개수가 다릅니다: ' + dataset_id);
            return [{ returnCode : '2', returnMsg : '영문 컬럼 개수가 다릅니다', dataset_id:dataset_id }];
        }else{

            let compareCheck = 0;
            let result = new Array();
            let headers;
            headers = await get_header_row(workBook.Sheets[sheet]);

            for (let j = 0; j < col_list.length; j++) {
                let itemCol = col_list[j];

                for (let jj = 0; jj < headers.length; jj++) {
                    let colName = headers[jj];

                    if (colName.eng == itemCol.name){
                        let output = await col_check( workBook.Sheets[sheet], jj, itemCol.nullable, itemCol.data_type );

                        if(!output[0]){
                            result.push( { column:colName.eng, returnCode : '4', returnMsg : 'null 값이 존재합니다', dataset_id:dataset_id});
                        }
                        if(!output[1]){
                            result.push( { column:colName.eng, returnCode : '5', returnMsg : '데이터타입이 다릅니다', dataset_id:dataset_id });
                        }
                        if(colName.kor == null){
                            result.push( { column:colName.eng, returnCode : '6', returnMsg : '한글 컬럼이 존재하지 않습니다', dataset_id:dataset_id });
                        }
                        continue;
                    }
                }//for jj

                for (let key in worksheet[0]){
                    if ( worksheet[0][key].toString().toLowerCase() == itemCol.name.toLowerCase() ) {
                        compareCheck++;
                        continue;
                    }
                }//for key

            }//for j

            if (col_list.length == compareCheck) {
                if(result.length > 0){
                    return result;
                }else{
                    logger.info('Sample validate success: ' + dataset_id );
                    return [{ returnCode : '0', returnMsg : '샘플 파일 검증 성공', dataset_id:dataset_id }];
                }
            }else {
                logger.error('Col different: ' + dataset_id );
                return [{ returnCode : '3', returnMsg : '영문 컬럼명이 다릅니다', dataset_id:dataset_id }];
            }

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
        colArr.push(colJson);
    }

    return colArr;
}

module.exports = router;