var express = require('express');
var router = express.Router();
const MongoClient = require('mongodb').MongoClient;
const config = require('../config.json');
const XLSX = require('xlsx');
const fs = require('fs');
const moment = require('moment-timezone');
const log4js = require('log4js');
const logger = log4js.getLogger("Samplecheck");

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
        const todayCheck = moment().tz('Asia/Seoul').format("YYYY-MM-DD");

        let backupDay;
        backupDay = moment(todayCheck).subtract(backupValue, backupUnit).tz('Asia/Seoul').format("YYYY-MM-DD");

        const db = client.db(config.db);
        const collection = db.collection(config.collection);

        collection.updateMany( {}, {$rename:{"sampleYn":"SampleYn"}})

        await db.createCollection('samplehistory').then(value => {
        }).catch(error => {});

        const collectionHistory = db.collection('samplehistory');
        let findResult = await collection.find({asset_type:'table', status:'검토완료',  $or: [ { SampleCheckDate : null } , { SampleCheckDate: { $ne: todayCheck }} ]}).limit(config.poolSize);
        let findList = await findResult.toArray();

        await collectionHistory.deleteMany( { date : {$lt: backupDay } });
        logger.info('Backup day: ' + backupDay);
        logger.info('Total count: ' + findResult.count());

        if (await collectionHistory.find({date:todayCheck, $or: [{total_count:0},{total_count: null}]}).count()< 1)
            await collectionHistory.insertOne( {date: todayCheck, 'total_count' : await findResult.count() } );

        while( findList.length > 0 ){

            logger.info('FindList length: ' + findList.length);
            let s_count = 0;
            let f_count = 0;
            for(let i=0; i<findList.length; i++){
                let item = findList[i];

                let col_list = await collection.find({asset_type:'column', "dataset_id" : item.dataset_id }).toArray();
                let result = await col_compare(item.dataset_id, col_list);

                if (result.successBool){
                    s_count++;
                    await collection.updateMany({asset_type:'table', "dataset_id" : item.dataset_id},{$set : { SampleYn: 'Y', SampleCheckDate : todayCheck, SampleCheckCode : result.returnCode ,SampleCheckMsg : config.code[result.returnCode] }} , {upsert:true ,multi: true});
                }else{
                    f_count++;
                    await collection.updateMany({asset_type:'table', "dataset_id" : item.dataset_id},{$set : { SampleYn: 'N', SampleCheckDate : todayCheck, SampleCheckCode : result.returnCode ,SampleCheckMsg : config.code[result.returnCode] }} , {upsert:true ,multi: true});
                }

            }//for i

            await collectionHistory.updateMany({date: todayCheck},  { $inc : {'exec_count' : findList.length, 'success_count': s_count, 'fail_count':f_count} } , {upsert: true} );
            logger.info('Sample check history update!');

            findList = await collection.find({asset_type:'table', status:'검토완료' , $or: [ { SampleCheckDate : null } , { SampleCheckDate: { $ne: todayCheck }} ]}).limit(config.poolSize).toArray();
            logger.info('One cycle done[pool size / findList length]');

        }//while

        logger.info('Sample validate done!');
        console.log('DONE!')
        client.close();
    });

});

async function col_compare(dataset_id, col_list) {

    let dirPath = dataset_id.split('.');
    let workBook ;

    const _file =  dirPath[0] + '.' + dirPath[1]  + '.' + dirPath[3] ;
    const fileCheck1 = 'C:\\sample\\' + dirPath[0] + '\\' + dirPath[1] + '\\' + dataset_id + ".xlsx";
    const fileCheck2 = 'C:\\sample\\' + dirPath[0] + '\\' + dirPath[1] + '\\' + _file + '.xlsx' ;


    if (await fs.existsSync(fileCheck1) ){
        workBook = await XLSX.readFile(fileCheck1, {type : 'buffer', cellDates : true , cellNF :  false , cellText : true});
        logger.info('Sample file path: ' + fileCheck1);
    }else if (await fs.existsSync(fileCheck2)) {
        workBook = await XLSX.readFile(fileCheck2 , {type : 'buffer', cellDates : true , cellNF :  false , cellText : true});
        logger.info('Sample file path: ' + fileCheck2);
    }else{
        logger.error('Sample file not found: ' + dataset_id);
        return { returnCode : '1', successBool : false }
    }

    let sheet = workBook.SheetNames[0]; // 배열이므로 .length를 사용하여 갯수 확인가능
    let worksheet = XLSX.utils.sheet_to_json(workBook.Sheets[sheet]);

    if ( worksheet.length == 0 ){
        logger.error('Sample file empty: ' + dataset_id);
        return { returnCode : '2', successBool : false }
    }else{
        const headers = await get_header_row(workBook.Sheets[sheet]);
        if (headers.includes('UNKNOWN')) {
            logger.error('korean row error: ' + dataset_id);
            return { returnCode : '2', successBool : false }
        }
    }


    if( Object.keys(worksheet[0]).length < col_list.length) {
        logger.error('Doc col more: ' + dataset_id);
        return { returnCode : '3', successBool : false }
    }else if( Object.keys(worksheet[0]).length > col_list.length) {
        logger.error('Sample col more: ' + dataset_id);
        return { returnCode : '4', successBool : false }
    }else {//sample.length == doc.length
        let compareCheck = 0;
        for (let j = 0; j < col_list.length; j++) {
            let itemCol = col_list[j];

            for (let key in worksheet[0]) {
                if (worksheet[0][key].toLowerCase() == itemCol.name.toLowerCase()) {
                    compareCheck++;
                    continue;
                }
            }
        }//for j

        if (col_list.length == compareCheck) {
            logger.info('Sample validate success: ' + dataset_id );
            return { returnCode : '0', successBool : true }
        }else {
            logger.error('Col different: ' + dataset_id );
            return { returnCode : '5', successBool : false }
        }
    }//els

}
async function get_header_row(sheet) {
    var headers = [];
    var range = XLSX.utils.decode_range(sheet['!ref']);
    //var C, R = range.s.r; /* start in the first row */
    /* walk every column in the range */
    let check;
    let C, cell, hdr;
    for( C = range.s.c; C <= range.e.c; ++C) {
        cell = sheet[XLSX.utils.encode_cell({c:C, r:1})] /* find the cell in the first row */
        hdr = "UNKNOWN"; // <-- replace with your desired default
        if(cell && cell.t) hdr = XLSX.utils.format_cell(cell);
        check = C;
        if(hdr == "UNKNOWN")
            break;
    }

    for(C = range.s.c; C < check; ++C) {
        cell = sheet[XLSX.utils.encode_cell({c:C, r:0})] /* find the cell in the first row */
        hdr = "UNKNOWN"; // <-- replace with your desired default
        if(cell && cell.t) hdr = XLSX.utils.format_cell(cell);
        headers.push(hdr);
    }
    return headers;
}


module.exports = router;