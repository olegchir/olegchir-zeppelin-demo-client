import {GlobalSettings} from './GlobalSettings';
import {Logger} from './Logger';
import axios from 'axios';

function applyGlobalConfig(minv: any) {
    GlobalSettings.CONFIG.LOG_LEVEL = minv["log-level"];
}

async function main() {
    const argv = process.argv;
    const minv = require('minimist')(argv.slice(2));
    applyGlobalConfig(minv);

    await job(minv);
}

async function job(minv: object) {
    const [Z_URL] = minv.['_'];
    const NOTE_LIST_URL = `${Z_URL}/api/notebook`;

    const notes = await axios.get(NOTE_LIST_URL);
    let noteId: string = null;
    for (let item: object of notes.data.body) {
        if ( item.path.indexOf('Spark Basic Features') >= 0 ) {
            noteId = item.id;
            break;
        }
    }

    const NOTE_INFO_URL = `${Z_URL}/api/notebook/${noteId}`;
    const ndata = (await axios.get(NOTE_INFO_URL)).data.body;


    Logger.direct(`NOTEBOOK`);
    Logger.direct(`=========`);
    Logger.direct(`"${ndata.name}" (${ndata.path})`);


    const PAR_TEXT = `%spark
    import org.jetbrains.ztools.spark.Tools
    Tools.init($intp, 3, true)
    println(Tools.getEnv.toJsonObject.toString)
    println("----")
    println(Tools.getCatalogProvider("spark").toJson)`;

    const CREATE_PAR_URL = `${Z_URL}/api/notebook/${noteId}/paragraph`;
    const par: object = await axios.post(CREATE_PAR_URL, {
        title: 'temp',
        text: PAR_TEXT,
        index: 0
    });
    Logger.debug('Paragraph created');

    const parId = par.data.body;
    Logger.debug(`Paragraph ID: ${parId}`);

    const RUN_PAR_URL = `${Z_URL}/api/notebook/run/${noteId}/${parId}`;
    await axios.post(RUN_PAR_URL);
    Logger.debug(`Paragraph executed`);

    const INFO_PAR_URL = `${Z_URL}/api/notebook/${noteId}/paragraph/${parId}`;
    const { data } = await axios.get(INFO_PAR_URL);

    const DEL_PAR_URL = `${Z_URL}/api/notebook/${noteId}/paragraph/${parId}`;
    await axios.delete(DEL_PAR_URL);

    const [varInfoData, dbInfoData] = (data.body.results.msg[0].data)
        .replace('\nimport org.jetbrains.ztools.spark.Tools\n', '')
        .split('\n----\n');
    const varInfo = JSON.parse(varInfoData);
    const dbInfo = JSON.parse(dbInfoData);

    Logger.direct(`\nVARIABLES`);
    Logger.direct(`=========`);
    for (const [key, {type}] of Object.entries(varInfo)) {
        Logger.direct(`${key} : ${type}`);
    }

    Logger.direct(`\nDATABASES`);
    Logger.direct(`=========`);
    for (const [key, database] of Object.entries(dbInfo.databases)) {
        Logger.direct(`Database: ${database.name} (${database.description})`);
        for (const table of database.tables) {
            const columnsJoined = table.columns.map(val => `${val.name}/${val.dataType}`).join(', ');
            Logger.direct(`${table.name} : [${columnsJoined}]`);
        }
    }
}



let asyncMain = async () => {
    try {
        await main();
        Logger.debug("Main function finished");
    } catch (e) {
        console.error(e);
        process.exit(1);
    }
    Logger.debug("Async try finished");
};

Logger.debug("Starting async main function")
asyncMain();
Logger.debug("First line of the log")
