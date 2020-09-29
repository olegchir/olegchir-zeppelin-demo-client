import {GlobalSettings} from "./GlobalSettings";

export class Logger {
    public static debug(message:any) {
        if (GlobalSettings.CONFIG.LOG_LEVEL == "debug") {
            console.debug(message);
        }
    }

    public static direct(message:any) {
        console.debug(message);
    }
}
