import winston from "winston";


export let LOGGER: winston.Logger = winston.createLogger({
    level: process.env.FUMAROLE_CLIENT_LOG_LEVEL || 'silent',
    format: winston.format.cli(),
    transports: [new winston.transports.Console()],
});


function getDefaultLogger() {
    return {
        level: process.env.FUMAROLE_CLIENT_LOG_LEVEL || 'error',
        format: winston.format.cli(),
        transports: [new winston.transports.Console()],
    }
}


export function setCustomLogger(customLogger: winston.Logger) {
    LOGGER = customLogger;
}

export function setDefaultLogger() {
    const spec = getDefaultLogger();
    LOGGER = winston.createLogger(spec);
}