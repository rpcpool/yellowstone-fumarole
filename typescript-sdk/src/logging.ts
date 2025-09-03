import winston from "winston";

export let LOGGER: winston.Logger = winston.createLogger({
  level: process.env.FUMAROLE_CLIENT_LOG_LEVEL || "silent",
  format: winston.format.cli(),
  transports: [new winston.transports.Console()],
});

function getDefaultFumaroleLogger() {
  return {
    level: process.env.FUMAROLE_CLIENT_LOG_LEVEL || "error",
    format: winston.format.json(),
    transports: [new winston.transports.Console()],
  };
}

/**
 * Sets a custom global logger for the Fumarole library context.
 *
 * @param customLogger The custom {@link winston.Logger} instance to use.
 */
export function setCustomFumaroleLogger(customLogger: winston.Logger) {
  LOGGER = customLogger;
}

/**
 * Sets the default global logger for the Fumarole library context.
 */
export function setDefaultFumaroleLogger() {
  const spec = getDefaultFumaroleLogger();
  LOGGER = winston.createLogger(spec);
}
