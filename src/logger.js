import winston from "winston";

/**
 * Custom format for the logger to standardize message structure.
 * It outputs: [LEVEL] MESSAGE
 */
const customFormat = winston.format.printf(({ level, message }) => {
  return `[${level.toUpperCase()}] ${message}`;
});

/**
 * Create the Winston logger instance.
 * Configured to be silent during tests based on NODE_ENV.
 */
const logger = winston.createLogger({
  level: "info",
  format: winston.format.combine(winston.format.splat(), customFormat),
  transports: [
    new winston.transports.Console({
      // Silence the logger if we are running tests
      silent: process.env.NODE_ENV === "test",
    }),
  ],
});

export default logger;
