/**
 * @file Date and timezone utilities for BOM river height data.
 *
 * BOM river height HTML files contain two types of date information:
 * 1. A full "Issued at" timestamp in the header (e.g., "03:24 PM ACST Tuesday 20 January 2026").
 * 2. Relative "Time/Day" timestamps in table rows (e.g., "03.10PM Tue").
 *
 * These utilities handle:
 * - Mapping Australian timezone abbreviations (ACST, AEDT, etc.) to UTC offsets.
 * - Parsing the "Issued at" header to establish a base date.
 * - Calculating absolute dates for table rows by finding the most recent occurrence of a day name
 *   on or before the "Issued at" date, handling month and year rollovers.
 * - Formatting components into standard ISO 8601 strings while preserving local offsets.
 */

import { parse, isValid } from "date-fns";

const timezones = {
  AWST: {
    long_name: "Australian Western Standard Time",
    offset: "UTC+08:00",
    dst_observed: false,
    territories: ["WA"],
  },
  ACWST: {
    long_name: "Australian Central Western Standard Time",
    offset: "UTC+08:45",
    dst_observed: false,
    territories: ["South-eastern WA", "Border Village, SA"],
  },
  ACST: {
    long_name: "Australian Central Standard Time",
    offset: "UTC+09:30",
    dst_observed: false,
    territories: ["NT"],
  },
  ACDT: {
    long_name: "Australian Central Daylight Time",
    standard_offset: "UTC+09:30",
    dst_offset: "UTC+10:30",
    dst_observed: true,
    territories: ["SA", "Broken Hill"],
  },
  AEST: {
    long_name: "Australian Eastern Standard Time",
    offset: "UTC+10:00",
    dst_observed: false,
    territories: ["QLD"],
  },
  AEDT: {
    long_name: "Australian Eastern Daylight Time",
    standard_offset: "UTC+10:00",
    dst_offset: "UTC+11:00",
    dst_observed: true,
    territories: ["NSW", "TAS", "VIC", "ACT"],
  },
};

const DAYS = {
  Sun: 0,
  Mon: 1,
  Tue: 2,
  Wed: 3,
  Thu: 4,
  Fri: 5,
  Sat: 6,
};

/**
 * Maps Australian timezone abbreviations to UTC offsets.
 * @param {string} tzAbbr - The timezone abbreviation (e.g., "ACST").
 * @returns {Promise<string>} - The UTC offset (e.g., "+09:30").
 */
export async function getTimezoneOffset(tzAbbr) {
  const tz = timezones[tzAbbr];

  if (!tz) {
    throw new Error(`Unknown timezone abbreviation: ${tzAbbr}`);
  }

  const offsetStr = tz.dst_offset || tz.offset;
  return offsetStr.replace("UTC", "");
}

/**
 * Formats a date and time into an ISO 8601 string with offset.
 * @param {number} year
 * @param {number} month
 * @param {number} day
 * @param {string} timeStr - Time in "HH:mm AM/PM" or "HH.mmAM/PM" format.
 * @param {string} offset - UTC offset (e.g., "+09:30").
 * @returns {string} - ISO 8601 string.
 */
export function formatIsoDate(year, month, day, timeStr, offset) {
  const match = timeStr.match(/(\d{1,2})[:.](\d{2})\s*([AP]M)/i);
  if (!match) {
    throw new Error(`Invalid time format: ${timeStr}`);
  }

  let [_, hours, minutes, ampm] = match;
  let h = parseInt(hours, 10);
  const m = parseInt(minutes, 10);

  if (ampm.toUpperCase() === "PM" && h < 12) {
    h += 12;
  } else if (ampm.toUpperCase() === "AM" && h === 12) {
    h = 0;
  }

  const yyyy = year.toString().padStart(4, "0");
  const mm = month.toString().padStart(2, "0");
  const dd = day.toString().padStart(2, "0");
  const hh = h.toString().padStart(2, "0");
  const min = m.toString().padStart(2, "0");

  return `${yyyy}-${mm}-${dd}T${hh}:${min}:00${offset}`;
}

/**
 * Parses the "Issued at" string from a BOM HTML file.
 * @param {string} text - The "Issued at" text.
 * @returns {Promise<Object>} - Parsed date components and ISO string.
 */
export async function parseIssuedAt(text) {
  // Example: "Issued at 03:24 PM ACST Tuesday 20 January 2026"
  const regex = /Issued at\s+(.+?)\s+([A-Z]{3,5})\s+(.+)$/i;
  const match = text.match(regex);

  if (!match) {
    throw new Error(`Could not parse "Issued at" string: ${text}`);
  }

  const [_, timeStr, tzAbbr, dateStr] = match;
  const offset = await getTimezoneOffset(tzAbbr);

  // dateStr example: "Tuesday 20 January 2026"
  const baseDate = parse(dateStr, "EEEE dd MMMM yyyy", new Date());
  if (!isValid(baseDate)) {
    throw new Error(`Could not parse date part of "Issued at": ${dateStr}`);
  }

  const year = baseDate.getFullYear();
  const month = baseDate.getMonth() + 1;
  const day = baseDate.getDate();

  const iso = formatIsoDate(year, month, day, timeStr, offset);

  return {
    year,
    month,
    day,
    time: timeStr,
    offset,
    iso,
  };
}

/**
 * Calculates the absolute date for a row based on the "Issued at" date and row day abbreviation.
 * @param {Object} baseDate - The parsed "Issued at" date components { year, month, day }.
 * @param {string} dayAbbr - The day abbreviation from the row (e.g., "Tue").
 * @returns {Object} - { year, month, day }.
 */
export function calculateRowDate(baseDate, dayAbbr) {
  const targetDay = DAYS[dayAbbr];
  if (targetDay === undefined) {
    throw new Error(`Unknown day abbreviation: ${dayAbbr}`);
  }

  // Use UTC to avoid local DST issues during day calculation
  const date = new Date(
    Date.UTC(baseDate.year, baseDate.month - 1, baseDate.day),
  );
  const currentDay = date.getUTCDay();

  // Find the most recent occurrence of targetDay on or before baseDate
  const diff = (currentDay - targetDay + 7) % 7;
  date.setUTCDate(date.getUTCDate() - diff);

  return {
    year: date.getUTCFullYear(),
    month: date.getUTCMonth() + 1,
    day: date.getUTCDate(),
  };
}
