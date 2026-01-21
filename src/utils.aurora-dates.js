/**
 * Resolves a local time string (e.g., "10:22 am") to an ISO 8601 string
 * using the date and timezone from a reference ISO string.
 *
 * @param {string} localTimeStr - e.g., "10:22 am"
 * @param {string} referenceIso - e.g., "2025-12-18T17:30:00+11:00"
 * @returns {string} - ISO 8601 string with local offset preserved
 */
export function resolveLocalTimeToUtc(localTimeStr, referenceIso) {
  if (!localTimeStr || !referenceIso) {
    return null;
  }

  // 1. Extract the date part and the timezone offset from the reference ISO string
  // Example: "2025-12-18T17:30:00+11:00" -> date: "2025-12-18", offset: "+11:00"
  const isoMatch = referenceIso.match(
    /^(\d{4}-\d{2}-\d{2})T\d{2}:\d{2}:\d{2}([+-]\d{2}:?\d{2}|Z)$/,
  );
  if (!isoMatch) {
    throw new Error(`Invalid reference ISO string: ${referenceIso}`);
  }

  const datePart = isoMatch[1];
  const offsetPart = isoMatch[2] === "Z" ? "+00:00" : isoMatch[2];

  // 2. Parse the local time string
  // Example: "10:22 am"
  const timeMatch = localTimeStr.match(/^(\d{1,2}):(\d{2})\s*(am|pm)$/i);
  if (!timeMatch) {
    throw new Error(`Invalid local time format: ${localTimeStr}`);
  }

  let [_, hours, minutes, ampm] = timeMatch;
  let h = parseInt(hours, 10);
  const m = parseInt(minutes, 10);

  if (ampm.toLowerCase() === "pm" && h < 12) {
    h += 12;
  } else if (ampm.toLowerCase() === "am" && h === 12) {
    h = 0;
  }

  const hh = h.toString().padStart(2, "0");
  const mm = m.toString().padStart(2, "0");

  // 3. Combine into a new ISO string with the same date and offset
  return `${datePart}T${hh}:${mm}:00${offsetPart}`;
}
