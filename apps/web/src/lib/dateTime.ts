const POSTGRES_TIMESTAMP_PATTERN =
  /^(\d{4}-\d{2}-\d{2}) (\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?)([+-]\d{2})(?::?(\d{2}))?$/;
const UTC_TIMESTAMP_PATTERN =
  /^(\d{4}-\d{2}-\d{2}) (\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?) UTC$/i;

export function normalizeDateTimeInput(value: string): string {
  const trimmed = value.trim();
  const postgresMatch = POSTGRES_TIMESTAMP_PATTERN.exec(trimmed);
  if (postgresMatch) {
    const [, date, time, hourOffset, minuteOffset = "00"] = postgresMatch;
    return `${date}T${time}${hourOffset}:${minuteOffset}`;
  }

  const utcMatch = UTC_TIMESTAMP_PATTERN.exec(trimmed);
  if (utcMatch) {
    const [, date, time] = utcMatch;
    return `${date}T${time}Z`;
  }

  return trimmed;
}

export function parseDateTime(value: string | null | undefined): Date | null {
  if (typeof value !== "string" || value.trim().length === 0) {
    return null;
  }
  const parsed = new Date(normalizeDateTimeInput(value));
  return Number.isNaN(parsed.getTime()) ? null : parsed;
}

export function formatDateTime(
  value: string | null | undefined,
  locale: string,
  options: Intl.DateTimeFormatOptions,
): string {
  return parseDateTime(value)?.toLocaleString(locale, options) ?? "-";
}
