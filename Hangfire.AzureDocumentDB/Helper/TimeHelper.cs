using System;
using System.Globalization;

namespace Hangfire.Azure.Documents.Helper
{
    internal static class TimeHelper
    {
        private static readonly DateTime epochDateTime = new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc);

        public static int ToEpoch(this DateTime date)
        {
            if (date.Equals(DateTime.MinValue)) return int.MinValue;
            TimeSpan epochTimeSpan = date - epochDateTime;
            return (int)epochTimeSpan.TotalSeconds;
        }

        public static DateTime ToDateTime(this int totalSeconds) => epochDateTime.AddSeconds(totalSeconds);

        public static string ToEpoch(this string s)
        {
            return DateTime.TryParse(s, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out DateTime date)
                ? date.ToEpoch().ToString(CultureInfo.InvariantCulture)
                : s;
        }
    }
}