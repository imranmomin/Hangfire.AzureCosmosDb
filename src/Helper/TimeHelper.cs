﻿using System;
using System.Globalization;

// ReSharper disable once CheckNamespace
namespace Hangfire.Azure.Documents.Helper;

internal static class TimeHelper
{
    private static readonly DateTime epochDateTime = new(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc);

    internal static int ToEpoch(this DateTime date)
    {
        if (date.Equals(DateTime.MinValue))
        {
            return int.MinValue;
        }

        TimeSpan epochTimeSpan = date - epochDateTime;
        return(int)epochTimeSpan.TotalSeconds;
    }

    internal static DateTime ToDateTime(this int totalSeconds) => epochDateTime.AddSeconds(totalSeconds);

    internal static string? TryParseToEpoch(this string? s)
    {
        if (s == null)
        {
            return null;
        }

        if (string.IsNullOrWhiteSpace(s))
        {
            return null;
        }

        return DateTime.TryParse(s, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out DateTime date)
            ? date.ToEpoch().ToString(CultureInfo.InvariantCulture)
            : s;
    }

    internal static bool TryParseEpochToDate(this string? s, out string? value)
    {
        value = null;

        if (s == null)
        {
            return false;
        }

        if (string.IsNullOrWhiteSpace(s))
        {
            return false;
        }

        if (!int.TryParse(s, out int epoch))
        {
            return false;
        }

        value = epoch.ToDateTime().ToLocalTime().ToString("d/M/yyyy HH:mm:ss");
        return true;
    }
}