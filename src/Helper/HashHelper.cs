using System;
using System.Text;

namespace Hangfire.Azure.Helper;

public static class HashHelper
{
    public static string GenerateHash(this string input)
    {
        if (string.IsNullOrWhiteSpace(input)) throw new ArgumentNullException(nameof(input));

        using System.Security.Cryptography.MD5 md5 = System.Security.Cryptography.MD5.Create();
        byte[] inputBytes = Encoding.ASCII.GetBytes(input);
        byte[] hashBytes = md5.ComputeHash(inputBytes);
        return BitConverter.ToString(hashBytes).Replace("-", string.Empty);
    }
}