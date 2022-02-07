using System;
using System.Security.Cryptography;
using System.Text;

namespace Hangfire.Azure.Helper;

internal static class HashHelper
{
	internal static string GenerateHash(this string input)
	{
		if (string.IsNullOrWhiteSpace(input)) throw new ArgumentNullException(nameof(input));

		using MD5 md5 = MD5.Create();
		byte[] inputBytes = Encoding.ASCII.GetBytes(input);
		byte[] hashBytes = md5.ComputeHash(inputBytes);
		return BitConverter.ToString(hashBytes).Replace("-", string.Empty);
	}
}