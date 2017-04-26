using System.Linq;
using System.Collections;
using System.Globalization;

using Newtonsoft.Json.Serialization;

namespace Hangfire.AzureDocumentDB.Json
{
    internal class LowerCaseDelimitedPropertyNamesContractResovler : DefaultContractResolver
    {
        private readonly char _delimiter;

        public LowerCaseDelimitedPropertyNamesContractResovler() : this('_') { }

        private LowerCaseDelimitedPropertyNamesContractResovler(char delimiter)
        {
            _delimiter = delimiter;
        }

        protected override string ResolvePropertyName(string propertyName)
        {
            string camelCaseString = ToCamelCase(propertyName);
            return new string(InsertDelimiterBeforeCaps(camelCaseString, _delimiter).OfType<char>().ToArray());
        }

        private static IEnumerable InsertDelimiterBeforeCaps(IEnumerable input, char delimiter)
        {
            bool lastCharWasUppper = false;
            foreach (char c in input)
            {
                if (char.IsUpper(c))
                {
                    if (!lastCharWasUppper)
                    {
                        yield return delimiter;
                        lastCharWasUppper = true;
                    }
                    yield return char.ToLower(c);
                    continue;
                }
                yield return c;
                lastCharWasUppper = false;
            }
        }

        private static string ToCamelCase(string s)
        {
            if (string.IsNullOrEmpty(s))
                return s;

            if (!char.IsUpper(s[0]))
                return s;

            string camelCase = char.ToLower(s[0], CultureInfo.InvariantCulture).ToString(CultureInfo.InvariantCulture);
            if (s.Length > 1)
                camelCase += s.Substring(1);

            return camelCase;
        }

    }
}