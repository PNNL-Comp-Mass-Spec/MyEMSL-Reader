using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Jayrock.Json;

namespace PacificaUnitTests
{
    public static class JsonCompare
    {
        public static void CompareDictionaryLists(List<Dictionary<string, object>> data, List<Dictionary<string, object>> data2, bool muteMinorDifferences = false)
        {
            if (data.Count != data2.Count)
            {
                Console.WriteLine("entry count mismatch!");
                return;
            }

            for (var i = 0; i < data.Count; i++)
            {
                CompareDictionaries(data[i], data2[i], muteMinorDifferences);
            }
        }

        public static void CompareDictionaries(Dictionary<string, object> data, Dictionary<string, object> data2, bool muteMinorDifferences = false)
        {
            foreach (var entry in data)
            {
                if (!data2.TryGetValue(entry.Key, out var value2))
                {
                    Console.WriteLine("Mismatch: key not in data2: {0}", entry.Key);
                }
                else if (entry.Value == null && value2 == null)
                {
                    continue;
                }
                else if (entry.Value == null || value2 == null)
                {
                    if (!muteMinorDifferences)
                    {
                        Console.WriteLine("{0}: Null value mismatch: '{1}' vs. '{2}'", entry.Key, entry.Value ?? "null", value2 ?? "null");
                    }
                }
                else if (entry.Value is IEnumerable ie && value2 is IEnumerable ie2 && !ie.Cast<object>().Any() && !ie2.Cast<object>().Any())
                {
                    if (!muteMinorDifferences)
                    {
                        Console.WriteLine("{0}: Empty nested collections", entry.Key, entry.Value.GetType(), value2.GetType(), entry.Value, value2);
                    }
                }
                else if (entry.Value.GetType() != value2.GetType())
                {
                    if (value2 is DateTime date2 && DateTime.TryParse(entry.Value.ToString(), out var date) && date == date2) continue;
                    if (entry.Value is JsonNumber jn && value2 is long num2 && jn.ToInt64() == num2) continue;
                    Console.WriteLine("{0}: Type/Value mismatch: {1} vs. {2}; '{3}' vs. '{4}'", entry.Key, entry.Value.GetType(), value2.GetType(), entry.Value, value2);
                }
                else if (!entry.Value.Equals(value2))
                {
                    Console.WriteLine("{0}: value equality mismatch!: '{1}' vs. '{2}'", entry.Key, entry.Value, value2);
                }
            }
        }
    }
}
