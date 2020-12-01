using System;
using System.ComponentModel;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;

namespace Pacifica.Core
{
    public static class ExtensionMethods
    {
        /// <summary>
        /// Convert a DateTime type to a UNIX timestamp.
        /// </summary>
        /// <param name="dt">A DateTime to be converted.</param>
        /// <returns>Seconds since January 1st, 1970 00:00:00 UTC.</returns>
        /// <remarks>
        /// Providing a DateTime with Kind set to anything other than DateTimeKind.Utc
        /// will convert the structure to UTC before adjusting to the UNIX epoch.
        /// </remarks>
        [Obsolete("Unused")]
        // ReSharper disable once UnusedMember.Global
        public static ulong ToUnixTime(this DateTime dt)
        {
            if (dt.Kind != DateTimeKind.Utc)
            {
                dt = dt.ToUniversalTime();
            }

            var t = dt - new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            return (ulong)Math.Round(t.TotalSeconds);
        }

        /// <summary>
        /// Determine the Description attribute of a given enum value
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="enumerationValue"></param>
        /// <returns>Description attribute, or simply enum.ToString</returns>
        // ReSharper disable once UnusedTypeParameter
        public static string GetDescription<T>(this object enumerationValue) where T : struct
        {
            var type = enumerationValue.GetType();
            if (!type.IsEnum)
            {
                throw new ArgumentException("EnumerationValue must be of Enum type", nameof(enumerationValue));
            }

            // Tries to find a DescriptionAttribute for a potential friendly name for the enum
            var memberInfo = type.GetMember(enumerationValue.ToString());
            if (memberInfo.Length > 0)
            {
                var attrs = memberInfo[0].GetCustomAttributes(typeof(DescriptionAttribute), false);

                if (attrs.Length > 0)
                {
                    // Pull out the description value
                    return ((DescriptionAttribute)attrs[0]).Description;
                }
            }
            // If we have no description attribute, just return the ToString of the enum
            return enumerationValue.ToString();
        }

        [Obsolete("Unused")]
        // ReSharper disable once UnusedMember.Global
        public static T DeepClone<T>(this T a)
        {
            using (var stream = new MemoryStream())
            {
                var formatter = new BinaryFormatter();
                formatter.Serialize(stream, a);
                stream.Position = 0;
                return (T)formatter.Deserialize(stream);
            }
        }
    }
}