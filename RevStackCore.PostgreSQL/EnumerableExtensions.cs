using System;
using System.Collections.Generic;

namespace RevStackCore.PostgreSQL
{
    public static class EnumerableExtensions
    {
        public static IEnumerable<IEnumerable<T>> Batch<T>(this IEnumerable<T> source, int batchSize)
        {
            var buffer = new List<T>(batchSize);

            foreach (var item in source)
            {
                buffer.Add(item);
                if (buffer.Count >= batchSize)
                {
                    yield return buffer.ToArray();
                    buffer.Clear();
                }
            }

            if (buffer.Count > 0)
            {
                yield return buffer.ToArray();
            }
        }
    }
}

