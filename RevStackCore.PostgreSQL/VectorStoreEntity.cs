using System;
namespace RevStackCore.PostgreSQL
{
    public class VectorStoreEntity<TKey> : IVectorStoreEntity<TKey>
    {
        public TKey Id { get; set; }
        public string Text { get; set; }
        //public string Description { get; set; } 
        public string AdditionalMetadata { get; set; }
        public float[] Embedding { get; set; }
    }
}

