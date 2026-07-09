using System;
namespace RevStackCore.PostgreSQL
{
    public interface IVectorStoreEntity<TKey>
    {
        TKey Id { get; set; }
        string Text { get; set; }
        //string Description { get; set; }
        string AdditionalMetadata { get; set; }
    }
}

