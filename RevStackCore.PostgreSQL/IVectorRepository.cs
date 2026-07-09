using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using RevStackCore.Pattern;

namespace RevStackCore.PostgreSQL
{
    public interface IVectorStoreRepository<TEntity, TKey> where TEntity : class, IEntity<TKey>
    {
        Task<IList<TEntity>> SearchAsync(string query, string filter, int limit);
        Task<IEnumerable<TEntity>> GetAsync();
        Task<TEntity?> GetById(TKey id);
        Task InsertAsync(IEnumerable<TEntity> entities);
        Task RefreshAsync(IEnumerable<TEntity> entities);
        Task DeleteAsync(TEntity entity);
    }
}

