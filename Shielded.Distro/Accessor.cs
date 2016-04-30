using System.Collections.Generic;

namespace Shielded.Distro
{
    /// <summary>
    /// It's annoying that one type parameter is not enough, and we always
    /// must type in the key and then the entity type, even though the latter
    /// completely identifies both. So, this class. If it could be static and
    /// be base class for other classes, but so that they specify these type
    /// args, that would be great. But no.
    /// </summary>
    public class Accessor<TKey, T> where T : DistributedBase<TKey>, new()
    {
        public IEnumerable<T> GetAll() { return Repository.GetAll<TKey, T>(Query.All); }

        public T Find(TKey id) { return Repository.Find<TKey, T>(id); }

        public T TryFind(TKey id) { return Repository.TryFind<TKey, T>(id); }

        public void Remove(T entity) { Repository.Remove<TKey, T>(entity); }

        /// <summary>
        /// Returns the "live", distributed entity, which will be ref-equal to your
        /// object if it is a proxy already, or a new shielded proxy otherwise.
        /// </summary>
        public T Insert(T entity) { return Repository.Insert<TKey, T>(entity); }

        /// <summary>
        /// Returns the "live", distributed entity.
        /// </summary>
        public T Update(T entity) { return Repository.Update<TKey, T>(entity); }
    }
}

