using System;
using System.Collections.Generic;
using Shielded;
using Shielded.ProxyGen;
using ShieldedDb.Models;

namespace ShieldedDb.Data
{
    public static class ContextExt
    {
        public static T New<T, TKey>(this IDictionary<TKey, T> dict, TKey id) where T : class, IEntity<TKey>, new()
        {
            var entity = Factory.NewShielded<T>();
            Shield.InTransaction(() => {
                entity.Id = id;
                dict.Add(id, entity);
            });
            return entity;
        }
    }
}

