using System;
using Shielded.Distro;

namespace nancySh.Models
{
    // there can be only one! with id == 1.
    public class AccountNumberGenerator : DistributedBase<int>
    {
        public virtual int LastUsed { get; set; }

        private static AccountNumberGenerator _instance
        {
            get
            {
                return Repository.InTransaction(() => {
                    var generator = Repository.TryFind<int, AccountNumberGenerator>(1);
                    if (generator != null)
                        return generator;
                    generator = new AccountNumberGenerator { Id = 1 };
                    return Repository.Insert<int, AccountNumberGenerator>(generator);
                });
            }
        }

        public static int GetNewNumber()
        {
            var instance = Repository.Update<int, AccountNumberGenerator>(_instance);
            instance.LastUsed = instance.LastUsed + 1;
            return instance.LastUsed;
        }
    }
}

