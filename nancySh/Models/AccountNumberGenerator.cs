using System;
using Shielded.Distro;

namespace nancySh.Models
{
    // there can be only one! with id == 1.
    public class AccountNumberGenerator : DistributedBase<int>
    {
        public virtual int LastUsed { get; set; }

        public static AccountNumberGenerator Instance
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

        public int GetNewNumber()
        {
            if (Repository.Update<int, AccountNumberGenerator>(this) != this)
                throw new InvalidOperationException("There can be only one!");
            LastUsed = LastUsed + 1;
            return LastUsed;
        }
    }
}

