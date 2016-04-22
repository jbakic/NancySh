using System;
using Shielded.Distro;
using System.Runtime.Serialization;
using System.Collections.Generic;
using Shielded;

namespace nancySh.Models
{
    public class Account : DistributedBase<int>
    {
        public static Accessor<int, Account> Repo = new Accessor<int, Account>();

        public virtual string Owner { get; set; }
        public virtual decimal Balance { get; set; }
        public virtual DateTime LastBoking { get; set; }

        [IgnoreDataMember]
        public IEnumerable<Booking> Bookings
        {
            get
            {
                return Repository.GetAll<Guid, Booking>(new AccountBookingsQuery(Id));
            }
        }

        public static Account New(string owner, decimal initialBalance)
        {
            var generator = AccountNumberGenerator.Instance;
            generator.LastUsed = generator.LastUsed + 1;
            var id = generator.LastUsed;
            var acc = Account.Repo.Insert(new Account {
                Id = id,
                Owner = owner,
                LastBoking = DateTime.UtcNow,
                Balance = initialBalance,
            });
            Repository.Insert<Guid, Booking>(new Booking {
                Id = Guid.NewGuid(),
                AccountId = acc.Id,
                UtcTime = acc.LastBoking,
                Change = initialBalance,
            });
            return acc;
        }

        public void Book(DateTime time, decimal change)
        {
            Balance = Balance + change;
            LastBoking = time;
            Repository.Insert<Guid, Booking>(new Booking {
                Id = Guid.NewGuid(),
                AccountId = Id,
                UtcTime = time,
                Change = change,
            });
        }
    }
}

