using System;
using Shielded.Distro;

namespace nancySh.Models
{
    public class AccountBookingsQuery : Query
    {
        public int AccountId;

        public AccountBookingsQuery() { }

        public AccountBookingsQuery(int accountId)
        {
            AccountId = accountId;
        }

        public override bool Check(DistributedBase entity)
        {
            var booking = entity as Booking;
            return booking != null && booking.AccountId == AccountId;
        }

        public override bool Equals(Query other)
        {
            var acc = other as AccountBookingsQuery;
            return acc != null && acc.AccountId == AccountId;
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hash = 17;
                hash = hash * 23 + base.GetHashCode();
                hash = hash * 23 + AccountId;
                return hash;
            }
        }
    }
}

