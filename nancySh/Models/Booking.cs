using System;
using Shielded.Distro;

namespace nancySh.Models
{
    public class Booking : DistributedBase<Guid>
    {
        public virtual int AccountId { get; set; }
        public virtual decimal Change { get; set; }
        public virtual DateTime UtcTime { get; set; }
    }
}

