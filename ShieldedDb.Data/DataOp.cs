using System;
using System.Collections.Generic;

namespace ShieldedDb.Data
{
    public enum DataOpType
    {
        Insert,
        Update,
        Delete
    }

    public class DataOp
    {
        public DataOpType OpType;
        public IDistributed Entity;

        public static DataOp Insert(IDistributed entity)
        {
            // to make sure it's readable after a RunToCommit completes.
            var o = entity.IdValue;
            return new DataOp { OpType = DataOpType.Insert, Entity = entity };
        }

        public static DataOp Update(IDistributed entity)
        {
            var o = entity.IdValue;
            return new DataOp { OpType = DataOpType.Update, Entity = entity };
        }

        public static DataOp Delete(IDistributed entity)
        {
            var o = entity.IdValue;
            return new DataOp { OpType = DataOpType.Delete, Entity = entity };
        }
    }
}

