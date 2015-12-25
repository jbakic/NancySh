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
            return new DataOp { OpType = DataOpType.Insert, Entity = Map.NonShieldedClone(entity) };
        }

        public static DataOp Update(IDistributed entity)
        {
            return new DataOp { OpType = DataOpType.Update, Entity = Map.NonShieldedClone(entity) };
        }

        public static DataOp Delete(IDistributed entity)
        {
            return new DataOp { OpType = DataOpType.Delete, Entity = Map.NonShieldedClone(entity) };
        }
    }
}

