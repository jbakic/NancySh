﻿using System;
using System.Collections.Generic;

namespace Shielded.Distro
{
    public enum DataOpType
    {
        Insert,
        Update,
        Delete,
        Ignore
    }

    public class DataOp
    {
        public DataOpType OpType;
        public DistributedBase Entity;

        public static DataOp Insert(DistributedBase entity)
        {
            // to make sure it's writable after a RunToCommit completes.
            entity.Version = entity.Version;
            return new DataOp { OpType = DataOpType.Insert, Entity = entity };
        }

        public static DataOp Update(DistributedBase entity)
        {
            entity.Version = entity.Version;
            return new DataOp { OpType = DataOpType.Update, Entity = entity };
        }

        public static DataOp Delete(DistributedBase entity)
        {
            entity.Version = entity.Version;
            return new DataOp { OpType = DataOpType.Delete, Entity = entity };
        }

        public static DataOp Ignore(DistributedBase entity)
        {
            entity.Version = entity.Version;
            return new DataOp { OpType = DataOpType.Ignore, Entity = entity };
        }
    }
}

