
fsbinlog.snapshotMeta#6b49d850 fields_mask:#
    CommitPosition:long
    CommitCrc:#
    CommitTs:# = fsbinlog.SnapshotMeta;

fsbinlog.levStart#044c644b
    SchemaId:int
    ExtraBytes:int
    SplitMod:int
    SplitMin:int
    SplitMax:int
 = fsbinlog.LevStart;

fsbinlog.levUpgradeToGms#b75009a0 fields_mask:#
    PayloadOffset:long
    Crc:#
    Ts:# = fsbinlog.LevUpgradeToGms;
