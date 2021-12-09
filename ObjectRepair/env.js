const env = {
    OBJECT_REPAIR_BUCKETD_HOSTPORT: process.env.OBJECT_REPAIR_BUCKETD_HOSTPORT,
    OBJECT_REPAIR_SPROXYD_HOSTPORT: process.env.OBJECT_REPAIR_SPROXYD_HOSTPORT,
    OBJECT_REPAIR_RAFT_SESSION_ID: process.env.OBJECT_REPAIR_RAFT_SESSION_ID,
    OBJECT_REPAIR_RAFT_LOG_BATCH_SIZE: process.env.OBJECT_REPAIR_RAFT_LOG_BATCH_SIZE || 1000,
    OBJECT_REPAIR_LOOKBACK_WINDOW: process.env.OBJECT_REPAIR_LOOKBACK_WINDOW || 10000,
    OBJECT_REPAIR_LOG_LEVEL: process.env.OBJECT_REPAIR_LOG_LEVEL || 'info',
    OBJECT_REPAIR_DUMP_LEVEL: process.env.OBJECT_REPAIR_DUMP_LEVEL || 'error',
    OBJECT_REPAIR_LOG_INTERVAL: process.env.OBJECT_REPAIR_LOG_INTERVAL || 300,
};

module.exports = { env };
