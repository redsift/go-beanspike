package beanspike

const (
	AerospikeHost = "aerospike"
    AerospikePort = 3000
	AerospikeHostEnv = "AEROSPIKE_HOST"
	AerospikePortEnv = "AEROSPIKE_PORT"   
	AerospikeNamespace = "test"
	AerospikeNameStatus = "status"
	AerospikeNameBody = "body"
	AerospikeNameBy = "by"
	// For the scan policy
	AerospikeQueryQueueSize = 4

	// For bin AerospikeNameStatus
	AerospikeSymReady = "READY"
	AerospikeSymReserved = "RESERVED"
)