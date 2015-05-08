package beanspike

const (
	AerospikeHost = "aerospike"
    AerospikePort = 3000
	AerospikeHostEnv = "AEROSPIKE_HOST"
	AerospikePortEnv = "AEROSPIKE_PORT"   
	// TODO change this
	AerospikeNamespace = "test"
	
	// run admin operations at most 1/10 seconds
	AerospikeAdminDelay = 2
	AerospikeAdminScanSize = 50
	
	AerospikeMetadataSet = "metadata"
	
	AerospikeNameStatus = "status"
	AerospikeNameBody = "body"
	AerospikeNameBy = "by"
	AerospikeNameReason = "reason"
	AerospikeNameDelay = "delay"
	AerospikeNameDelayValue = "seconds"
	AerospikeNameTtr = "ttr"
	// For the scan policy
	AerospikeQueryQueueSize = 4

	// For bin AerospikeNameStatus
	AerospikeSymReady = "READY"
	AerospikeSymReserved = "RESERVED"
	AerospikeSymBuried = "BURIED"
	AerospikeSymDelayed = "DELAYED"
)