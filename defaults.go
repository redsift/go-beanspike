package beanspike

const (
	AerospikeHost      = "aerospike-exports"
	AerospikePort      = 3000
	AerospikeHostEnv   = "AEROSPIKE_HOST"
	AerospikePortEnv   = "AEROSPIKE_PORT"
	AerospikeNamespace = "beanspike"

	CompressionSizeThreshold = 1024 * 2

	// run admin operations at most 1/10 seconds
	AerospikeAdminDelay    = 2
	AerospikeAdminScanSize = 2500

	AerospikeMetadataSet = "metadata"

	// Bin names. Don't exceed 14 chars
	AerospikeNameStatus         = "status"
	AerospikeNameBody           = "body"
	AerospikeNameBy             = "by"
	AerospikeNameReason         = "reason"
	AerospikeNameDelay          = "delay"
	AerospikeNameDelayValue     = "seconds" // these are not used, review
	AerospikeNameTtr            = "ttr"
	AerospikeNameTtrKey         = "ttrkey"
	AerospikeNameTtrValue       = "seconds" // these are not used, review
	AerospikeNameCompressedSize = "csize"
	AerospikeNameSize           = "size"
	AerospikeNameRetries        = "retries"
	AerospikeNameRetryFlag      = "retryflag"
	AerospikeNameMetadata       = "metadata"
	AerospikeNameBuriedMetadata = "buried_meta"
	AerospikeNameToB            = "tob"

	// For the scan policy
	AerospikeQueryQueueSize = 4

	// For bin AerospikeNameStatus
	AerospikeSymReady       = "READY"
	AerospikeSymReserved    = "RESERVED"
	AerospikeSymReservedTtr = "RESERVEDTTR"
	AerospikeSymBuried      = "BURIED"
	AerospikeSymDelayed     = "DELAYED"
	AerospikeSymDeleted     = "DELETED"

	AerospikeKeySuffixTtr     = "reservedttr"
	AerospikeKeySuffixDelayed = "delayed"
)
