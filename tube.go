package beanspike

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	as "github.com/aerospike/aerospike-client-go"
	pt "github.com/aerospike/aerospike-client-go/types/particle_type"
	lz4 "github.com/cloudflare/golz4"
)

var (
	ErrEmptyRecord        = errors.New("ASSERT: Record empty")
	ErrNotBuriedOrDelayed = errors.New("Job is not buried or delayed")
	ErrJobNotFound        = errors.New("Job not found")
)

func (tube *Tube) Put(body []byte, delay time.Duration, ttr time.Duration, lz bool,
	metadata string) (id int64, err error) {
	id, err = tube.Conn.newJobID()
	if err != nil {
		return
	}

	key, err := as.NewKey(AerospikeNamespace, tube.Name, id)
	if err != nil {
		return
	}

	policy := as.NewWritePolicy(0, 0)
	policy.RecordExistsAction = as.CREATE_ONLY
	policy.SendKey = true
	policy.CommitLevel = as.COMMIT_MASTER

	client := tube.Conn.aerospike

	bins := make([]*as.Bin, 0, 9)

	bins = append(bins, as.NewBin(AerospikeNameSize, len(body)))

	if lz && shouldCompress(body) {
		var cbody []byte
		cbody, err = compress(body)
		if err != nil {
			return
		}

		if len(cbody) >= len(body) {
			// incompressible, leave it raw, marked with a -value for AerospikeNameCompressedSize
			bins = append(bins, as.NewBin(AerospikeNameBody, body))
			bins = append(bins, as.NewBin(AerospikeNameCompressedSize, -len(cbody)))
		} else {
			bins = append(bins, as.NewBin(AerospikeNameBody, cbody))
			bins = append(bins, as.NewBin(AerospikeNameCompressedSize, len(cbody)))
		}
	} else {
		bins = append(bins, as.NewBin(AerospikeNameBody, body))
	}

	if delay == 0 {
		bins = append(bins, as.NewBin(AerospikeNameStatus, AerospikeSymReady))
	} else {
		// put the delay entry in first
		var binDelay *as.Bin
		binDelay, err = tube.delayJob(id, delay)
		if err != nil {
			return
		}
		bins = append(bins, as.NewBin(AerospikeNameStatus, AerospikeSymDelayed))
		bins = append(bins, binDelay)
	}

	if ttr != 0 {
		bins = append(bins, as.NewBin(AerospikeNameTtr, int64(ttr.Seconds())))
	}

	if len(metadata) > 0 {
		bins = append(bins, as.NewBin(AerospikeNameMetadata, metadata))
	}

	err = client.PutBins(policy, key, bins...)
	if err != nil {
		return
	}

	if tube.Conn != nil {
		tube.Conn.stats("tube.put.count", tube.Name, float64(1))
	}

	return id, nil
}

func (tube *Tube) Delete(id int64) (bool, error) {
	return tube.delete(id, 0)
}

func (tube *Tube) delete(id int64, genID uint32) (bool, error) {
	key, err := as.NewKey(AerospikeNamespace, tube.Name, id)
	if err != nil {
		return false, err
	}

	// if genID is 0, fetch record to get the latest generation
	if genID == 0 {
		client := tube.Conn.aerospike
		record, err := client.Get(nil, key, AerospikeNameStatus)
		if err != nil {
			return false, err
		}
		if record == nil {
			return false, ErrEmptyRecord
		}

		genID = record.Generation
	}

	// nil out body before deleting record to address aerospike limitations.
	// also set status to DELETED
	// Ref: https://discuss.aerospike.com/t/expired-deleted-data-reappears-after-server-is-restarted/470
	policy := as.NewWritePolicy(0, 0)
	policy.RecordExistsAction = as.UPDATE_ONLY
	policy.SendKey = true
	policy.CommitLevel = as.COMMIT_MASTER
	policy.GenerationPolicy = as.NONE

	binBody := as.NewBin(AerospikeNameBody, nil)
	binCSize := as.NewBin(AerospikeNameCompressedSize, nil)
	binSize := as.NewBin(AerospikeNameSize, 0)
	binStatus := as.NewBin(AerospikeNameStatus, AerospikeSymDeleted)

	for i := 0; i <= int(genID); i++ {
		policy.Generation = uint32(i)
		err = tube.Conn.aerospike.PutBins(policy, key, binBody, binCSize, binSize, binStatus)
		if err != nil {
			return false, err
		}
	}

	ex, err := tube.Conn.aerospike.Delete(nil, key)
	if err != nil {
		if tube.Conn != nil {
			tube.Conn.stats("tube.delete.count", tube.Name, float64(1))
		}
	}

	return ex, err
}

func (tube *Tube) Stats() (s *Stats, err error) {
	stm := as.NewStatement(AerospikeNamespace, tube.Name)
	stm.SetAggregateFunction("beanspikeStats", "add_stat_ops", nil, true)

	recordset, err := tube.Conn.aerospike.Query(nil, stm)

	if err != nil {
		return nil, err
	}

	defer recordset.Close()

	s = new(Stats)

	for res := range recordset.Results() {
		if res.Err != nil {
			return nil, res.Err
		}

		if resultsVal := res.Record.Bins["SUCCESS"]; resultsVal != nil {
			results := resultsVal.(map[interface{}]interface{})

			s.Jobs += results["count"].(int)
			s.Ready += results["ready"].(int)
			s.Buried += results["buried"].(int)
			s.Delayed += results["delayed"].(int)
			s.Reserved += results["reserved"].(int)
			s.Deleted += results["deleted"].(int)

			s.JobSize += results["js"].(int)
			s.UsedSize += results["rs"].(int)
			s.SkippedSize += results["ss"].(int)
		} else if errorVal := res.Record.Bins["FAILURE"]; errorVal != nil {
			return nil, fmt.Errorf("Stats error. %v", errorVal)
		} else {
			return nil, errors.New("Internal error performing stats on namespace")
		}
	}

	return s, nil
}

func (tube *Tube) Touch(id int64) error {
	client := tube.Conn.aerospike

	key, err := as.NewKey(AerospikeNamespace, tube.Name, id)
	if err != nil {
		return err
	}

	record, err := client.Get(nil, key, AerospikeNameStatus, AerospikeNameBy, AerospikeNameTtr, AerospikeNameTtrKey)
	if err != nil {
		return err
	}
	if record == nil {
		return ErrEmptyRecord
	}

	ttrValue := record.Bins[AerospikeNameTtr]

	if record.Bins[AerospikeNameTtrKey] == nil || ttrValue == nil {
		return errors.New("Job does not need Touching")
	}

	if record.Bins[AerospikeNameStatus] != AerospikeSymReservedTtr {
		return errors.New("Job is not reserved, may have timed out")
	}

	if record.Bins[AerospikeNameBy] != tube.Conn.clientID {
		return errors.New("Job is not reserved by this client")
	}

	touch, err := as.NewKey(AerospikeNamespace, AerospikeMetadataSet, record.Bins[AerospikeNameTtrKey].(string))
	if err != nil {
		return err
	}

	if tube.Conn != nil {
		tube.Conn.stats("tube.touch.count", tube.Name, float64(1))
	}

	policy := as.NewWritePolicy(0, uint32(ttrValue.(int)))
	policy.CommitLevel = as.COMMIT_MASTER
	return client.Touch(policy, touch)
}

func (tube *Tube) Release(id int64, delay time.Duration) error {
	return tube.ReleaseWithRetry(id, delay, false, false)
}

func (tube *Tube) ReleaseWithRetry(id int64, delay time.Duration, incr, retryFlag bool) error {
	client := tube.Conn.aerospike

	key, err := as.NewKey(AerospikeNamespace, tube.Name, id)
	if err != nil {
		return err
	}

	record, err := client.Get(nil, key, AerospikeNameStatus, AerospikeNameBy, AerospikeNameRetries,
		AerospikeNameTtrKey)
	if err != nil {
		return err
	}
	if record == nil {
		return ErrEmptyRecord
	}

	if status := record.Bins[AerospikeNameStatus]; status != AerospikeSymReserved &&
		status != AerospikeSymReservedTtr {
		return errors.New("Job is not reserved")
	}

	if record.Bins[AerospikeNameBy] != tube.Conn.clientID {
		return errors.New("Job is not reserved by this client")
	}

	if binTtr := record.Bins[AerospikeNameTtrKey]; binTtr != nil {
		entry := binTtr.(string)

		key, err := as.NewKey(AerospikeNamespace, AerospikeMetadataSet, entry)

		if err == nil {
			client.Delete(nil, key)
		}
	}

	writePolicy := as.NewWritePolicy(record.Generation, 0)
	writePolicy.GenerationPolicy = as.EXPECT_GEN_EQUAL
	writePolicy.RecordExistsAction = as.UPDATE_ONLY

	bins := make([]*as.Bin, 0, 6)
	if incr {
		retries := 1
		retriesVal := record.Bins[AerospikeNameRetries]
		if retriesVal != nil {
			retries = retriesVal.(int)
			retries += 1
		}
		bins = append(bins, as.NewBin(AerospikeNameRetries, retries))
	}

	// Set retryflag
	{
		retryFlagValue := 0
		if retryFlag {
			retryFlagValue = 1
		}
		bins = append(bins, as.NewBin(AerospikeNameRetryFlag, retryFlagValue))
	}

	bins = append(bins, as.NewBin(AerospikeNameBy, as.NewNullValue()))
	bins = append(bins, as.NewBin(AerospikeNameTtrKey, as.NewNullValue()))

	if tube.Conn != nil {
		tube.Conn.stats("tube.release.count", tube.Name, float64(1))
	}

	if delay.Seconds() == 0 {
		bins = append(bins, as.NewBin(AerospikeNameStatus, AerospikeSymReady))
		return client.PutBins(writePolicy, record.Key, bins...)
	}

	binDelay, err := tube.delayJob(id, delay)
	if err != nil {
		return err
	}
	bins = append(bins, as.NewBin(AerospikeNameStatus, AerospikeSymDelayed))
	bins = append(bins, binDelay)
	return client.PutBins(writePolicy, record.Key, bins...)
}

func (tube *Tube) Bury(id int64, reason []byte) error {
	client := tube.Conn.aerospike

	key, err := as.NewKey(AerospikeNamespace, tube.Name, id)
	if err != nil {
		return err
	}

	record, err := client.Get(nil, key, AerospikeNameStatus, AerospikeNameBy, AerospikeNameMetadata)
	if err != nil {
		return err
	}
	if record == nil {
		return ErrEmptyRecord
	}

	if status := record.Bins[AerospikeNameStatus]; status != AerospikeSymReserved &&
		status != AerospikeSymReservedTtr {
		return errors.New("Job is not reserved")
	}

	if record.Bins[AerospikeNameBy] != tube.Conn.clientID {
		return errors.New("Job is not reserved by this client")
	}

	writePolicy := as.NewWritePolicy(record.Generation, 0)
	writePolicy.GenerationPolicy = as.EXPECT_GEN_EQUAL
	writePolicy.RecordExistsAction = as.UPDATE_ONLY

	binStatus := as.NewBin(AerospikeNameStatus, AerospikeSymBuried)
	binReason := as.NewBin(AerospikeNameReason, reason)
	binBuriedMeta := as.NewBin(AerospikeNameBuriedMetadata, record.Bins[AerospikeNameMetadata])
	binMeta := as.NewBin(AerospikeNameMetadata, nil)

	if tube.Conn != nil {
		tube.Conn.stats("tube.buried.count", tube.Name, float64(1))
	}

	return client.PutBins(writePolicy, record.Key, binStatus, binReason, binBuriedMeta, binMeta)
}

// Job does not have to be reserved by this client
func (tube *Tube) KickJob(id int64) error {
	client := tube.Conn.aerospike

	key, err := as.NewKey(AerospikeNamespace, tube.Name, id)
	if err != nil {
		return err
	}

	record, err := client.Get(nil, key, AerospikeNameStatus)
	if err != nil {
		return err
	}
	if record == nil {
		return ErrEmptyRecord
	}

	if status := record.Bins[AerospikeNameStatus]; status != AerospikeSymBuried && status != AerospikeSymDelayed {
		return ErrNotBuriedOrDelayed
	}

	writePolicy := as.NewWritePolicy(record.Generation, 0)
	writePolicy.GenerationPolicy = as.EXPECT_GEN_EQUAL
	writePolicy.RecordExistsAction = as.UPDATE_ONLY

	binStatus := as.NewBin(AerospikeNameStatus, AerospikeSymReady)

	binReason := as.NewBin(AerospikeNameReason, as.NewNullValue())
	binDelay := as.NewBin(AerospikeNameDelay, as.NewNullValue())

	binBuriedMeta := as.NewBin(AerospikeNameBuriedMetadata, nil)
	binMeta := as.NewBin(AerospikeNameMetadata, record.Bins[AerospikeNameBuriedMetadata])

	if tube.Conn != nil {
		tube.Conn.stats("tube.kick.count", tube.Name, float64(1))
	}

	return client.PutBins(writePolicy, record.Key, binStatus, binReason, binDelay, binBuriedMeta, binMeta)
}

func (tube *Tube) Reserve() (id int64, body []byte, ttr time.Duration, retries int, retryFlag bool,
	metadata string, err error) {
	client := tube.Conn.aerospike

	if tube.first {
		tube.first = false

		_, _ = tube.bumpReservedEntries(AerospikeAdminScanSize)
	}

	for i := 0; i < 2; i++ {
		stm := as.NewStatement(AerospikeNamespace, tube.Name, AerospikeNameBody, AerospikeNameTtr,
			AerospikeNameCompressedSize, AerospikeNameSize, AerospikeNameStatus, AerospikeNameRetries,
			AerospikeNameRetryFlag, AerospikeNameMetadata)
		stm.Addfilter(as.NewEqualFilter(AerospikeNameStatus, AerospikeSymReady))

		policy := as.NewQueryPolicy()
		policy.RecordQueueSize = AerospikeQueryQueueSize

		var recordset *as.Recordset
		recordset, err = client.Query(policy, stm)

		if err != nil {
			return 0, nil, 0, 0, false, "", err
		}

		defer func(rs *as.Recordset) {
			rs.Close()
		}(recordset)
		for res := range recordset.Results() {
			if res.Err != nil {
				if err == nil {
					err = res.Err
				}
			} else {
				var body []byte
				bodyVal := res.Record.Bins[AerospikeNameBody]
				if bodyVal != nil {
					body = bodyVal.([]byte)
				}
				ttr = 0
				retries = 0
				retryFlag = false

				if key := res.Record.Key.Value(); key != nil && key.GetType() == pt.INTEGER &&
					body != nil {
					job := res.Record.Key.Value().GetObject().(int64)

					var binTtrExp *as.Bin
					reserve := AerospikeSymReserved
					if ttrValue := res.Record.Bins[AerospikeNameTtr]; ttrValue != nil {
						ttr = time.Duration(ttrValue.(int)) * time.Second
						reserve = AerospikeSymReservedTtr
						binTtrExp, err = tube.timeJob(job, ttr)
						if err != nil {
							return 0, nil, 0, 0, false, "", err
						}
					}

					if retriesValue := res.Record.Bins[AerospikeNameRetries]; retriesValue != nil {
						retries = retriesValue.(int)
					}

					if retryFlagValue := res.Record.Bins[AerospikeNameRetryFlag]; retryFlagValue != nil {
						retryFlag = retryFlagValue.(int) > 0
					}

					lockErr := tube.attemptJobReservation(res.Record, reserve, binTtrExp)
					if lockErr == nil {
						if czValue := res.Record.Bins[AerospikeNameCompressedSize]; czValue != nil {
							cz := czValue.(int)
							if cz > 0 {

								if ozValue := res.Record.Bins[AerospikeNameSize]; ozValue != nil {
									// was compressed
									body, err = decompress(body, ozValue.(int))
									if err != nil {
										return 0, nil, 0, 0, false, "", err
									}
								} else {
									return 0, nil, 0, 0, false, "",
										errors.New("Could not establish original size of compressed content")
								}
							}
						}

						if tube.Conn != nil {
							tube.Conn.stats("tube.reserve.count", tube.Name, float64(1))
						}

						metadata := ""
						if m, ok := res.Record.Bins[AerospikeNameMetadata].(string); ok {
							metadata = m
						}

						// success, we have this job
						return job, body, ttr, retries, retryFlag, metadata, nil
					}
					// else something happened to this job in the way
					// TODO: Handle println
					fmt.Printf("!!! Job lock failed due to %v\n", lockErr)
				} else {
					// if the key is nil or not an int something is wrong. WritePolicy is not set
					// correctly. Skip this record and set err if this is the only record
					if err == nil {
						err = errors.New("Missing appropriate entry in job tube")
					}
				}
			}
		}
		count, _ := tube.bumpReservedEntries(AerospikeAdminScanSize)
		if count != 0 {
			continue
		}

		// no jobs to return, use the cycles to admin the set
		count, _ = tube.bumpDelayedEntries(AerospikeAdminScanSize)

		if count == 0 {
			_, _ = tube.deleteZombieEntries(AerospikeAdminScanSize)
			break
		}
	}

	// Some form of error or no job fall through
	if err == nil {
		err = ErrJobNotFound
	}
	return 0, nil, 0, 0, false, "", err
}

func (tube *Tube) ReserveBatch(batchSize int) (jobs []*Job, err error) {
	id, body, ttr, retries, retryFlag, metadata, err := tube.Reserve()
	if err != nil {
		return nil, err
	}

	if id == 0 {
		return nil, nil
	}

	jobs = append(jobs, &Job{
		ID:        id,
		Body:      body,
		TTR:       ttr,
		Retries:   retries,
		RetryFlag: retryFlag,
		Tube:      tube,
	})

	if len(metadata) == 0 {
		return jobs, nil
	}

	client := tube.Conn.aerospike

	for i := 0; i < 1; i++ {
		if len(jobs) >= batchSize {
			return jobs, nil
		}

		stm := as.NewStatement(AerospikeNamespace, tube.Name, AerospikeNameBody, AerospikeNameTtr,
			AerospikeNameCompressedSize, AerospikeNameSize, AerospikeNameStatus, AerospikeNameRetries,
			AerospikeNameRetryFlag)
		stm.Addfilter(as.NewEqualFilter(AerospikeNameMetadata, metadata))

		policy := as.NewQueryPolicy()
		policy.RecordQueueSize = batchSize

		var recordset *as.Recordset
		recordset, err = client.Query(policy, stm)

		if err != nil {
			return jobs, nil
		}

		defer func(rs *as.Recordset) {
			rs.Close()
		}(recordset)
		for res := range recordset.Results() {
			if res.Err != nil {
				if err == nil {
					err = res.Err
				}
			} else {
				if st, ok := res.Record.Bins[AerospikeNameStatus].(string); ok {
					if !(st == AerospikeSymReady || st == AerospikeSymDelayed) {
						continue
					}
				} else {
					continue
				}

				var body []byte
				bodyVal := res.Record.Bins[AerospikeNameBody]
				if bodyVal != nil {
					body = bodyVal.([]byte)
				}
				ttr = 0
				retries = 0
				retryFlag = false

				if key := res.Record.Key.Value(); key != nil && key.GetType() == pt.INTEGER &&
					body != nil {
					job := res.Record.Key.Value().GetObject().(int64)

					var binTtrExp *as.Bin
					reserve := AerospikeSymReserved
					if ttrValue := res.Record.Bins[AerospikeNameTtr]; ttrValue != nil {
						ttr = time.Duration(ttrValue.(int)) * time.Second
						reserve = AerospikeSymReservedTtr
						binTtrExp, err = tube.timeJob(job, ttr)
						if err != nil {
							return jobs, nil
						}
					}

					if retriesValue := res.Record.Bins[AerospikeNameRetries]; retriesValue != nil {
						retries = retriesValue.(int)
					}

					if retryFlagValue := res.Record.Bins[AerospikeNameRetryFlag]; retryFlagValue != nil {
						retryFlag = retryFlagValue.(int) > 0
					}

					lockErr := tube.attemptJobReservation(res.Record, reserve, binTtrExp)
					if lockErr == nil {
						if czValue := res.Record.Bins[AerospikeNameCompressedSize]; czValue != nil {
							cz := czValue.(int)
							if cz > 0 {

								if ozValue := res.Record.Bins[AerospikeNameSize]; ozValue != nil {
									// was compressed
									body, err = decompress(body, ozValue.(int))
									if err != nil {
										return jobs, nil
									}
								} else {
									return jobs, nil
								}
							}
						}

						if tube.Conn != nil {
							tube.Conn.stats("tube.reserve.count", tube.Name, float64(1))
						}

						// success, we have this job
						jobs = append(jobs, &Job{
							ID:        job,
							Body:      body,
							TTR:       ttr,
							Retries:   retries,
							RetryFlag: retryFlag,
							Tube:      tube,
						})

						if len(jobs) >= batchSize {
							return jobs, nil
						}

						continue
					}
					// else something happened to this job in the way
					// TODO: Handle println
					fmt.Printf("!!! Job lock failed due to %v\n", lockErr)
				} else {
					// if the key is nil or not an int something is wrong. WritePolicy is not set
					// correctly. Skip this record and set err if this is the only record
					if err == nil {
						err = errors.New("Missing appropriate entry in job tube")
					}
				}
			}
		}
	}

	if len(jobs) > 0 {
		return jobs, nil
	}

	// Some form of error or no job fall through
	if err == nil {
		err = ErrJobNotFound
	}
	return nil, err
}

func (tube *Tube) PeekBuried() (id int64, body []byte, ttr time.Duration, reason []byte, err error) {
	client := tube.Conn.aerospike

	for i := 0; i < 2; i++ {
		stm := as.NewStatement(AerospikeNamespace, tube.Name, AerospikeNameBody, AerospikeNameTtr,
			AerospikeNameCompressedSize, AerospikeNameSize, AerospikeNameStatus, AerospikeNameReason)
		stm.Addfilter(as.NewEqualFilter(AerospikeNameStatus, AerospikeSymBuried))

		policy := as.NewQueryPolicy()
		policy.RecordQueueSize = AerospikeQueryQueueSize

		recordset, err := client.Query(policy, stm)

		if err != nil {
			return 0, nil, 0, nil, err
		}

		defer func(rs *as.Recordset) {
			rs.Close()
		}(recordset)

		for res := range recordset.Results() {
			if res.Err != nil {
				if err == nil {
					err = res.Err
				}
			} else {
				var body []byte
				bodyVal := res.Record.Bins[AerospikeNameBody]
				if bodyVal != nil {
					body = bodyVal.([]byte)
				}
				ttr = 0

				key := res.Record.Key.Value()
				if key != nil && key.GetType() == pt.INTEGER && body != nil {
					job := key.GetObject().(int64)

					if ttrValue := res.Record.Bins[AerospikeNameTtr]; ttrValue != nil {
						ttr = time.Duration(ttrValue.(int)) * time.Second
					}

					if czValue := res.Record.Bins[AerospikeNameCompressedSize]; czValue != nil {
						cz := czValue.(int)
						if cz > 0 {

							if ozValue := res.Record.Bins[AerospikeNameSize]; ozValue != nil {
								// was compressed
								body, err = decompress(body, ozValue.(int))
								if err != nil {
									return 0, nil, 0, nil, err
								}
							} else {
								return 0, nil, 0, nil,
									errors.New("Could not establish original size of compressed content")
							}
						}
					}

					if reasonValue := res.Record.Bins[AerospikeNameReason]; reasonValue != nil {
						reason = reasonValue.([]byte)
					}

					if tube.Conn != nil {
						tube.Conn.stats("tube.peekburied.count", tube.Name, float64(1))
					}

					// success, we have this job
					return job, body, ttr, reason, nil
				}

				// if the key is nil or not an int something is wrong. WritePolicy is not set
				// correctly. Skip this record and set err if this is the only record
				if err == nil {
					err = errors.New("Missing appropriate entry in job tube")
				}
			}
		}
	}

	// Some form of error or no job fall through
	return 0, nil, 0, nil, err
}

// this could be done in the future with UDFs triggered on expiry
// note, delays are defined as best effort delay
func (tube *Tube) delayJob(id int64, delay time.Duration) (*as.Bin, error) {
	delayKey := tube.Name + ":delay:" + strconv.FormatInt(id, 10)

	key, err := as.NewKey(AerospikeNamespace, AerospikeMetadataSet, delayKey)
	if err != nil {
		return nil, err
	}

	if key == nil {
		return nil, errors.New("No delay key generated")
	}

	policy := as.NewWritePolicy(0, uint32(delay.Seconds()))
	policy.RecordExistsAction = as.CREATE_ONLY
	policy.CommitLevel = as.COMMIT_MASTER

	delayBin := as.NewBin(AerospikeNameDelayValue, int32(delay.Seconds()))
	err = tube.Conn.aerospike.PutBins(policy, key, delayBin)
	if err != nil {
		return nil, err
	}

	return as.NewBin(AerospikeNameDelay, delayKey), nil
}

func (tube *Tube) timeJob(id int64, ttr time.Duration) (*as.Bin, error) {
	ttrKey := tube.Name + ":ttr:" + strconv.FormatInt(id, 10)

	key, err := as.NewKey(AerospikeNamespace, AerospikeMetadataSet, ttrKey)
	if err != nil {
		return nil, err
	}

	if key == nil {
		return nil, errors.New("No ttr key generated")
	}

	policy := as.NewWritePolicy(0, uint32(ttr.Seconds()))
	policy.RecordExistsAction = as.CREATE_ONLY
	policy.CommitLevel = as.COMMIT_MASTER

	ttrBin := as.NewBin(AerospikeNameTtrValue, int32(ttr.Seconds()))
	err = tube.Conn.aerospike.PutBins(policy, key, ttrBin)
	if err != nil {
		return nil, err
	}

	return as.NewBin(AerospikeNameTtrKey, ttrKey), nil
}

// puts an expiring entry that locks out other scans on the tube
func (tube *Tube) shouldOperate(scan string) bool {
	key, _ := as.NewKey(AerospikeNamespace, AerospikeMetadataSet, tube.Name+":"+scan)

	policy := as.NewWritePolicy(0, AerospikeAdminDelay)
	policy.RecordExistsAction = as.CREATE_ONLY

	binBy := as.NewBin(AerospikeNameBy, tube.Conn.clientID)

	err := tube.Conn.aerospike.PutBins(policy, key, binBy)
	if err != nil {
		return false
	}
	return true
}

// admin function to move RESERVED operations with a TTR to READY
// if required
func (tube *Tube) bumpReservedEntries(n int) (int, error) {
	if !tube.shouldOperate(AerospikeKeySuffixTtr) {
		return 0, nil
	}

	client := tube.Conn.aerospike

	stm := as.NewStatement(AerospikeNamespace, tube.Name, AerospikeNameTtrKey)
	stm.Addfilter(as.NewEqualFilter(AerospikeNameStatus, AerospikeSymReservedTtr))

	policy := as.NewQueryPolicy()
	policy.RecordQueueSize = n

	recordset, err := client.Query(policy, stm)

	// build a list of delayed jobs
	if err != nil {
		return 0, err
	}

	defer recordset.Close()
	type Entry struct {
		generation uint32
		key        *as.Key
	}
	entries := make([]*Entry, 0, n)
	keys := make([]*as.Key, 0, n)

	for res := range recordset.Results() {
		if len(keys) >= AerospikeAdminScanSize {
			break
		}

		if res.Err != nil {
			return 0, err
		}

		if binTtr := res.Record.Bins[AerospikeNameTtrKey]; binTtr != nil {
			entry := binTtr.(string)

			key, _ := as.NewKey(AerospikeNamespace, AerospikeMetadataSet, entry)
			keys = append(keys, key)

			val := &Entry{res.Record.Generation, res.Record.Key}
			entries = append(entries, val)
		}
	}

	batchPolicy := as.NewBatchPolicy()
	batchPolicy.Priority = as.HIGH
	records, err := client.BatchGetHeader(batchPolicy, keys)
	if err != nil {
		return 0, err
	}

	count := 0
	for i := 0; i < len(records); i++ {
		record := records[i]
		if record == nil {
			// the reserve has expired
			update := as.NewWritePolicy(entries[i].generation, 0)
			update.RecordExistsAction = as.UPDATE_ONLY
			update.CommitLevel = as.COMMIT_MASTER
			update.GenerationPolicy = as.EXPECT_GEN_EQUAL
			update.SendKey = true

			binStatus := as.NewBin(AerospikeNameStatus, AerospikeSymReady)
			// removing Delay may not be required
			// binTtrKey := as.NewBin(AerospikeNameTtrKey, as.NewNullValue())
			err := client.PutBins(update, entries[i].key, binStatus)
			if err != nil {
				return count, err
			}
			count++
		}
	}

	return count, nil
}

// admin function to move DELAYED operations to READY
// returns the number of jobs processed and if any action was taken
func (tube *Tube) bumpDelayedEntries(n int) (int, error) {
	if !tube.shouldOperate(AerospikeKeySuffixDelayed) {
		return 0, nil
	}

	client := tube.Conn.aerospike

	stm := as.NewStatement(AerospikeNamespace, tube.Name, AerospikeNameDelay)
	stm.Addfilter(as.NewEqualFilter(AerospikeNameStatus, AerospikeSymDelayed))

	policy := as.NewQueryPolicy()
	policy.RecordQueueSize = n

	recordset, err := client.Query(policy, stm)

	// build a list of delayed jobs
	if err != nil {
		return 0, err
	}

	defer recordset.Close()

	type Entry struct {
		generation uint32
		key        *as.Key
	}
	entries := make([]*Entry, 0, n)
	keys := make([]*as.Key, 0, n)
	for res := range recordset.Results() {
		if len(keys) >= AerospikeAdminScanSize {
			break
		}

		if res.Err != nil {
			return 0, err
		}
		delay := res.Record.Bins[AerospikeNameDelay]
		if delay == nil {
			// TODO: Looks like this might need cleanup.
			continue
		}

		entry := delay.(string)
		key, _ := as.NewKey(AerospikeNamespace, AerospikeMetadataSet, entry)
		keys = append(keys, key)

		val := &Entry{res.Record.Generation, res.Record.Key}
		entries = append(entries, val)
	}

	// batch query against the expire list, if entry is missing, bump to ready
	batchPolicy := as.NewBatchPolicy()
	batchPolicy.Priority = as.HIGH
	records, err := client.BatchGetHeader(batchPolicy, keys)
	if err != nil {
		return 0, err
	}

	count := 0
	for i := 0; i < len(records); i++ {
		record := records[i]

		if record == nil || record.Expiration < AerospikeAdminDelay || record.Expiration == math.MaxUint32 {
			// the record has expired or will expire before this operation runs again
			update := as.NewWritePolicy(entries[i].generation, 0)
			update.RecordExistsAction = as.UPDATE_ONLY
			update.CommitLevel = as.COMMIT_MASTER
			update.GenerationPolicy = as.EXPECT_GEN_EQUAL
			update.SendKey = true

			binStatus := as.NewBin(AerospikeNameStatus, AerospikeSymReady)
			// removing Delay may not be required
			binDelay := as.NewBin(AerospikeNameDelay, as.NewNullValue())
			err := client.PutBins(update, entries[i].key, binStatus, binDelay)
			if err != nil {
				return count, err
			}
			count++
		}
	}

	return count, nil
}

func (tube *Tube) deleteZombieEntries(n int) (int, error) {
	client := tube.Conn.aerospike

	stm := as.NewStatement(AerospikeNamespace, tube.Name)
	stm.Addfilter(as.NewEqualFilter(AerospikeNameStatus, AerospikeSymDeleted))

	policy := as.NewQueryPolicy()
	policy.RecordQueueSize = n

	recordset, err := client.Query(policy, stm)
	if err != nil {
		return 0, err
	}

	defer recordset.Close()

	type Entry struct {
		generation uint32
		key        *as.Key
	}

	count := 0
	for res := range recordset.Results() {
		if count >= AerospikeAdminScanSize {
			break
		}

		if res.Err != nil {
			return 0, err
		}

		if key := res.Record.Key.Value(); key != nil && key.GetType() == pt.INTEGER {
			job := res.Record.Key.Value().GetObject().(int64)
			tube.Delete(job)
			count++
		} else {
			tube.Conn.aerospike.Delete(nil, res.Record.Key)
			count++
		}
	}

	if count > 0 {
		// TODO: Log to statsd
		if tube.Conn != nil {
			tube.Conn.stats("tube.zombie.count", tube.Name, float64(count))
		}
	}

	return count, nil
}

func (tube *Tube) attemptJobReservation(record *as.Record, status string, binTtrExp *as.Bin) (err error) {
	writePolicy := as.NewWritePolicy(record.Generation, 0)
	writePolicy.GenerationPolicy = as.EXPECT_GEN_EQUAL
	writePolicy.RecordExistsAction = as.UPDATE_ONLY

	bins := make([]*as.Bin, 0, 3)

	bins = append(bins, as.NewBin(AerospikeNameStatus, status))
	bins = append(bins, as.NewBin(AerospikeNameBy, tube.Conn.clientID))
	if binTtrExp != nil {
		bins = append(bins, binTtrExp)
	}
	return tube.Conn.aerospike.PutBins(writePolicy, record.Key, bins...)
}

func registerUDFs(client *as.Client) error {
	for _ = range time.Tick(100 * time.Millisecond) {
		udfs, err := client.ListUDF(nil)
		if err != nil {
			return err
		}
		for _, udf := range udfs {
			if udf.Filename == "setupDone.lua" {
				return nil
			}
		}
	}

	return nil
}

func shouldCompress(body []byte) bool {
	// Don't compress small payloads
	return len(body) > CompressionSizeThreshold
}

func decompress(body []byte, outlen int) ([]byte, error) {
	decompressed := make([]byte, outlen)

	err := lz4.Uncompress(body, decompressed)
	if err != nil {
		return nil, err
	}

	return decompressed, nil
}

func compress(body []byte) ([]byte, error) {
	cbody := make([]byte, lz4.CompressBound(body))
	sz, err := lz4.Compress(body, cbody)
	if err != nil {
		return nil, err
	}
	if sz == 0 {
		return nil, errors.New("Failed to produce compressed output")
	}
	return cbody[:sz], nil
}
