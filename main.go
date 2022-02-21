package main

import (
	"os"
	"strconv"
	"time"

	log "github.com/go-kit/kit/log"
	"github.com/go-kit/log/level"
	"github.com/signalfx/signalfx-go/signalflow"
)

func main() {

	logger := log.NewLogfmtLogger(os.Stderr)
	logger = log.With(logger, "ts", log.DefaultTimestampUTC)
	switch os.Getenv("SFX_LOG_LEVEL") {
	case "info":
		logger = level.NewFilter(logger, level.AllowInfo())
	case "warn":
		logger = level.NewFilter(logger, level.AllowWarn())
	case "error":
		logger = level.NewFilter(logger, level.AllowError())
	default:
		logger = level.NewFilter(logger, level.AllowAll())
	}

	// Play with these for testing
	lateArrivalSeconds := 20 // the number of seconds after a resolution window timestamp should be expected to show up in the data and should be considered late if not there and therefore deem the results incomplete
	// lateArrivalSeconds is only relevant if lookBackInSecondsEnd is < than it
	resolutionInSeconds := 240
	lookBackInSeconds := 3600
	lookBackInSecondsEnd := 0
	flow := "A = data('node_load1').publish(label='A')"
	timeoutSecs := 300
	cycleTimeSeconds := 20 // how often to start a new computation - we wont start a computation if the previous one is running

	client, err := signalflow.NewClient(
		signalflow.StreamURLForRealm(os.Getenv("SFX_REALM")),
		signalflow.AccessToken(os.Getenv("SFX_TOKEN")),
		//signalflow.WriteTimeout(20*time.Second),
	//	signalflow.ReadTimeout(60*time.Second),
	//	signalflow.MetadataTimeout(40*time.Second),
	)
	if err != nil {
		level.Error(logger).Log("msg", "Failed to create SFX client "+err.Error())
		panic("Failed to create SFX client " + err.Error())
	}

	// fixedOlderTime := time.Date(2022, time.Month(2), 17, 10, 0, 0, 0, time.Local)
	for {
		start := time.Now().Unix()
		computationJobId := "not_set"
		data, err := client.Execute(&signalflow.ExecuteRequest{
			Program:      flow,
			StopMs:       time.Now().Add(-1*time.Second*time.Duration(lookBackInSecondsEnd)).Unix() * 1000,
			StartMs:      time.Now().Add(-1*time.Second*time.Duration(lookBackInSeconds)).Unix() * 1000,
			Immediate:    true,
			ResolutionMs: int64(resolutionInSeconds * 1000),
		})
		if err != nil {
			level.Error(logger).Log("msg", "Failed to execute request "+err.Error())
		}

		// We need to kill the SFX comp at some point in case bad flow makes it thru
		endtimer := time.NewTimer(time.Duration(timeoutSecs) * time.Second)
		go func() {
			<-endtimer.C
			if data != nil && !data.IsFinished() {
				level.Error(logger).Log("msg", "The computation for job ID "+computationJobId+" timed out so killing it")
				data.Stop()
			}
		}()

		if data != nil {
			computationJobId = data.Handle()
			if computationJobId == "" {
				level.Warn(logger).Log("msg", "The job ID is empty")
			} else {
				level.Info(logger).Log("msg", "Computation started for job "+computationJobId)
			}
			messageCount := 0
			timestamps := map[int64]int{}
			mtsCount := 0
			earliestTimestamp := time.Now().Add(10000 * time.Hour)
			latestTimestamp := time.Now().Add(-10000 * time.Hour)
			for msg := range data.Data() {
				messageCount++
				if len(msg.Payloads) > 0 {

					for _, pl := range msg.Payloads {
						if _, ok := timestamps[msg.Timestamp().UnixMilli()]; !ok {
							timestamps[msg.Timestamp().UnixMilli()] = 1
						} else {
							timestamps[msg.Timestamp().UnixMilli()]++
						}
						if earliestTimestamp.After(msg.Timestamp()) {
							earliestTimestamp = msg.Timestamp()
						}
						if latestTimestamp.Before(msg.Timestamp()) {
							latestTimestamp = msg.Timestamp()
						}
						meta := data.TSIDMetadata(pl.TSID)
						if len(meta.CustomProperties) == 0 {
							level.Warn(logger).Log("msg", "No meta properties for job "+computationJobId)
						} else {
							mtsCount = mtsCount + len(meta.CustomProperties)
							level.Debug(logger).Log("msg", "Got "+strconv.Itoa(len(meta.CustomProperties))+" meta properties for job "+computationJobId)
						}
					}
				} else {
					level.Warn(logger).Log("msg", "No payloads in message for job "+computationJobId+" with timestamp "+msg.Timestamp().String())
				}

				if data.IsFinished() {
					level.Debug(logger).Log("msg", "Computation finished for job "+computationJobId)
					break
				}
			}

			if messageCount == 0 {
				level.Error(logger).Log("msg", "No messages from job "+computationJobId)
			} else {
				data.Lag()
				level.Info(logger).Log("msg", "Got "+strconv.Itoa(mtsCount)+" datapoints in "+strconv.Itoa(messageCount)+" messages from job "+computationJobId+" with timestamps from: "+earliestTimestamp.String()+" to "+latestTimestamp.String()+". Resolution used was "+strconv.Itoa(int(data.Resolution().Seconds()))+" seconds. Mean timestamp message count is: "+strconv.FormatFloat(float64(messageCount)/float64(len(timestamps)), 'f', 2, 64))
			}

			// Were the results complete?
			// Start at the earliest timestamp - if we have it - if we dont its incomplete anyway!
			// The earlient timestamp will always be a multiple of the resolution since the top of the previous hour that can be fitted in
			incomplete := false
			if len(timestamps) == 0 {
				incomplete = true
			} else {
				// the time between first timestamp and lookback (or lateArrivalSeconds if greater)
				end := lateArrivalSeconds
				if lateArrivalSeconds < lookBackInSecondsEnd {
					end = lookBackInSecondsEnd
				}
				expectedTimestamps := int(time.Now().Add(-1*time.Duration(end)*time.Second).Sub(earliestTimestamp).Seconds() / data.Resolution().Seconds())

				level.Debug(logger).Log("msg", "Duration of window in seconds is: "+strconv.Itoa(int(time.Now().Add(-1*time.Duration(end)*time.Second).Sub(earliestTimestamp).Seconds())))
				level.Debug(logger).Log("msg", "Expected timestamps is: "+strconv.Itoa(expectedTimestamps))

				if (len(timestamps) < expectedTimestamps) || float64(messageCount%len(timestamps)) > 0 {
					incomplete = true
				}
			}
			if incomplete {
				level.Error(logger).Log("msg", "Job "+computationJobId+" returned incomplete results")
			}

		}

		if data.Err() != nil {
			level.Error(logger).Log("msg", "Error in computation for job "+computationJobId+", Error is: "+data.Err().Error())
		}

		// sleep for what is left of the cycle
		remaining := cycleTimeSeconds - int(time.Now().Unix()-start)
		if remaining > 0 {
			level.Info(logger).Log("msg", "Sleeping for "+strconv.Itoa(remaining)+" seconds")
			time.Sleep(time.Duration(remaining) * time.Second)
		}
	}
}
