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
	resolutionInSeconds := 300
	lookBackInSeconds := 3600
	flow := "A = data('node_load1').publish(label='A')"
	timeoutSecs := 300
	cycleTimeSeconds := 20 // how often to start a new computation - we wont start a computation if the previous one is running

	client, err := signalflow.NewClient(
		signalflow.StreamURLForRealm(os.Getenv("SFX_REALM")),
		signalflow.AccessToken(os.Getenv("SFX_TOKEN")),
		signalflow.WriteTimeout(20*time.Second),
		signalflow.ReadTimeout(60*time.Second),
		signalflow.MetadataTimeout(20*time.Second),
	)
	if err != nil {
		level.Error(logger).Log("msg", "Failed to create SFX client "+err.Error())
		panic("Failed to create SFX client " + err.Error())
	}

	for {
		start := time.Now().Unix()
		computationJobId := "not_set"
		data, err := client.Execute(&signalflow.ExecuteRequest{
			Program:      flow,
			StopMs:       time.Now().Unix() * 1000,
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
			level.Error(logger).Log("msg", "The computation timed out so killing it")
			data.Stop()
		}()

		if data != nil {
			computationJobId = data.Handle()
			if computationJobId == "" {
				level.Warn(logger).Log("msg", "The job ID is empty")
			} else {
				level.Info(logger).Log("msg", "Computation started for job "+computationJobId)
			}
			timestampCount := 0
			earliestTimestamp := time.Now().Add(10000 * time.Hour)
			latestTimestamp := time.Now().Add(-10000 * time.Hour)
			for msg := range data.Data() {
				timestampCount++
				if len(msg.Payloads) > 0 {

					for _, pl := range msg.Payloads {
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

			if timestampCount == 0 {
				level.Error(logger).Log("msg", "No messages from job "+computationJobId)
			} else {
				level.Info(logger).Log("msg", "Got "+strconv.Itoa(timestampCount)+" messages from job "+computationJobId+" with timestamps from: "+earliestTimestamp.String()+" to "+latestTimestamp.String())
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
