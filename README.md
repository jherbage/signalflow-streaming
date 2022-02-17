Experiment with the [signalflow client](https://github.com/signalfx/signalfx-go/tree/master/signalflow)

Expects the token and realm to be made available as env vars SFX_TOKEN and SFX_REALM.
Set the debug level with env variable SFX_LOG_LEVEL set to error,warn,info or leave blank for debug
The flow to execute, frequency, start for the window and the resolution are set in the code. Stop is just set to now ie dont wait for future metrics.

to run:
```go run main.go```
