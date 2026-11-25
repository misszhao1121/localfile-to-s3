package main

// Mode is a subcommand.
type Mode string

const (
	ModeMigrate Mode = "migrate"
	ModeVerify  Mode = "verify"
	ModeRetry   Mode = "retry"
	ModeStats   Mode = "stats"
	ModeReset   Mode = "reset"
	ModeWeb     Mode = "web"
	ModeCount   Mode = "count"
)
