module github.com/taylortyler-1/automata/examples/claude

go 1.26.2

require (
	github.com/taylortyler-1/automata v0.5.2
	github.com/taylortyler-1/automata/extensions/claude v0.5.2
)

require (
	github.com/anthropics/anthropic-sdk-go v1.21.0 // indirect
	github.com/tidwall/gjson v1.18.0 // indirect
	github.com/tidwall/match v1.1.1 // indirect
	github.com/tidwall/pretty v1.2.1 // indirect
	github.com/tidwall/sjson v1.2.5 // indirect
)

replace (
	github.com/taylortyler-1/automata => ../..
	github.com/taylortyler-1/automata/extensions/claude => ../../extensions/claude
)
