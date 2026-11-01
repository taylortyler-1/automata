module github.com/taylortyler-1/automata/examples/openai

go 1.26.2

require (
	github.com/taylortyler-1/automata v0.5.2
	github.com/taylortyler-1/automata/extensions/openai v0.5.2
)

replace (
	github.com/taylortyler-1/automata => ../..
	github.com/taylortyler-1/automata/extensions/openai => ../../extensions/openai
)
