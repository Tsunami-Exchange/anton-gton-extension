package main

import (
	"github.com/xssnick/gton/cmd/node/node"

	"github.com/Tsunami-Exchange/anton-gton-extension/extension"
)

func main() {
	node.Run(extension.New)
}
