package inspect

import (
	_ "embed"
	"html/template"
)

var (
	//go:embed index.html
	indexFile string
	//nolint:gochecknoglobals // is the easiest way to initialise once the template
	indexTemplate = template.Must(template.New("").Parse(indexFile))
)
