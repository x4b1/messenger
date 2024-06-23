package inspect

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"html/template"
	"time"
)

var (
	//go:embed index.tmpl
	indexFile string
	//nolint:gochecknoglobals // is the easiest way to initialise once the template
	indexTemplate = template.Must(template.New("").Funcs(funcMap).Parse(indexFile))
	//nolint:gochecknoglobals // is the easiest way to initialise once the template
	funcMap = template.FuncMap{
		"prettyJson": prettyJSON,
		"nextPage":   nextPage,
		"prevPage":   prevPage,
		"formatDate": formatDate,
	}
)

func prettyJSON(b []byte) string {
	var prettyJSON bytes.Buffer
	err := json.Indent(&prettyJSON, b, "", "  ")
	if err != nil {
		return string(b)
	}
	return prettyJSON.String()
}

func nextPage(page int) int {
	return page + 1
}

func prevPage(page int) int {
	page--
	if page < 0 {
		return 0
	}

	return page
}

func formatDate(d time.Time) string {
	return d.Format(time.RFC3339)
}
