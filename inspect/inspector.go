package inspect

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"text/template"

	"github.com/x4b1/messenger"
)

const defaultLimit = 25

//go:embed index.tmpl
var indexFile string

type Pagination struct {
	Page  int
	Limit int
}

type Query struct {
	Pagination
}

type Result struct {
	Total int
	Msgs  []*messenger.GenericMessage
}

type Store interface {
	Find(ctx context.Context, q *Query) (*Result, error)
}

func NewInspector(s Store) *Inspector {
	return &Inspector{s}
}

var _ http.Handler = (*Inspector)(nil)

type Inspector struct {
	s Store
}

func (i *Inspector) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	tmpl, err := template.New("index").Funcs(
		template.FuncMap{
			"prettyJson": func(b []byte) string {
				var prettyJSON bytes.Buffer
				err := json.Indent(&prettyJSON, b, "", "  ")
				if err != nil {
					log.Print(err)
				}
				return prettyJSON.String()
			},
			"nextPage": func(page int) int {
				return page + 1
			},
			"prevPage": func(page int) int {
				page--
				if page < 0 {
					return 0
				}

				return page
			},
		},
	).Parse(indexFile)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(err.Error()))
		return
	}

	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	if page == 0 {
		page = 1
	}

	res, err := i.s.Find(r.Context(), &Query{
		Pagination: Pagination{
			Page:  page,
			Limit: defaultLimit,
		},
	})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(err.Error()))
		return
	}

	if err := tmpl.Execute(w, struct {
		*Result
		Page int
	}{
		Result: res,
		Page:   page,
	}); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(err.Error()))
	}
}
