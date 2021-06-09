package report

import (
	"log"
	"time"
)

type Log struct{ report }

func (Log) Error(err error) {
	log.Printf("error: %s\n", err.Error())
}

func (l Log) Report() {
	log.Printf("Report of %s execution:\n", l.stats.Time)
	log.Printf("\t- Took %d ms\n", time.Since(l.stats.Time).Milliseconds())
	log.Printf("\t- Got %d messages\n", l.stats.Total)
	log.Printf("\t- %d published messages\n", l.stats.Success)
	log.Printf("\t- %d failed messages\n", l.stats.Fail)
}
