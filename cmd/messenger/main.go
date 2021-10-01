package main

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/kr/pretty"
	"github.com/urfave/cli/v2"
	"gopkg.in/yaml.v2"
)

type Conf struct {
	Store Store `yaml:"store"`
}

type Store struct {
	Driver   string `yaml:"driver"`
	Postgres *Postgres
}

type Postgres struct {
	Host     string
	Port     uint
	User     string
	Password string
	Table    *string
	Schema   *string
}

func Run(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) error {
	app := &cli.App{
		Name:      "messenger",
		Usage:     "Publish messages",
		Reader:    stdin,
		Writer:    stdout,
		ErrWriter: stderr,
		Commands: []*cli.Command{
			{
				Name: "run",
				Flags: []cli.Flag{
					&cli.StringSliceFlag{
						Name:    "set",
						Aliases: []string{"s"},
					},
					&cli.PathFlag{
						Name:    "conf",
						Aliases: []string{"c"},
					},
				},
				Action: func(c *cli.Context) error {
					f, err := os.Open(c.Path("conf"))
					if err != nil {
						return err
					}
					var conf Conf
					if err := yaml.NewDecoder(f).Decode(&conf); err != nil {
						return err
					}

					pretty.Println(conf)

					return nil
				},
			},
		},
	}

	return app.RunContext(ctx, args)
}

func main() {
	ctx := context.Background()
	err := Run(ctx, os.Args, os.Stdin, os.Stdout, os.Stderr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %s", err)
		os.Exit(1)
	}
}
