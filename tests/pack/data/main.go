// Command mftik-data-check pulls one data artifact with mftiko's artifact
// package and unpacks it read-only, the way the STS agent stages a session
// input, then prints a JSON report. It is copied into a pinned mftiko
// checkout so it can import internal/platform/artifact (E04-D076).
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/BullionBear/mftiko/internal/platform/artifact"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "mftik-data-check: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	fs := flag.NewFlagSet("mftik-data-check", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	registry := fs.String("registry", "", "host[:port]")
	repository := fs.String("repository", "", "repository without the host")
	digest := fs.String("digest", "", "data manifest digest")
	data := fs.String("data", "", "artifact data directory")
	dest := fs.String("dest", "", "input directory to create, MFTIKO_INPUT_DIR/<name>")
	if err := fs.Parse(os.Args[1:]); err != nil {
		return err
	}
	if *registry == "" || *repository == "" || *digest == "" || *data == "" || *dest == "" || fs.NArg() != 0 {
		return errors.New("registry, repository, digest, data, and dest are required")
	}

	client, err := artifact.Open(artifact.Registry{
		Address:   *registry,
		PlainHTTP: true,
	}, *data)
	if err != nil {
		return err
	}
	fetched, err := client.PullData(context.Background(), artifact.DataRequest{
		Repository: *repository,
		Digest:     *digest,
	})
	if err != nil {
		return err
	}
	// The STS agent creates the parent with 0700 and unpacks into it (internal/sts/agent/inputs.go).
	if err := os.MkdirAll(filepath.Dir(*dest), 0o700); err != nil {
		return err
	}
	if err := artifact.UnpackReadOnly(context.Background(), []string{fetched.Layer.Path}, *dest, artifact.UnpackLimits{}); err != nil {
		return err
	}
	report := map[string]any{
		"digest":     fetched.Digest,
		"schema":     fetched.Config.Schema,
		"has_source": fetched.Config.Source != nil,
		"dest":       *dest,
	}
	encoded, err := json.Marshal(report)
	if err != nil {
		return err
	}
	fmt.Printf("%s\n", encoded)
	return nil
}
