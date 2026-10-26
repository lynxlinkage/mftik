// Command mftik-pack-check pulls one strategy image with mftiko's artifact
// package, unpacks it, and prints a JSON report. It is copied into a pinned
// mftiko checkout so it can import internal/platform/artifact (E04-D076).
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
		fmt.Fprintf(os.Stderr, "mftik-pack-check: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	fs := flag.NewFlagSet("mftik-pack-check", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	registry := fs.String("registry", "", "host[:port]")
	repository := fs.String("repository", "", "repository without the host")
	digest := fs.String("digest", "", "image manifest digest")
	data := fs.String("data", "", "artifact data directory")
	if err := fs.Parse(os.Args[1:]); err != nil {
		return err
	}
	if *registry == "" || *repository == "" || *digest == "" || *data == "" || fs.NArg() != 0 {
		return errors.New("registry, repository, digest, and data are required")
	}

	client, err := artifact.Open(artifact.Registry{
		Address:   *registry,
		PlainHTTP: true,
	}, *data)
	if err != nil {
		return err
	}
	fetched, err := client.Pull(context.Background(), artifact.Request{
		Repository: *repository,
		Digest:     *digest,
		Kind:       artifact.KindStrategy,
		OS:         "linux",
		Arch:       "amd64",
		Contracts:  []string{"worker.v1", "stsctl.v1", "md.v1", "td.v1"},
		MaxBytes:   artifact.DefaultMaxBytes,
	})
	if err != nil {
		return err
	}
	if len(fetched.Layers) != 3 {
		return fmt.Errorf("%d layers", len(fetched.Layers))
	}
	rootfs, err := client.UnpackImage(context.Background(), fetched.Digest, fetched.Layers, artifact.UnpackLimits{})
	if err != nil {
		return err
	}
	if len(fetched.Manifest.Entrypoint) == 0 {
		return errors.New("no entrypoint")
	}
	exe := filepath.Join(rootfs, filepath.FromSlash(fetched.Manifest.Entrypoint[0]))
	if _, err := os.Stat(exe); err != nil {
		return fmt.Errorf("entrypoint: %w", err)
	}
	var total int64
	for _, layer := range fetched.Layers {
		info, err := os.Stat(layer.Path)
		if err != nil {
			return err
		}
		total += info.Size()
	}
	env := fetched.Manifest.Env
	if env == nil {
		env = map[string]string{}
	}
	report := map[string]any{
		"media_type":  artifact.MediaTypeConfig,
		"schema":      fetched.Manifest.Schema,
		"kind":        fetched.Manifest.Kind,
		"name":        fetched.Manifest.Name,
		"runtime":     fetched.Manifest.Runtime,
		"os":          fetched.Manifest.OS,
		"arch":        fetched.Manifest.Arch,
		"sdk_lang":    fetched.Manifest.SDK.Lang,
		"sdk_version": fetched.Manifest.SDK.Version,
		"contracts":   fetched.Manifest.Contracts,
		"entrypoint":  fetched.Manifest.Entrypoint,
		"env":         env,
		"layer_bytes": total,
		"rootfs":      rootfs,
		"digest":      fetched.Digest,
	}
	encoded, err := json.Marshal(report)
	if err != nil {
		return err
	}
	fmt.Printf("%s\n", encoded)
	return nil
}
