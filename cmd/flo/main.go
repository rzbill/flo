package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	serverrun "github.com/rzbill/flo/internal/cmd/server"
	cfgpkg "github.com/rzbill/flo/internal/config"
	pebblestore "github.com/rzbill/flo/internal/storage/pebble"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "init":
		fmt.Println("flo init: nothing to do yet")
	case "server":
		if len(os.Args) >= 3 && os.Args[2] == "start" {
			serverStart(os.Args[3:])
		} else {
			usage()
			os.Exit(2)
		}
	case "ns":
		nsCmd()
	case "channel":
		channelCmd()
	case "publish":
		publishCmd()
	case "subscribe":
		subscribeCmd()
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Println("Usage: flo [init|server start|ns create|channel create|publish|subscribe]")
}

func apiURL() string {
	if v := os.Getenv("FLO_HTTP"); v != "" {
		return v
	}
	return "http://127.0.0.1:8080"
}

func nsCmd() {
	fs := flag.NewFlagSet("ns", flag.ExitOnError)
	create := fs.Bool("create", false, "Create namespace")
	name := fs.String("name", "default", "Namespace name")
	_ = fs.Parse(os.Args[2:])
	if *create {
		body := map[string]string{"namespace": *name}
		b, _ := json.Marshal(body)
		resp, err := http.Post(apiURL()+"/v1/ns/create", "application/json", bytes.NewReader(b))
		if err != nil {
			fmt.Println("error:", err)
			os.Exit(1)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		fmt.Println("status:", resp.Status)
		return
	}
	usage()
}

func channelCmd() {
	fs := flag.NewFlagSet("channel", flag.ExitOnError)
	create := fs.Bool("create", false, "Create channel")
	ns := fs.String("ns", "default", "Namespace")
	name := fs.String("name", "", "Channel name")
	parts := fs.Int("partitions", 0, "Partitions override")
	_ = fs.Parse(os.Args[2:])
	if *create {
		body := map[string]any{"namespace": *ns, "channel": *name, "partitions": *parts}
		b, _ := json.Marshal(body)
		resp, err := http.Post(apiURL()+"/v1/channels/create", "application/json", bytes.NewReader(b))
		if err != nil {
			fmt.Println("error:", err)
			os.Exit(1)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		fmt.Println("status:", resp.Status)
		return
	}
	usage()
}

func publishCmd() {
	fs := flag.NewFlagSet("publish", flag.ExitOnError)
	ns := fs.String("ns", "default", "Namespace")
	ch := fs.String("channel", "", "Channel")
	data := fs.String("data", "", "Payload data")
	_ = fs.Parse(os.Args[2:])
	body := map[string]any{"namespace": *ns, "channel": *ch, "payload": []byte(*data)}
	b, _ := json.Marshal(body)
	resp, err := http.Post(apiURL()+"/v1/channels/publish", "application/json", bytes.NewReader(b))
	if err != nil {
		fmt.Println("error:", err)
		os.Exit(1)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	fmt.Println("status:", resp.Status)
}

func subscribeCmd() {
	fs := flag.NewFlagSet("subscribe", flag.ExitOnError)
	ns := fs.String("ns", "default", "Namespace")
	ch := fs.String("channel", "", "Channel")
	_ = fs.Parse(os.Args[2:])
	url := fmt.Sprintf("%s/v1/channels/subscribe?namespace=%s&channel=%s", apiURL(), *ns, *ch)
	resp, err := http.Get(url)
	if err != nil {
		fmt.Println("error:", err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	buf := make([]byte, 4096)
	for {
		n, err := resp.Body.Read(buf)
		if n > 0 {
			os.Stdout.Write(buf[:n])
		}
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Println("stream error:", err)
			break
		}
	}
}

func serverStart(args []string) {
	fs := flag.NewFlagSet("server start", flag.ExitOnError)
	dataDir := fs.String("data-dir", "./data", "Data directory")
	grpcAddr := fs.String("grpc", ":50051", "gRPC listen address")
	httpAddr := fs.String("http", ":8080", "HTTP listen address")
	fsync := fs.String("fsync", "always", "Fsync mode: always|never")
	_ = fs.Parse(args)

	mode := pebblestore.FsyncModeAlways
	switch *fsync {
	case "never":
		mode = pebblestore.FsyncModeNever
	case "always":
		mode = pebblestore.FsyncModeAlways
	default:
		fmt.Println("invalid --fsync; use always|never")
		os.Exit(2)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	cfg := cfgpkg.Default()
	fmt.Println("starting flo server on", *grpcAddr, *httpAddr)
	if err := serverrun.Run(ctx, serverrun.Options{DataDir: *dataDir, GRPCAddr: *grpcAddr, HTTPAddr: *httpAddr, Fsync: mode, Config: cfg}); err != nil {
		fmt.Println("server error:", err)
		os.Exit(1)
	}
	// brief delay to allow logs flush
	time.Sleep(100 * time.Millisecond)
}
