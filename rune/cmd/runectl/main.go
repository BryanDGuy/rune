// Command runectl is a command-line client for a Rune node or cluster.
//
//	runectl get <key>                stream a value to stdout
//	runectl set <key> [file]         store a value from a file or stdin (-)
//	runectl delete <key>...          delete one or more keys
//	runectl exists <key>...          count how many keys are present
//	runectl info                     print server storage and cache stats
//	runectl ping                     check connectivity
//	runectl route <key>              print which node owns a key
//
// The target node defaults to localhost:7946 and can be set per command with
// -addr or the RUNE_ADDR environment variable. Pointed at any node in a
// cluster, requests are routed to the owner by server-side forwarding.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	runev1 "github.com/bryandguy/rune/rune/internal/gen/rune/v1"
	runesdk "github.com/bryandguy/rune/sdk/go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const defaultAddr = "localhost:7946"

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	cmds := map[string]func([]string) error{
		"get":    cmdGet,
		"set":    cmdSet,
		"delete": cmdDelete,
		"exists": cmdExists,
		"info":   cmdInfo,
		"ping":   cmdPing,
		"route":  cmdRoute,
	}

	name := os.Args[1]
	if name == "-h" || name == "--help" || name == "help" {
		usage()
		return
	}
	run, ok := cmds[name]
	if !ok {
		fmt.Fprintln(os.Stderr, "unknown command")
		usage()
		os.Exit(2)
	}
	if err := run(os.Args[2:]); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `runectl — command-line client for Rune

Usage: runectl <command> [flags]

Commands:
  get <key>           stream a value to stdout
  set <key> [file]    store a value from a file, or stdin if omitted/"-"
  delete <key>...     delete one or more keys
  exists <key>...     count how many of the given keys are present
  info                print server storage and cache stats
  ping                check connectivity
  route <key>         print which node owns a key (its x-rune-owner)

Connection (per command):
  -addr string   node address (default "localhost:7946", or $RUNE_ADDR)
`)
}

func dial(fs *flag.FlagSet) (*grpc.ClientConn, error) {
	addr := fs.Lookup("addr").Value.String()
	return grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
}

func newFlagSet(name string) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ExitOnError)
	def := defaultAddr
	if v := os.Getenv("RUNE_ADDR"); v != "" {
		def = v
	}
	fs.String("addr", def, "node address")
	return fs
}

func cmdGet(args []string) error {
	fs := newFlagSet("get")
	_ = fs.Parse(args)
	if fs.NArg() != 1 {
		return errors.New("usage: runectl get <key>")
	}
	conn, err := dial(fs)
	if err != nil {
		return err
	}
	defer conn.Close()

	r, err := runesdk.NewClient(conn).Get(context.Background(), fs.Arg(0))
	if errors.Is(err, runesdk.ErrNotFound) {
		return fmt.Errorf("key %q not found", fs.Arg(0))
	}
	if err != nil {
		return err
	}
	defer r.Close()
	_, err = io.Copy(os.Stdout, r)
	return err
}

func cmdSet(args []string) error {
	fs := newFlagSet("set")
	ttl := fs.Duration("ttl", 0, "time-to-live, e.g. 10m (0 = no expiry)")
	_ = fs.Parse(args)
	if fs.NArg() < 1 || fs.NArg() > 2 {
		return errors.New(`usage: runectl set <key> [file]  (omit file or use "-" for stdin)`)
	}

	src := io.Reader(os.Stdin)
	if fs.NArg() == 2 && fs.Arg(1) != "-" {
		f, err := os.Open(fs.Arg(1))
		if err != nil {
			return err
		}
		defer f.Close()
		src = f
	}

	conn, err := dial(fs)
	if err != nil {
		return err
	}
	defer conn.Close()

	var opts *runesdk.SetOptions
	if *ttl > 0 {
		opts = &runesdk.SetOptions{TTL: *ttl}
	}
	return runesdk.NewClient(conn).Set(context.Background(), fs.Arg(0), src, opts)
}

func cmdDelete(args []string) error {
	fs := newFlagSet("delete")
	_ = fs.Parse(args)
	if fs.NArg() < 1 {
		return errors.New("usage: runectl delete <key> [<key>...]")
	}
	conn, err := dial(fs)
	if err != nil {
		return err
	}
	defer conn.Close()

	_, err = runev1.NewRuneServiceClient(conn).Delete(context.Background(), &runev1.DeleteRequest{Keys: fs.Args()})
	return err
}

func cmdExists(args []string) error {
	fs := newFlagSet("exists")
	_ = fs.Parse(args)
	if fs.NArg() < 1 {
		return errors.New("usage: runectl exists <key> [<key>...]")
	}
	conn, err := dial(fs)
	if err != nil {
		return err
	}
	defer conn.Close()

	resp, err := runev1.NewRuneServiceClient(conn).Exists(context.Background(), &runev1.ExistsRequest{Keys: fs.Args()})
	if err != nil {
		return err
	}
	fmt.Printf("%d of %d key(s) present\n", resp.Count, fs.NArg())
	return nil
}

func cmdInfo(args []string) error {
	fs := newFlagSet("info")
	_ = fs.Parse(args)
	conn, err := dial(fs)
	if err != nil {
		return err
	}
	defer conn.Close()

	resp, err := runev1.NewRuneServiceClient(conn).Info(context.Background(), &runev1.InfoRequest{})
	if err != nil {
		return err
	}
	fmt.Printf("storage:     %d / %d bytes\n", resp.StorageUsedBytes, resp.StorageMaxBytes)
	fmt.Printf("cache:       %d hits, %d misses\n", resp.CacheHits, resp.CacheMisses)
	fmt.Printf("connections: %d active\n", resp.ActiveConnections)
	fmt.Printf("evictions:   %d total\n", resp.EvictionsTotal)
	return nil
}

func cmdPing(args []string) error {
	fs := newFlagSet("ping")
	_ = fs.Parse(args)
	conn, err := dial(fs)
	if err != nil {
		return err
	}
	defer conn.Close()

	resp, err := runev1.NewRuneServiceClient(conn).Ping(context.Background(), &runev1.PingRequest{})
	if err != nil {
		return err
	}
	fmt.Println(resp.Message)
	return nil
}

func cmdRoute(args []string) error {
	fs := newFlagSet("route")
	_ = fs.Parse(args)
	if fs.NArg() != 1 {
		return errors.New("usage: runectl route <key>")
	}
	conn, err := dial(fs)
	if err != nil {
		return err
	}
	defer conn.Close()

	// Read only the response header (x-rune-owner) and cancel before the value transfers.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := runev1.NewRuneServiceClient(conn).Get(ctx, &runev1.GetRequest{Key: fs.Arg(0)})
	if err != nil {
		return err
	}
	md, err := stream.Header()
	if err != nil {
		return err
	}
	if owner := md.Get(metaOwner); len(owner) > 0 {
		fmt.Printf("%s -> %s\n", fs.Arg(0), owner[0])
		return nil
	}
	fmt.Printf("%s -> served locally (single-node, or the target node owns it)\n", fs.Arg(0))
	return nil
}

const metaOwner = "x-rune-owner"
