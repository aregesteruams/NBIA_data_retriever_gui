package main

import (
	"fmt"
	"github.com/DavidGamba/go-getoptions"
	"os"
	"path/filepath"
	"time"
)

var (
	TokenUrl = "https://services.cancerimagingarchive.net/nbia-api/oauth/token"
	ImageUrl = "https://services.cancerimagingarchive.net/nbia-api/services/v2/getImage"
	MetaUrl  = "https://services.cancerimagingarchive.net/nbia-api/services/v2/getSeriesMetaData"
)

// Options command line parameters
type Options struct {
	Input           string
	Output          string
	Proxy           string
	Concurrent      int
	Meta            bool
	Username        string
	Password        string
	Version         bool
	Debug           bool
	Help            bool
	MetaUrl         string
	TokenUrl        string
	ImageUrl        string
	SaveLog         bool
	Prompt          bool
	Force           bool
	SkipExisting    bool
	MaxRetries      int
	RetryDelay      time.Duration
	MaxConnsPerHost int
	ServerFriendly  bool
	RequestDelay    time.Duration
	NoMD5           bool
	NoDecompress    bool
	RefreshMetadata bool
	MetadataWorkers int
	Auth            string

	opt *getoptions.GetOpt
}

func InitOptions() *Options {
	opt := &Options{
		opt:             getoptions.New(),
		RetryDelay:      10 * time.Second,       // Server-friendly: 10 second initial retry delay
		MaxConnsPerHost: 8,                      // Balanced setting
		RequestDelay:    500 * time.Millisecond, // Server-friendly: delay between requests
		MetadataWorkers: 20,                     // Default metadata workers
	}

	setLogger(false, "")

	opt.opt.BoolVar(&opt.Help, "help", false, opt.opt.Alias("h"),
		opt.opt.Description("show help information"))
	opt.opt.BoolVar(&opt.Debug, "debug", false,
		opt.opt.Description("show more info"))
	opt.opt.BoolVar(&opt.SaveLog, "save-log", false,
		opt.opt.Description("save debug log info to file"))
	opt.opt.BoolVar(&opt.Version, "version", false, opt.opt.Alias("v"),
		opt.opt.Description("show version information"))
	opt.opt.StringVar(&opt.Input, "input", "", opt.opt.Alias("i"),
		opt.opt.Description("path to input tcia file"))
	opt.opt.StringVar(&opt.Output, "output", "./", opt.opt.Alias("o"),
		opt.opt.Description("Output directory for downloaded files"))
	opt.opt.StringVar(&opt.Proxy, "proxy", "", opt.opt.Alias("x"),
		opt.opt.Description("the proxy to use [http, socks5://user:passwd@host:port]"))
	opt.opt.IntVar(&opt.Concurrent, "processes", 2, opt.opt.Alias("p"),
		opt.opt.Description("start how many download at same time"))
	opt.opt.BoolVar(&opt.Meta, "meta", false, opt.opt.Alias("m"),
		opt.opt.Description("get Meta info of all files"))
	opt.opt.StringVar(&opt.Username, "user", "nbia_guest", opt.opt.Alias("u"),
		opt.opt.Description("username for control data"))
	opt.opt.BoolVar(&opt.Prompt, "prompt", false, opt.opt.Alias("w"),
		opt.opt.Description("input password for control data"))
	opt.opt.StringVar(&opt.Password, "passwd", "",
		opt.opt.Description("set password for control data in command line"))
	opt.opt.StringVar(&opt.TokenUrl, "token-url", TokenUrl,
		opt.opt.Description("the api url of login token"))
	opt.opt.StringVar(&opt.MetaUrl, "meta-url", MetaUrl,
		opt.opt.Description("the api url get meta data"))
	opt.opt.StringVar(&opt.ImageUrl, "image-url", ImageUrl,
		opt.opt.Description("the api url to download image data"))
	opt.opt.BoolVar(&opt.Force, "force", false, opt.opt.Alias("f"),
		opt.opt.Description("force re-download even if files exist"))
	opt.opt.BoolVar(&opt.SkipExisting, "skip-existing", false,
		opt.opt.Description("skip download if image file already exists"))
	opt.opt.IntVar(&opt.MaxRetries, "max-retries", 3,
		opt.opt.Description("maximum number of download retries"))
	opt.opt.IntVar(&opt.MaxConnsPerHost, "max-connections", 8,
		opt.opt.Description("maximum concurrent connections per host"))
	opt.opt.BoolVar(&opt.ServerFriendly, "server-friendly", false,
		opt.opt.Description("use extra conservative settings to avoid server issues"))
	opt.opt.BoolVar(&opt.NoMD5, "no-md5", false,
		opt.opt.Description("disable MD5 validation for downloaded files"))
	opt.opt.BoolVar(&opt.NoDecompress, "no-decompress", false,
		opt.opt.Description("keep downloaded files as ZIP archives (skip extraction)"))
	opt.opt.BoolVar(&opt.RefreshMetadata, "refresh-metadata", false,
		opt.opt.Description("force refresh all metadata from server (ignore cache)"))
	opt.opt.IntVar(&opt.MetadataWorkers, "metadata-workers", 20,
		opt.opt.Description("number of parallel metadata fetch workers"))
	opt.opt.StringVar(&opt.Auth, "auth", "",
		opt.opt.Description("path to JSON API key file for Gen3 authentication"))

	_, err := opt.opt.Parse(os.Args[1:])
	if err != nil {
		logger.Fatal(err)
	}

	// Apply server-friendly settings if enabled
	if opt.ServerFriendly {
		opt.Concurrent = 1
		opt.MaxConnsPerHost = 2
		opt.RetryDelay = 30 * time.Second
		opt.RequestDelay = 2 * time.Second
		opt.MetadataWorkers = 5  // Reduce metadata workers in server-friendly mode
		logger.Info("Server-friendly mode: Using extra conservative settings")
	}

	if opt.Debug || opt.SaveLog {
		setLogger(opt.Debug, filepath.Join(opt.Output, "progress.log"))
	}

	if opt.opt.Called("help") || len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "%s", opt.opt.Help())
		os.Exit(1)
	}

	// Validate incompatible options
	if !opt.NoMD5 && opt.NoDecompress {
		logger.Fatal("MD5 validation (default) and --no-decompress are incompatible. Use --no-md5 with --no-decompress.")
	}

	if opt.TokenUrl != "" && opt.TokenUrl != TokenUrl {
		TokenUrl = opt.TokenUrl
		logger.Infof("Using custom token url: %s", TokenUrl)
	}

	if opt.MetaUrl != "" && opt.MetaUrl != MetaUrl {
		MetaUrl = opt.MetaUrl
		logger.Infof("Using custom meta url: %s", MetaUrl)
	}

	// Set ImageUrl based on MD5 flag if not manually specified
	if opt.ImageUrl != ImageUrl && opt.ImageUrl != "" {
		// User specified a custom URL
		ImageUrl = opt.ImageUrl
		logger.Infof("Using custom image url: %s", ImageUrl)
	} else if !opt.NoMD5 {
		// Try v2 API first for MD5 support (will fallback to v1 if needed)
		ImageUrl = "https://services.cancerimagingarchive.net/nbia-api/services/v2/getImageWithMD5Hash"
		logger.Infof("Using MD5 validation endpoint (v2 with v1 fallback)")
	}
	// else use default ImageUrl (v2 getImage)

	if opt.Prompt {
		logger.Infof("Please input password for %s: ", opt.Username)
		_, err = fmt.Scanln(&opt.Password)
		if err != nil {
			logger.Fatalf("failed to scan prompt: %v", err)
		}
	}

	return opt
}
