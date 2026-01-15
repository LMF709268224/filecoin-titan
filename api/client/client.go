package client

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"net/url"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/Filecoin-Titan/titan/api"
	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"

	"github.com/filecoin-project/go-jsonrpc"

	"github.com/Filecoin-Titan/titan/lib/rpcenc"
)

var (
	defaultHTTP3Client *http.Client
	http3Clients       = make(map[*quic.Transport]*http.Client)
	http3ClientsMu     sync.RWMutex
)

// NewScheduler creates a new http jsonrpc client.
func NewScheduler(ctx context.Context, addr string, requestHeader http.Header, opts ...jsonrpc.Option) (api.Scheduler, jsonrpc.ClientCloser, error) {
	pushURL, err := getPushURL(addr)
	if err != nil {
		return nil, nil, err
	}

	// TODO server not support https now
	pushURL = strings.Replace(pushURL, "https", "http", 1)

	rpcOpts := []jsonrpc.Option{rpcenc.ReaderParamEncoder(pushURL), jsonrpc.WithErrors(api.RPCErrors)}
	if len(opts) > 0 {
		rpcOpts = append(rpcOpts, opts...)
	}

	var res api.SchedulerStruct
	closer, err := jsonrpc.NewMergeClient(ctx, addr, "titan",
		api.GetInternalStructs(&res),
		requestHeader,
		rpcOpts...,
	)

	return &res, closer, err
}

func getPushURL(addr string) (string, error) {
	pushURL, err := url.Parse(addr)
	if err != nil {
		return "", err
	}
	switch pushURL.Scheme {
	case "ws":
		pushURL.Scheme = "http"
	case "wss":
		pushURL.Scheme = "https"
	}
	///rpc/v0 -> /rpc/streams/v0/push

	pushURL.Path = path.Join(pushURL.Path, "../streams/v0/push")
	return pushURL.String(), nil
}

// NewCandidate creates a new http jsonrpc client for candidate
func NewCandidate(ctx context.Context, addr string, requestHeader http.Header, opts ...jsonrpc.Option) (api.Candidate, jsonrpc.ClientCloser, error) {
	pushURL, err := getPushURL(addr)
	if err != nil {
		return nil, nil, err
	}

	rpcOpts := []jsonrpc.Option{rpcenc.ReaderParamEncoder(pushURL), jsonrpc.WithErrors(api.RPCErrors), jsonrpc.WithHTTPClient(NewHTTP3Client())}
	if len(opts) > 0 {
		rpcOpts = append(rpcOpts, opts...)
	}

	addr = strings.Replace(addr, "ws", "https", 1)
	addr = strings.Replace(addr, "0.0.0.0", "localhost", 1)

	var res api.CandidateStruct
	closer, err := jsonrpc.NewMergeClient(ctx, addr, "titan",
		api.GetInternalStructs(&res),
		requestHeader,
		rpcOpts...,
	)

	return &res, closer, err
}

func NewEdge(ctx context.Context, addr string, requestHeader http.Header, opts ...jsonrpc.Option) (api.Edge, jsonrpc.ClientCloser, error) {
	pushURL, err := getPushURL(addr)
	if err != nil {
		return nil, nil, err
	}

	rpcOpts := []jsonrpc.Option{
		rpcenc.ReaderParamEncoder(pushURL),
		jsonrpc.WithErrors(api.RPCErrors),
		jsonrpc.WithNoReconnect(),
		jsonrpc.WithTimeout(30 * time.Second),
	}

	if len(opts) > 0 {
		rpcOpts = append(rpcOpts, opts...)
	}

	var res api.EdgeStruct
	closer, err := jsonrpc.NewMergeClient(ctx, addr, "titan",
		api.GetInternalStructs(&res),
		requestHeader,
		rpcOpts...,
	)

	return &res, closer, err
}

func NewL5(ctx context.Context, addr string, requestHeader http.Header, opts ...jsonrpc.Option) (api.L5, jsonrpc.ClientCloser, error) {
	pushURL, err := getPushURL(addr)
	if err != nil {
		return nil, nil, err
	}

	rpcOpts := []jsonrpc.Option{rpcenc.ReaderParamEncoder(pushURL), jsonrpc.WithErrors(api.RPCErrors)}
	if len(opts) > 0 {
		rpcOpts = append(rpcOpts, opts...)
	}

	var res api.CandidateStruct
	closer, err := jsonrpc.NewMergeClient(ctx, addr, "titan",
		api.GetInternalStructs(&res),
		requestHeader,
		rpcOpts...,
	)

	return &res, closer, err
}

// NewCommonRPCV0 creates a new http jsonrpc client.
func NewCommonRPCV0(ctx context.Context, addr string, requestHeader http.Header, opts ...jsonrpc.Option) (api.Common, jsonrpc.ClientCloser, error) {
	var res api.CommonStruct
	closer, err := jsonrpc.NewMergeClient(ctx, addr, "titan",
		api.GetInternalStructs(&res), requestHeader, opts...)

	return &res, closer, err
}

func NewLocator(ctx context.Context, addr string, requestHeader http.Header, opts ...jsonrpc.Option) (api.Locator, jsonrpc.ClientCloser, error) {
	pushURL, err := getPushURL(addr)
	if err != nil {
		return nil, nil, err
	}

	rpcOpts := []jsonrpc.Option{rpcenc.ReaderParamEncoder(pushURL), jsonrpc.WithNoReconnect(), jsonrpc.WithTimeout(30 * time.Second)}
	if len(opts) > 0 {
		rpcOpts = append(rpcOpts, opts...)
	}

	var res api.LocatorStruct
	closer, err := jsonrpc.NewMergeClient(ctx, addr, "titan",
		api.GetInternalStructs(&res),
		requestHeader,
		rpcOpts...,
	)

	return &res, closer, err
}

func NewHTTP3Client() *http.Client {
	if defaultHTTP3Client != nil {
		return defaultHTTP3Client
	}

	defaultHTTP3Client = &http.Client{
		Transport: &http3.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
			QUICConfig: &quic.Config{
				MaxIncomingStreams:             10,
				MaxIncomingUniStreams:          10,
				InitialStreamReceiveWindow:     16 * 1024,
				InitialConnectionReceiveWindow: 32 * 1024,
				MaxStreamReceiveWindow:         64 * 1024,
				MaxConnectionReceiveWindow:     128 * 1024,
				KeepAlivePeriod:                30 * time.Second,
			},
		},
	}

	return defaultHTTP3Client
}

// NewHTTP3ClientWithPacketConn new http3 client for nat trave
func NewHTTP3ClientWithPacketConn(transport *quic.Transport) (*http.Client, error) {
	http3ClientsMu.RLock()
	client, ok := http3Clients[transport]
	http3ClientsMu.RUnlock()
	if ok {
		return client, nil
	}

	http3ClientsMu.Lock()
	defer http3ClientsMu.Unlock()

	// Double check after lock
	if client, ok := http3Clients[transport]; ok {
		return client, nil
	}

	dial := func(ctx context.Context, addr string, tlsCfg *tls.Config, cfg *quic.Config) (quic.EarlyConnection, error) {
		remoteAddr, err := net.ResolveUDPAddr("udp", addr)
		if err != nil {
			return nil, err
		}

		return transport.DialEarly(ctx, remoteAddr, tlsCfg, cfg)
	}

	roundTripper := &http3.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
		QUICConfig: &quic.Config{
			MaxIncomingStreams:             10,
			MaxIncomingUniStreams:          10,
			InitialStreamReceiveWindow:     16 * 1024,
			InitialConnectionReceiveWindow: 32 * 1024,
			MaxStreamReceiveWindow:         64 * 1024,
			MaxConnectionReceiveWindow:     128 * 1024,
			KeepAlivePeriod:                30 * time.Second,
		},
		Dial: dial,
	}

	client = &http.Client{Transport: roundTripper, Timeout: 20 * time.Second}
	http3Clients[transport] = client

	return client, nil
}
