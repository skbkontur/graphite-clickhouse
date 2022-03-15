module github.com/lomik/graphite-clickhouse

go 1.14

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/bradfitz/gomemcache v0.0.0-20220106215444-fb4bf637b56d
	github.com/dgryski/go-expirecache v0.0.0-20170314133854-743ef98b2adb
	github.com/go-graphite/carbonapi v0.0.0-20180220165555-9db1310e484a
	github.com/go-graphite/protocol v0.4.3
	github.com/gogo/protobuf v1.2.1
	github.com/golang/snappy v0.0.3
	github.com/google/renameio v0.1.0 // indirect
	github.com/lomik/graphite-pickle v0.0.0-20171221213606-614e8df42119
	github.com/lomik/og-rek v0.0.0-20170411191824-628eefeb8d80 // indirect
	github.com/lomik/stop v0.0.0-20161127103810-188e98d969bd // indirect
	github.com/lomik/zapwriter v0.0.0-20210624082824-c1161d1eb463
	github.com/msaf1980/go-stringutils v0.0.15
	github.com/pelletier/go-toml v1.9.2-0.20210512132240-d08347058532
	github.com/pkg/errors v0.9.1
	github.com/prometheus/common v0.4.1
	github.com/prometheus/prometheus v1.8.2-0.20190814100549-343d8d75fd76
	github.com/stretchr/testify v1.7.0
	go.uber.org/tools v0.0.0-20190618225709-2cfd321de3ee // indirect
	go.uber.org/zap v1.17.0
	golang.org/x/lint v0.0.0-20190301231843-5614ed5bae6f // indirect; prometheus
	golang.org/x/sync v0.0.0-20190227155943-e225da77a7e6 // indirect; prometheus
	golang.org/x/tools v0.0.0-20190312170243-e65039ee4138 // indirect; prometheus
)
