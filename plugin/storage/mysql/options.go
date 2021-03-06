// Copyright (c) 2018 The Jaeger Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mysql

import (
	"flag"
	"fmt"

	"github.com/spf13/viper"

	"github.com/jaegertracing/jaeger/pkg/mysql/config"
)

const (
	url      = "mysql.url"
	user     = "mysql.user"
	password = "mysql.password"
	host     = "mysql.host"
	port     = "mysql.port"
	db       = "mysql.db"
)

// Options stores the configuration entries for this storage
type Options struct {
	Configuration config.Configuration
}

// AddFlags from this storage to the CLI
func (opt *Options) AddFlags(flagSet *flag.FlagSet) {
	flagSet.String(url, opt.Configuration.Url, "The mysql cluster url")
	flagSet.String(user, opt.Configuration.User, "The mysql cluster user")
	flagSet.String(password, opt.Configuration.Password, "The mysql cluster password")
	flagSet.String(host, opt.Configuration.Host, "The mysql cluster host")
	flagSet.Int(port, opt.Configuration.Port, "The mysql cluster port")
	flagSet.String(db, opt.Configuration.Db, "The mysql cluster db")
}

// InitFromViper initializes the options struct with values from Viper
func (opt *Options) InitFromViper(v *viper.Viper) {
	opt.Configuration.Url = v.GetString(url)
	opt.Configuration.User = v.GetString(user)
	opt.Configuration.Password = v.GetString(password)
	opt.Configuration.Host = v.GetString(host)
	opt.Configuration.Port = v.GetInt(port)
	opt.Configuration.Db = v.GetString(db)
	if opt.Configuration.Url != "" {
		opt.Configuration.Url = fmt.Sprintf(
			"%s:%s@tcp(%s:%d)/%s?charset=utf8", opt.Configuration.User, 
												opt.Configuration.Password,
												opt.Configuration.Host,
												opt.Configuration.Port,
												opt.Configuration.Db)
	}
}
