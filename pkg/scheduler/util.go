/*
Copyright 2018 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package scheduler

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v2"

	"volcano.sh/volcano/pkg/scheduler/conf"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/plugins"
	"volcano.sh/volcano/pkg/util"
)

var defaultSchedulerConf = `
actions: "enqueue, allocate, backfill"
tiers:
- plugins:
  - name: priority
  - name: gang
  - name: conformance
- plugins:
  - name: overcommit
  - name: drf
  - name: predicates
  - name: proportion
  - name: nodeorder
`

func unmarshalSchedulerConf(confStr string) ([]framework.Action, []conf.Tier, []conf.Configuration, map[string]string, error) {
	var actions []framework.Action

	schedulerConf := &conf.SchedulerConfiguration{}

	if err := yaml.Unmarshal([]byte(confStr), schedulerConf); err != nil {
		return nil, nil, nil, nil, err
	}
	// Set default settings for each plugin if not set
	for i, tier := range schedulerConf.Tiers {
		// drf with hierarchy enabled
		hdrf := false
		// proportion enabled
		proportion := false
		for j := range tier.Plugins {
			if tier.Plugins[j].Name == "drf" &&
				tier.Plugins[j].EnabledHierarchy != nil &&
				*tier.Plugins[j].EnabledHierarchy {
				hdrf = true
			}
			if tier.Plugins[j].Name == "proportion" {
				proportion = true
			}
			plugins.ApplyPluginConfDefaults(&schedulerConf.Tiers[i].Plugins[j])
		}
		if hdrf && proportion {
			return nil, nil, nil, nil, fmt.Errorf("proportion and drf with hierarchy enabled conflicts")
		}
	}

	actionNames := strings.Split(schedulerConf.Actions, ",")
	for _, actionName := range actionNames {
		if action, found := framework.GetAction(strings.TrimSpace(actionName)); found {
			actions = append(actions, action)
		} else {
			return nil, nil, nil, nil, fmt.Errorf("failed to find Action %s, ignore it", actionName)
		}
	}

	return actions, schedulerConf.Tiers, schedulerConf.Configurations, schedulerConf.MetricsConfiguration, nil
}

// 启动一个 uds，这个 http server 的作用是可以动态设置 klog 的 level
func runSchedulerSocket() {
	fs := flag.CommandLine
	startKlogLevel := fs.Lookup("v").Value.String()
	socketDir := os.Getenv(util.SocketDirEnvName)
	if socketDir == "" {
		socketDir = util.DefaultSocketDir
	}
	// 动态设置 klog 的 level
	util.ListenAndServeKlogLogLevel("klog", startKlogLevel, socketDir)
}
