# Logstash Plugin

This plugin is a modified version of the Lucidworks logstash json_batch. That plugin is available [here](https://github.com/lucidworks/logstash-output-json_batch). 

It has been modified to support ClickHouse JSON Format, but also supports fault tolerance.

# Usage

Please note that the name of the plugin when used is `clickhouse`, it only supports json in its current form. If further output formats are added in the future, this might change back to json_batch.

    output {
      clickhouse {
        headers => ["Authorization", "Basic YWRtaW46cGFzc3dvcmQxMjM="]
        http_hosts => ["http://your.clickhouse1/", "http://your.clickhouse2/", "http://your.clickhouse3/"]
        table => "table_name"
        mutations => {
          "to1" => "from1"
          "to2" => [ "from2", "(.)(.)", '\1\2' ]
        }
      }
    }

## Other custom options
* `save_on_failure` (default: true) - enable / disable request body save on failure
* `save_dir` (default: /tmp) - directory where failed request body will be saved
* `automatic_retries` (default: 1) - number of connect retry attempts to each host in `http_hosts`
* `request_tolerance` (default: 5) - number of http request send retry attempts if response status code is not 200
* `backoff_time` (default: 3) - time to wait in seconds for next retry attempt of connect or request
* `skip_unknown` (0 or 1, default: 1) - skip unknown fields when inserting into clickhouse. Uses `--input_format_skip_unknown_fields` parameter

Default batch size is 50, with a wait of at most 5 seconds per send. These can be tweaked with the parameters `flush_size` and `idle_flush_time` respectively.

# Installation 

The easiest way to use this plugin is by installing it through rubygems like any other logstash plugin. To get the latest versio installed, you should run the following command: `bin/logstash-plugin install logstash-output-clickhouse`

# Building the gem and installing a local version

To build the gem yourself, use `gem build logstash-output-clickhouse.gemspec` in the root of this repository. Alternatively, you can download a built version of the gem from the `dist` branch of this repository. 

To install, run the following command, assuming the gem is in the local directory: `$LOGSTASH_HOME/bin/plugin install logstash-output-clickhouse-X.Y.Z.gem`

