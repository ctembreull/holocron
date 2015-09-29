require 'modules/errors'
require 'modules/notifier'

Dir[File.dirname(__FILE__) + '/processors/*.rb'].each {|f| require f;}

# Your starting point for daemon specific classes. This directory is
# already included in your load path, so no need to specify it.

###
# Self-configuration based on our AWS stack configuration
###
def __configure
  __environment = DaemonKit.env

  @cur = 0
  @max = DaemonKit.arguments.options[:max].nil? ? -1 : DaemonKit.arguments.options[:max]

  #aws_config  = YAML.load_file(File.expand_path('../../config/aws.yml', __FILE__))
  #instance_id = `cat #{@aws_config['ec2']['instance_id_path']}`.chomp

  #unless instance_id.empty?
  #  DaemonKit.logger.info "CONFIGURING => Requesting instance stack data from EC2"
  #
  #  AWS.config(aws_config['authentication'])
  #  ec2 = AWS::EC2.new(ec2_endpoint: aws_config['ec2']['endpoint'])
  #  __environment = ec2.instances[instance_id].tags['Stack'].downcase
  #end

  #api_config = YAML.load_file(File.expand_path('../../config/thor_api.yml', __FILE__))
  #DaemonKit.logger.info "CONFIGURING => [#{__environment}] Requesting application configuration from #{api_config['host']}#{api_config['uri']}"

  #response = Faraday.get("#{api_config['host']}#{api_config['uri']}")
  #DaemonKit.arguments.options[:app_config] = JSON.parse(response.body)
  app_config = test_config()
  DaemonKit.arguments.options[:app_config] = app_config

  @notifier = VendorX::Notification::Notifier.new
  unless (app_config['notifier'].nil? || app_config['notifier'].empty?)
    app_config['notifier'].each do |type|
      require "modules/#{type}"
      @notifier.add_handler(constantize("VendorX::Notification::#{type.capitalize}").new)
    end
  end

  ## Provider class is a way of organizing and navigating the data we get from whatever provider we use.
  @formatter = nil
  unless app_config['provider'].nil?
    begin
      require "modules/#{app_config['provider']}"
      @formatter = constantize("VendorX::Provider::#{app_config['provider'].capitalize}")
      DaemonKit.logger.info "FORMATTER => VendorX::Provider::#{app_config['provider'].capitalize}"
    rescue LoadError => e
      raise VendorX::Errors::NotImplementedError, "provider: #{app_config['provider']}"
    end
  end

  @processors = []
  unless app_config['transform'].nil?
    app_config['transform'].each { |tx| @processors << constantize(tx) }
  end

  ## The Queue module is where the Provider puts its data. Default is Redis.
  raise VendorX::Errors::ConfigurationError, 'Queue configuration not specified' if app_config['queue'].nil?
  begin
    require "modules/#{app_config['queue']['type']}"
    @queue = constantize("VendorX::Queue::#{app_config['queue']['type'].capitalize}").new
    DaemonKit.logger.info "QUEUE => #{@queue.class.name}"
  rescue LoadError => e
    raise VendorX::Errors::NotImplementedError, "queue: #{app_config['queue']['type']}"
  end

  ## The storage module is where we put our data for later retrieval. Default is Elasticsearch.
  @storage = nil
  unless app_config['storage'].nil?
    begin
      require "modules/#{app_config['storage']['type']}"
      @storage = constantize("VendorX::Storage::#{app_config['storage']['type'].capitalize}").new
      DaemonKit.logger.info "STORAGE => #{@storage.class.name}"
    rescue LoadError => e
      raise VendorX::Errors::NotImplementedError, "storage: #{app_config['storage']['type']}"
    end
  end

  ## The graph module allows us to perform deeper network analysis on content.  Default is Neo4j.
  @graph = nil
  unless app_config['graph'].nil?
    begin
      require "modules/#{app_config['graph']['type']}" unless app_config['graph'].nil?
      @graph = constantize("VendorX::Graph::#{app_config['graph']['type'].capitalize}").new
      DaemonKit.logger.info "GRAPH => #{@graph.class.name}"
    rescue Loaderror => e
      raise VendorX::Errors::NotImplementedError "graph: #{app_config['graph']['type']}"
    end
  end

end

def constantize(camel_cased_word)
  names = camel_cased_word.split('::')

  # Trigger a builtin NameError exception including the ill-formed constant in the message.
  Object.const_get(camel_cased_word) if names.empty?

  # Remove the first blank element in case of '::ClassName' notation.
  names.shift if names.size > 1 && names.first.empty?

  names.inject(Object) do |constant, name|
    if constant == Object
      constant.const_get(name)
    else
      candidate = constant.const_get(name)
      next candidate if constant.const_defined?(name, false)
      next candidate unless Object.const_defined?(name)

      # Go down the ancestors to check it it's owned
      # directly before we reach Object or the end of ancestors.
      constant = constant.ancestors.inject do |const, ancestor|
        break const    if ancestor == Object
        break ancestor if ancestor.const_defined?(name, false)
        const
      end

      # owner is in Object, so raise
      constant.const_get(name, false)
    end
  end
end

def test_config
  app_config = {
    'provider' => 'datasift',
    'notifier' => ['flowdock'],
    'queue'    => {
      'type'  => 'redis',
      'host'  => 'analysis.dev.vendorx.com',
      'port'  => 6379,
      'db'    => 0,
      'queue' => 'interactions',
      'pull'  => 10,
      'sleep' => 15,
    },
    'storage' => {
      'type' => 'elasticsearch',
      'proto' => 'http',
      'host'  => 'analysis.dev.vendorx.com',
      'port'  => 8080,
      'username' => 'outpost',
      'password' => '4LpPpwA7kBzqsk',
      'log'      => false,
      'index'    => 'thor',
      'types'    => {
        'document' => 'tweet',
        'author'   => 'author'
      }
    },
    'graph' => {
      'type' => 'neo4j',
      'proto' => 'http',
      'host' => '54.191.179.69',
      'port' => 7474
    },
    'transform' => [
      'VendorX::Transform::IdToInt',
      'VendorX::Transform::AmericanizeKeys',
      'VendorX::Transform::RoundGeopoints',
      'VendorX::Transform::ReplaceTopics'
    ],
    'swarm' => {
      'min' => 1,
      'max' => 5,
      'interval' => 10000,
      'sleep' => 180
    }
  }
end
