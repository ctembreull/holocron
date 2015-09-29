# encoding: utf-8

class Definition
  include Mongoid::Document
  include Mongoid::Timestamps

  field :csdl,            type: String,  default: ''
  field :last_updated_at, type: DateTime
  field :datasift_hash,   type: String,  default: ''
  field :subscription_id, type: String,  default: ''
  field :valid,           type: Boolean, default: false

  after_find :network_setup

  def network_setup
    @redis_config    = YAML.load_file(File.expand_path('../../../config/', __FILE__) + '/redis.yml')[ENV['RACK_ENV']]
    @es_config       = YAML.load_file(File.expand_path('../../../config/', __FILE__) + '/elasticsearch.yml')[ENV['RACK_ENV']]
    datasift_config  = YAML.load_file(File.expand_path('../../../config/', __FILE__) + '/datasift.yml')

    @datasift_client = DataSift::User.new(datasift_config['username'], datasift_config['api_key'], false)
  end

  def refresh(force=false)    
    projects      = Project.where(active: true)
    new_timestamp = projects.max_by(&:terms_updated_at).terms_updated_at.utc.to_i
    if force == true || new_timestamp > last_updated_at.utc.to_i
      csdl       = Thor::CSDLHelpers.generate(projects).gsub(/\"\"/,'"')
      update_attributes!(last_updated_at: new_timestamp, csdl: csdl)

      definition = @datasift_client.createDefinition(csdl)
      puts "\tVALIDATION: #{definition.validate}"
      if definition.validate
        puts "\tWARNING: Definition did not validate"
        update_attributes!(datasift_hash: definition.hash, valid: true)
      else
        update_attributes!(datasift_hash: '', valid: false)
      end

      return true
    end
    false
  end

  def start_collection(force_refresh=false)
    updated = refresh(force_refresh)
    if updated || subscription_id.empty?

      unless subscription_id.empty?
        stop_collection
      end

      # create new stream
      pushdef = @datasift_client.createPushDefinition()
      pushdef.output_type               = 'redis'
      pushdef.output_params['host']     = @redis_config['public_host']
      pushdef.output_params['port']     = @redis_config['port']
      pushdef.output_params['database'] = @redis_config['db']
      pushdef.output_params['list']     = @redis_config['queue']
      pushdef.output_params['format']   = 'json_interaction'

      subscr  = pushdef.subscribeStreamHash(datasift_hash, "redis.outpost.#{ENV['RACK_ENV']}")

      update_attributes!(subscription_id: subscr.id)
    end
  end

  def stop_collection
    unless subscription_id.empty? || subscription_id.nil?
      subscription = @datasift_client.getPushSubscription(subscription_id)
      if subscription.status == "finished"
        # Datasift stopped the subscription; we can clear the fields and exit
      else
        # try to stop it
        begin
          subscription.stop()
          update_attributes(subscription_id: '')
        rescue DataSift::DataSiftError => e
          update_attributes(subscription_id: '', datasift_hash: '', valid: false)
        end
      end
    end
  end

  def collection_state
    redis  = Redis.new(host: @redis_config['private_host'], port: @redis_config['port'], db: @redis_config['db'])
    status = {
      redis: {
        buffer_size:     redis.llen(@redis_config['queue'])
      }
    }

    if subscription_id.empty?
      status[:datasift] = nil
    else
      begin
        subscription = @datasift_client.getPushSubscription(subscription_id)
        status[:datasift] = {
          subscription_id: subscription_id,
          state:           subscription.status,
          created_at:      subscription.created_at.strftime('%Y-%m-%d %H:%M:%S')
        }
      rescue DataSift::APIError => e
        status[:datasift] = {
          subscription_id: subscription_id,
          error:           "[#{e.class.name}] => #{e.message}"
        }
      end
    end

    status
  end

  def ingestor_config
    {
      queue:  {
        h:  @redis_config['private_host'],
        p:  @redis_config['port'],
        db: @redis_config['db'],
        q:  @redis_config['queue'],
      },
      index:  {
        type: 'tweet',
        h:  @es_config['host'],
        p:  @es_config['port'],
        db: @es_config['index'],
        q:  nil
      },
      config: {
        z: 30,
        n: 5,
        q: [
          'Processor::DataSift::IdToInt',
          'Processor::DataSift::AmericanizeKeys',
          'Processor::VendorX::ConditionalTopics'
        ]
      }
    }
  end

end
