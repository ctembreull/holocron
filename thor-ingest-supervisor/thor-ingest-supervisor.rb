require 'json'

def __configure
  api_config = YAML.load_file(File.expand_path('../../config/', __FILE__) + '/thor_api.yml')[DaemonKit.env]
  @aws_config = YAML.load_file(File.expand_path('../../config/', __FILE__) + '/aws.yml')

  AWS.config(@aws_config['authentication'])

  DaemonKit.logger.info "CONFIGURING => Requesting application configuration from #{api_config['host']}#{api_config['uri']}"
  response = Faraday.get("#{api_config['host']}#{api_config['uri']}")
  @app_config = JSON.parse(response.body)

  @queue      = Redis.new(host: @app_config['queue']['h'], port: @app_config['queue']['p'], db: @app_config['queue']['db'])
end

# Attempt to determine the way the size of the queue is trending, and for how long.
def histogram_trend
  histo = @histogram.dup.reverse
  last  = histo[0]
  trend = 0
  dir   = 0
  for x in histo
    break if x == 0
    if x > last
      break if dir < 0
      trend += 1
      dir = 1
      last = x
    elsif x < last
      break if dir > 0
      trend += 1
      dir = -1
      last = x
    else
      trend++
    end
  end
  return dir*trend
end
