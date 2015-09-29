# Change this file to be a wrapper around your daemon code.

# Do your post daemonization configuration here
# At minimum you need just the first line (without the block), or a lot
# of strange things might start happening...
DaemonKit::Application.running! do |config|
  __configure
  config.trap( 'HUP', Proc.new { __configure } ) # kill -HUP to reload our config

  @ec2 = AWS::EC2.new(:ec2_endpoint => 'ec2.us-west-2.amazonaws.com')
  @workers_required = 0
  @workers = []

  @histogram = []

  # quick hack to get Infinity. Infinity is a constant in Ruby, but cannot be
  # referred to or instantiated or accessed in any way other than to divide 1.0 by 0.
  # This, I presume, is a feature.
  Inf = 1.0/0

  @ranges = {
    0..100000      => 2,
    100001..250000 => 3,
    250001..500000 => 4,
    500001..Inf    => 5
  }
end

# Sample loop to show process
loop do
  # Get the size of the current processing queue
  current_qlen = @queue.llen @app_config['queue']['q']

  # push this measurement onto the histogram and trim the oldest entry
  @histogram << current_qlen
  @histogram.shift

  # How many workers does this queue size call for?
  desired_workers = 0
  @ranges.each {|k,v| desired_workers = v if k.include? qlen}

  # get the histogram trend
  # the histogram trend measures data in 2-minute increments. If a trend has
  # continued for 10 consecutive minutes (5 entries), add or subtract 1 from
  # the desired workers. If 20 minutes, add or subtract 2.
  trend = histogram_trend()
  if trend >= 0
    desired_workers += trend / 5
  elsif trend < 0
    desired_workers -= trend.abs / 5
  end

  # Finally, we want to have 2 workers running at all times. Don't go lower than 2.
  desired_workers = 2 if desired_workers < 2

  # Get our current processing swarm for this stack (without any terminated ones)
  instances = @ec2.instances.select { |instance|
    instance.tags['App']   == @aws_config['tags']['App']  &&
    instance.tags['Role']  == @aws_config['tags']['Role'] &&
    instance.tags['Stack'] == DaemonKit.env.capitalize
  }.keep_if {|instance| instance.status != :terminated}

  # how many more or fewer instances do we require?
  delta = desired_workers - instances.size

  DaemonKit.logger.info "QUEUE LENGTH: #{'%9s' % qlen} [#{instances.size}/#{desired_workers}]"

  # take action!
  if delta < 0
    # we need to kill some instances. Find the n oldest and terminate them.
    kill_list = instances.sort_by(&:launch_time)[0..(delta.abs - 1)]
    kill_list.each do|instance|
      DaemonKit.logger.info "\t RIP: Instance #{instance.id} []"
      instance.terminate
    end
  elsif delta > 0
    # we need to launch some instances.
    (0..(delta.abs - 1)).each do |n|
      instance = @ec2.instances.create(
        image_id: @aws_config['ec2']['image_id'],
        instance_type: @aws_config['ec2']['instance_type'],
        count: 1,
        security_groups: @aws_config['ec2']['security_groups'],
        key_pair: @ec2.key_pairs[@aws_config['ec2']['key_pair_id']],
        instance_initiated_shutdown_behavior: 'terminate',
        user_data: ''
      )
      instance.tag('Name',  value: "thor-integration-qproc-#{instances.size + 1 + n}")
      instance.tag('App',   value: @aws_config['tags']['App'])
      instance.tag('Role',  value: @aws_config['tags']['Role'])
      instance.tag('Stack', value: DaemonKit.env.capitalize)

      DaemonKit.logger.info "\t Spawned: Instance #{instance.id}"

    end
  elsif delta == 0
    # nothing to do here, we've got the swarm we need, if not the swarm we deserve right now.
    # I'm such a dork.
  end

  # sleep now. Wake in 2 minutes to check our status again.
  sleep 120
end
