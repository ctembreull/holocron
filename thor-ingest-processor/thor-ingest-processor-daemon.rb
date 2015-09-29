# encoding: utf-8

require 'pp'
require 'active_support'

DaemonKit::Application.running! do |config|
  __configure
  puts "SAFE MODE" if DaemonKit.arguments.options[:safe]
  config.trap( 'HUP', Proc.new { __configure } )
end

loop do
  break if @cur >= @max && @max > 0
  @cur += 1 if @max > 0
  puts "#{@cur} of #{@max}" if DaemonKit.arguments.options[:debug]

  DaemonKit.logger.info "AWAKE: #{@queue.size}"
  documents = @queue.dequeue
  runtime = Time.now.to_i

  doc_count = 0
  documents.each do |doc|
    doc_count += 1
    DaemonKit.logger.info "#{doc_count} of #{documents.size}" if DaemonKit.arguments.options[:debug]

    interaction = @formatter.nil? ? doc : @formatter.new(doc)

    pp interaction if DaemonKit.arguments.options[:debug]

    @processors.each { |pc| interaction = pc.process(interaction }

    pp interaction if DaemonKit.arguments.options[:debug]

    unless @storage.nil?
      @storage.store_document(interaction)
      @storage.store_authors(interaction) unless DaemonKit.arguments.options[:app_config]['storage']['types']['author'].nil?
    end

    # If configured to do so, extract authors and the relationships between them
    @graph.graph_connections(interaction) if (@graph.present? || DaemonKit.arguments.options[:safe])

    if DaemonKit.arguments.options[:safe]
      puts "requeueing"
      @queue.requeue(doc)
    end
  end

  if @queue.size == 0
    DaemonKit.logger.info "SLEEP: #{@queue.size} [runtime: #{Time.now.to_i - runtime} sec]"
    sleep @queue.sleep_time
  end

end
